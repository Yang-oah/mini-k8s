# One command for same-version G5-01 through G5-07.
# Run from Windows. The two workers cannot SSH to each other.
# Usage:
#   pwsh -File ".\scripts\acceptance\g5-live.ps1"
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [Console]::OutputEncoding

$IpA = "lihaoyang@10.128.176.165"
$IpB = "lihaoyang@10.128.176.125"
$KeyA = Join-Path $env:USERPROFILE ".ssh\id_ed25519"
$KeyB = Join-Path $env:USERPROFILE ".ssh\id_rsa"
$RemotePy = "/home/lihaoyang/mini-k8s-g4/g5_live.py"
$RemoteHome = "/home/lihaoyang/mini-k8s-g4"
$Bin = "$RemoteHome/bin"
$Pointer = "$RemoteHome/g5cur-evidence.path"
$FwdFlag = "$RemoteHome/g5cur-forwarder-down"
$EtcdFlag = "$RemoteHome/g5cur-etcd-down"
$RootStop = "$RemoteHome/g5cur-root.stop"
$LocalPy = Join-Path $PSScriptRoot "g5_live.py"
$Watcher = $null
$RunLock = "$RemoteHome/.r1-live.lock"
$LockOwned = $false
$Completed = $false

function Invoke-Node {
    param(
        [Parameter(Mandatory = $true)][string]$Target,
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$Command,
        [switch]$Tty
    )
    $sshArgs = @(
        "-i", $Key,
        "-o", "IdentitiesOnly=yes",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=10",
        "-o", "ServerAliveInterval=15"
    )
    if ($Tty) { $sshArgs += "-tt" } else { $sshArgs += "-T" }
    if ($Tty) {
        & ssh @sshArgs $Target $Command
        $code = $LASTEXITCODE
        if ($code -ne 0) {
            throw "远程命令失败，退出码 $code"
        }
        return
    }
    $lines = & ssh @sshArgs $Target $Command 2>&1
    $code = $LASTEXITCODE
    $lines | ForEach-Object { Write-Host $_ }
    if ($code -ne 0) {
        $text = (($lines | Out-String).Trim())
        throw "远程命令失败，退出码 $code`n$text"
    }
}

function Copy-Between {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$From,
        [Parameter(Mandatory = $true)][string]$To
    )
    & scp -i $Key -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 $From $To
    if ($LASTEXITCODE -ne 0) {
        throw "文件传输失败"
    }
}

function Read-Evidence {
    $path = (& ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "cat $Pointer").Trim()
    if (-not $path) {
        throw "没有拿到证据目录"
    }
    return $path
}

function Remote-Sha {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$Target,
        [Parameter(Mandatory = $true)][string]$Path
    )
    $text = (& ssh -i $Key -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $Target "sha256sum $Path" | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $text) {
        throw "无法计算 $Path 的校验"
    }
    return ($text -split "\s+")[0]
}

Write-Host "G5 同一版本验收。全程大约 15 到 20 分钟，请不要关闭这个窗口。"
Write-Host "两台机器运行场景和最终核对时可能多次提示输入 sudo 密码。"
Write-Host "中间会暂停 etcd 10 秒，脚本会自己把它拉起来。失败时也会恢复 etcd 和 node-b 转发。"
Write-Host "脚本不会把 G5 记成通过。跑完后把 G5-01 到 G5-07 的结果发回来。"
Write-Host ""

if (-not (Test-Path $LocalPy)) {
    throw "找不到 $LocalPy"
}
if (-not (Test-Path $KeyA) -or -not (Test-Path $KeyB)) {
    throw "缺少 SSH 私钥。165 用 id_ed25519，125 用 id_rsa。"
}

try {
    Write-Host "获取验收运行锁"
    Invoke-Node $IpA $KeyA "mkdir -- $RunLock"
    $LockOwned = $true
    Write-Host "上传验收程序"
    Copy-Between $KeyA $LocalPy "${IpA}:$RemotePy"
    Copy-Between $KeyB $LocalPy "${IpB}:$RemotePy"

    Write-Host "检查 165 上的源码和空集群"
    Invoke-Node $IpA $KeyA "python3 -u $RemotePy precheck"
    $Evidence = Read-Evidence
    Write-Host "证据目录 $Evidence"

    Write-Host "编译当前源码"
    Invoke-Node $IpA $KeyA "python3 -u $RemotePy build"

    $Stamp = [guid]::NewGuid().ToString("N")
    $ProxyLocal = Join-Path $env:TEMP "mini-proxy.g5cur.$Stamp"
    $FixLocal = Join-Path $env:TEMP "g5-service-fixture.g5cur.$Stamp"
    $ProxyRemote = "$Bin/mini-proxy.g5cur.upload.$Stamp"
    $FixRemote = "$Bin/g5-service-fixture.g5cur.upload.$Stamp"
    Copy-Between $KeyA "${IpA}:$Bin/mini-proxy.g5cur" $ProxyLocal
    Copy-Between $KeyA "${IpA}:$Bin/g5-service-fixture.g5cur" $FixLocal
    Copy-Between $KeyB $ProxyLocal "${IpB}:$ProxyRemote"
    Copy-Between $KeyB $FixLocal "${IpB}:$FixRemote"
    $ProxyA = Remote-Sha $KeyA $IpA "$Bin/mini-proxy.g5cur"
    $ProxyB = Remote-Sha $KeyB $IpB $ProxyRemote
    $FixA = Remote-Sha $KeyA $IpA "$Bin/g5-service-fixture.g5cur"
    $FixB = Remote-Sha $KeyB $IpB $FixRemote
    if ($ProxyA -ne $ProxyB -or $FixA -ne $FixB -or -not $ProxyA -or -not $FixA) {
        Invoke-Node $IpB $KeyB "rm -f -- $ProxyRemote $FixRemote"
        throw "传到 node-b 的程序和 node-a 不一致，没有替换"
    }
    Invoke-Node $IpB $KeyB "chmod 755 $ProxyRemote $FixRemote && mv -f -- $ProxyRemote $Bin/mini-proxy.g5cur && mv -f -- $FixRemote $Bin/g5-service-fixture.g5cur"
    Write-Host "proxy 和检查程序已复制到 node-b"

    Write-Host ""
    Write-Host "第 1 次 sudo：10.128.176.165。只换本机 proxy，并核对镜像和租约。"
    Invoke-Node $IpA $KeyA "sudo python3 -u $RemotePy prepare-a" -Tty

    Write-Host ""
    Write-Host "第 2 次 sudo：10.128.176.125。换 proxy，核对镜像和租约，留下只做 hairpin 和快照的 root 进程。"
    $ManifestText = (& ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "cat $Evidence/g4-manifest.json" | Out-String)
    if ($LASTEXITCODE -ne 0) { throw "无法读取 G4 构建清单" }
    $AgentHash = ($ManifestText | ConvertFrom-Json).sha256.'mini-agent'
    if ($AgentHash -notmatch '^[0-9a-f]{64}$') { throw "G4 Agent 摘要无效" }
    Invoke-Node $IpB $KeyB "sudo python3 -u $RemotePy prepare-b $AgentHash" -Tty
    $Baseline = Join-Path $env:TEMP "g5cur-baseline-b.json"
    Copy-Between $KeyB "${IpB}:/tmp/g5cur-baseline-b.json" $Baseline
    Copy-Between $KeyA $Baseline "${IpA}:$Evidence/baseline-b.json"

    $Watcher = Start-Job -Name g5curWatch -ScriptBlock {
        param($KeyA, $KeyB, $IpA, $IpB, $RemotePy)
        $ErrorActionPreference = "Continue"
        $optsA = @("-i", $KeyA, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-T")
        $optsB = @("-i", $KeyB, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-T")
        $seen = ""
        $log = Join-Path $env:TEMP "g5cur-watcher.log"
        while ($true) {
            $raw = & ssh @optsA $IpA "cat /home/lihaoyang/mini-k8s-g4/g5cur-request.json 2>/dev/null || true"
            $text = ($raw | Out-String).Trim()
            if (-not $text.StartsWith("{")) {
                Start-Sleep -Milliseconds 300
                continue
            }
            try { $req = $text | ConvertFrom-Json } catch {
                Start-Sleep -Milliseconds 300
                continue
            }
            if ($req.id -eq $seen) {
                Start-Sleep -Milliseconds 300
                continue
            }
            $seen = [string]$req.id
            $payload = "{}"
            if ($req.payload) { $payload = $req.payload | ConvertTo-Json -Compress -Depth 8 }
            $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payload))
            $remote = "python3 -u $RemotePy $($req.action) '$b64'"
            Add-Content -Path $log -Value ("---- " + $req.action)
            $out = & ssh @optsB $IpB $remote 2>&1 | Out-String
            $ok = $LASTEXITCODE -eq 0
            Add-Content -Path $log -Value $out
            $ack = @{ id = [string]$req.id; ok = $ok; output = $out } | ConvertTo-Json -Compress -Depth 6
            $tmp = Join-Path $env:TEMP "g5cur-ack.json"
            $utf8 = New-Object System.Text.UTF8Encoding $false
            [System.IO.File]::WriteAllText($tmp, $ack, $utf8)
            & scp -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new $tmp "${IpA}:/home/lihaoyang/mini-k8s-g4/g5cur-ack.tmp"
            & ssh @optsA $IpA "mv -f /home/lihaoyang/mini-k8s-g4/g5cur-ack.tmp /home/lihaoyang/mini-k8s-g4/g5cur-ack.json"
        }
    } -ArgumentList $KeyA, $KeyB, $IpA, $IpB, $RemotePy

    Write-Host ""
    Write-Host "第 3 次 sudo：10.128.176.165。输入密码之后不需要再操作。"
    Write-Host "G5-06 会稳定 5 分钟，大约每分钟打印一行。"
    Invoke-Node $IpA $KeyA "python3 -u $RemotePy driver" -Tty
    $Completed = $true
    Write-Host ""
    Write-Host "跑完了。把上面的 G5-01 到 G5-07 结果发回来。这还不是 mini demo 验收通过。"
}
finally {
    if ($LockOwned) {
    if ($Watcher) {
        Stop-Job $Watcher -ErrorAction SilentlyContinue
        Remove-Job $Watcher -Force -ErrorAction SilentlyContinue
    }
    try {
        $etcdDown = (& ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "if test -f $EtcdFlag; then echo DOWN; fi" | Out-String)
        if ($etcdDown -match "DOWN") {
            Write-Host "场景中断，正在恢复 etcd"
            & ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "python3 -u $RemotePy recover-etcd"
        }
    } catch {
        Write-Host "恢复 etcd 时出错"
    }
    try {
        $fwdDown = (& ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "if test -f $FwdFlag; then echo DOWN; fi" | Out-String)
        if ($fwdDown -match "DOWN") {
            Write-Host "场景中断，正在恢复 node-b 转发"
            & ssh -i $KeyB -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpB "python3 -u $RemotePy start-forwarder"
            if ($LASTEXITCODE -ne 0) { throw "node-b 转发恢复失败，保留故障标记" }
            & ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "rm -f $FwdFlag"
        }
    } catch {
        Write-Host "恢复 node-b 转发时出错"
    }
    if (-not $Completed) {
        foreach ($target in @(@($IpA, $KeyA), @($IpB, $KeyB))) {
            Write-Host "尝试恢复 $($target[0]) 原 proxy；失败时保留现场和运行锁"
            try {
                Invoke-Node $target[0] $target[1] "sudo python3 -u $RemotePy rollback-proxy" -Tty
            } catch {
                Write-Host "原 proxy 未确认恢复：$($target[0])"
            }
        }
    }
    try {
        & ssh -i $KeyB -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpB "touch $RootStop"
    } catch {
        Write-Host "没有停掉 node-b 的取证进程"
    }
    try {
        & ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "python3 -u $RemotePy report"
    } catch {
    }
    if ($LockOwned -and $Completed) {
        foreach ($target in @(@($IpA, $KeyA), @($IpB, $KeyB))) {
            try {
                Write-Host "核对 $($target[0]) 的 proxy，可能需要输入 sudo 密码。"
                Invoke-Node $target[0] $target[1] "sudo python3 -u $RemotePy finalize-proxy" -Tty
            } catch {
                $Completed = $false
                Write-Host "proxy 身份未确认，运行锁已保留：$($target[0])"
            }
        }
    }
    if ($LockOwned -and $Completed) {
        & ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "if test ! -e $EtcdFlag && test ! -e $FwdFlag; then rmdir -- $RunLock; else exit 1; fi"
        if ($LASTEXITCODE -ne 0) { Write-Host "恢复状态未确认，运行锁已保留，请检查现场。" }
    } elseif ($LockOwned) {
        Write-Host "验收未正常结束，运行锁已保留；确认故障标记、组件和测试资源后再人工处理。"
    }
    }
}
