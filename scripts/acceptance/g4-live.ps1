# One command for same-version G4-01 through G4-06.
# Run from Windows. It connects to both workers because they cannot SSH to each other.
# Usage:
#   pwsh -File ".\scripts\acceptance\g4-live.ps1"
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [Console]::OutputEncoding

. (Join-Path $PSScriptRoot "r1-live-config.ps1")
$RemotePy = "$RemoteHome/g4_live.py"

$Agent = "$RemoteHome/bin/mini-agent.g4cur"
$Pointer = "$RemoteHome/g4cur-evidence.path"
$FwdFlag = "$RemoteHome/g4cur-forwarder-down"
$RunLock = "$RemoteHome/.r1-live.lock"
$LockOwned = $false
$Completed = $false
$LocalPy = Join-Path $PSScriptRoot "g4_live.py"
$Watcher = $null

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

Write-Host "G4 同一版本验收。全程大约 15 到 25 分钟，请不要关闭这个窗口。"
Write-Host "会要四次 sudo 密码：node-a 让 root 进程读取新证书，node-b 读基线，node-a 换程序并跑场景，结束前 node-b 再对一次租约。"
Write-Host "脚本不停止 etcd。两台 proxy 只在开头为了读取新证书各重启一次，场景中途不再动它们。失败时会尽量把 node-b 的转发拉起来，并留下现场。"
Write-Host ""

if (-not (Test-Path $LocalPy)) {
    throw "找不到 $LocalPy"
}
if (-not (Test-Path $KeyA) -or -not (Test-Path $KeyB)) {
    throw "缺少配置的 SSH 私钥。"
}

try {
    # 与 G5 共用同一把锁，避免脚本文件、请求通道和故障注入互相覆盖。
    Invoke-Node $IpA $KeyA "mkdir -- $RunLock"
    $LockOwned = $true
    Write-Host "上传验收程序"
    Copy-Between $KeyA $LocalPy "${IpA}:$RemotePy"
    Copy-Between $KeyB $LocalPy "${IpB}:$RemotePy"

    Write-Host "先检查两台机器、源码和集群状态"
    Invoke-Node $IpA $KeyA "env $RemoteEnv python3 -u $RemotePy readonly-precheck"
    Invoke-Node $IpB $KeyB "env $RemoteEnv python3 -u $RemotePy precheck-b"

    Write-Host ""
    Write-Host "第 1 次 sudo：$IpA。只让 root 身份的 Agent 和 proxy 读取新证书，不跑场景。"
    Invoke-Node $IpA $KeyA "env $RemoteEnv python3 -u $RemotePy reload-root" -Tty

    Write-Host "检查 node-a 上的源码和空集群"
    Invoke-Node $IpA $KeyA "env $RemoteEnv python3 -u $RemotePy precheck"
    $Evidence = Read-Evidence
    Write-Host "证据目录 $Evidence"

    Write-Host "如果上次中断，先把 node-b 转发拉起来"
    Invoke-Node $IpB $KeyB "env $RemoteEnv python3 -u $RemotePy start-forwarder"
    Invoke-Node $IpB $KeyB "env $RemoteEnv python3 -u $RemotePy precheck-b"

    Write-Host ""
    Write-Host "第 2 次 sudo：$IpB。先让 proxy 读取新证书，再只读镜像、容器和 IP 租约。"
    Invoke-Node $IpB $KeyB "sudo env $RemoteEnv python3 -u $RemotePy baseline-b" -Tty
    $TempJson = Join-Path $env:TEMP "g4cur-baseline-b.json"
    Copy-Between $KeyB "${IpB}:/tmp/g4cur-baseline-b.json" $TempJson
    Copy-Between $KeyA $TempJson "${IpA}:$Evidence/baseline-b.json"

    Write-Host "编译当前源码"
    Invoke-Node $IpA $KeyA "env $RemoteEnv python3 -u $RemotePy build"
    Copy-Between $KeyA "${IpA}:$Agent" (Join-Path $env:TEMP "mini-agent.g4cur")
    # 运行中的可执行文件不能直接覆盖；先上传到同目录的唯一文件并校验。
    $AgentUpload = "$Agent.upload.$([guid]::NewGuid().ToString('N'))"
    Copy-Between $KeyB (Join-Path $env:TEMP "mini-agent.g4cur") "${IpB}:$AgentUpload"
    $HashA = (& ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "sha256sum $Agent").Split()[0]
    $HashB = (& ssh -i $KeyB -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpB "sha256sum $AgentUpload").Split()[0]
    if ($HashA -ne $HashB -or -not $HashA) {
        throw "上传的 Agent 程序与 node-a 不一致；未替换运行文件"
    }
    Invoke-Node $IpB $KeyB "chmod 755 $AgentUpload && mv -f -- $AgentUpload $Agent"
    Write-Host "Agent 程序已复制到 node-b"

    $Watcher = Start-Job -Name g4curWatch -ScriptBlock {
        param($KeyA, $KeyB, $IpA, $IpB, $RemotePy, $RemoteEnv, $RemoteHome)
        $ErrorActionPreference = "Continue"
        $optsA = @("-i", $KeyA, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-T")
        $optsB = @("-i", $KeyB, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-T")
        $seen = ""
        $log = Join-Path $env:TEMP "g4cur-watcher.log"
        while ($true) {
            $raw = & ssh @optsA $IpA "cat $RemoteHome/g4cur-request.json 2>/dev/null || true"
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
            if ($req.payload) { $payload = $req.payload | ConvertTo-Json -Compress -Depth 6 }
            $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payload))
            $remote = "env $RemoteEnv python3 -u $RemotePy $($req.action) '$b64'"
            Add-Content -Path $log -Value ("---- " + $req.action)
            $out = & ssh @optsB $IpB $remote 2>&1 | Out-String
            $ok = $LASTEXITCODE -eq 0
            Add-Content -Path $log -Value $out
            $ack = @{ id = [string]$req.id; ok = $ok; output = $out } | ConvertTo-Json -Compress -Depth 6
            $tmp = Join-Path $env:TEMP "g4cur-ack.json"
            $utf8 = New-Object System.Text.UTF8Encoding $false
            [System.IO.File]::WriteAllText($tmp, $ack, $utf8)
            & scp -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new $tmp "${IpA}:$RemoteHome/g4cur-ack.tmp"
            & ssh @optsA $IpA "mv -f $RemoteHome/g4cur-ack.tmp $RemoteHome/g4cur-ack.json"
        }
    } -ArgumentList $KeyA, $KeyB, $IpA, $IpB, $RemotePy, $RemoteEnv, $RemoteHome

    Write-Host ""
    Write-Host "第 3 次 sudo：$IpA。这次会换上新程序并跑完全部 G4 场景。"
    Write-Host "输入密码之后不需要再操作，直到它自己停下来。"
    Invoke-Node $IpA $KeyA "env $RemoteEnv python3 -u $RemotePy driver" -Tty

    Write-Host ""
    Write-Host "第 4 次 sudo：$IpB。核对租约和容器是否回到测试前。"
    Invoke-Node $IpB $KeyB "sudo env $RemoteEnv python3 -u $RemotePy final-b" -Tty
    $Final = Join-Path $env:TEMP "g4cur-final-b.json"
    Copy-Between $KeyB "${IpB}:/tmp/g4cur-final-b.json" $Final
    Copy-Between $KeyA $Final "${IpA}:$Evidence/final-b.json"
    Invoke-Node $IpA $KeyA "env $RemoteEnv python3 -u $RemotePy finish"
    $Completed = $true
    Write-Host ""
    Write-Host "跑完了。把上面的 G4-01 到 G4-06 结果发回来。这还不是 G5 通过。"
}
finally {
    if ($LockOwned) {
    $flag = ""
    try {
        $flag = (& ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "if test -f $FwdFlag; then echo DOWN; fi" | Out-String)
    } catch {
        $flag = ""
    }
    if ($flag -match "DOWN") {
        Write-Host "场景中断，正在恢复 node-b 转发"
        & ssh -i $KeyB -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpB "env $RemoteEnv python3 -u $RemotePy start-forwarder"
    }
    if ($Watcher) {
        Stop-Job $Watcher -ErrorAction SilentlyContinue
        Remove-Job $Watcher -Force -ErrorAction SilentlyContinue
    }
    try {
        if (Test-Path (Join-Path $PSScriptRoot "g4_live.py")) {
            & ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "env $RemoteEnv python3 -u $RemotePy report"
        }
    } catch {
    }
    if ($Completed) {
        & ssh -i $KeyA -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T $IpA "if test ! -e $FwdFlag; then rmdir -- $RunLock; else exit 1; fi"
        if ($LASTEXITCODE -ne 0) { Write-Host "恢复状态未确认，运行锁已保留，请检查现场。" }
    } else {
        Write-Host "验收未正常结束，运行锁已保留；确认故障标记、组件和测试资源后再人工处理。"
    }
    }
}
