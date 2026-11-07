# Run the existing live gates in order against an explicitly selected Linux snapshot.
[CmdletBinding()]
param([switch]$IncludeR2A1)

$ErrorActionPreference = 'Stop'
if (-not $env:R1_SOURCE_DIR -or -not $env:R1_EXPECTED_SHA) {
    throw 'Set R1_SOURCE_DIR and R1_EXPECTED_SHA to the Linux snapshot and its SHA-256.'
}
. (Join-Path $PSScriptRoot 'r1-live-config.ps1')
if (-not (Test-Path -LiteralPath $KeyA -PathType Leaf) -or -not (Test-Path -LiteralPath $KeyB -PathType Leaf)) {
    throw 'Check the SSH identity files for both workers.'
}

$sshArgs = @('-i', $KeyA, '-o', 'IdentitiesOnly=yes', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=10')
$runId = [guid]::NewGuid().ToString('N')
$g3Log = "$SourceDir/.live-g3-$runId.log"
$summary = Join-Path $env:TEMP "mini-k8s-live-$runId.json"
$results = [ordered]@{
    sourceDirectory = $SourceDir
    sourceSha256 = $ExpectedSha
    G3 = [ordered]@{ result = 'NOT_RUN'; log = $g3Log; evidence = $null }
    G4 = [ordered]@{ result = 'NOT_RUN'; evidence = $null }
    G5 = [ordered]@{ result = 'NOT_RUN'; evidence = $null }
}
if ($IncludeR2A1) { $results['R2-A1'] = [ordered]@{ result = 'NOT_RUN'; evidence = $null } }

function Read-ControlPlane([string]$Command) {
    $response = & ssh @sshArgs -T $IpA $Command
    if ($LASTEXITCODE -ne 0) { throw "Could not read control-plane evidence (SSH exit $LASTEXITCODE)." }
    return ($response -join "`n").Trim()
}

function Read-EvidencePointer([string]$Pointer) {
    return Read-ControlPlane "if test -f $Pointer; then cat -- $Pointer; fi"
}

try {
    Write-Host 'G3 runs G1 and G2 first. Enter sudo passwords in the terminal when prompted.'
    $results.G3.result = 'FAIL'
    $g3Run = 'bash {0}/scripts/acceptance/r1-run-g3.sh 2>&1 | tee {1}; exit ${{PIPESTATUS[0]}}' -f $SourceDir, $g3Log
    $testPath = "$($RemoteBinPath):/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    & ssh @sshArgs -tt $IpA "env $RemoteEnv R1_TEST_PATH=$testPath bash -o pipefail -c '$g3Run'"
    $g3Exit = $LASTEXITCODE
    $results.G3.evidence = Read-ControlPlane "sed -n 's/^Evidence: //p' $g3Log | tail -n 1"
    if ($g3Exit -eq 2) { $results.G3.result = 'BLOCKED' }
    if ($g3Exit -ne 0 -or -not $results.G3.evidence) { throw "G3 did not pass (exit $g3Exit)." }
    $results.G3.result = 'PASS'

    foreach ($gate in @('G4', 'G5')) {
        $results[$gate].result = 'FAIL'
        $name = $gate.ToLowerInvariant()
        $pointer = "$RemoteHome/$($name)cur-evidence.path"
        $previous = Read-EvidencePointer $pointer
        & (Join-Path $PSScriptRoot "$name-live.ps1")
        $evidence = Read-EvidencePointer $pointer
        if (-not $evidence -or $evidence -eq $previous -or $evidence -notmatch '^/[A-Za-z0-9_./-]+$' -or $evidence -match '(^|/)\.\.(/|$)') {
            throw "$gate did not produce a fresh evidence directory."
        }
        $results[$gate].evidence = $evidence
        $precheck = Read-ControlPlane "cat -- $evidence/precheck.json" | ConvertFrom-Json
        $build = Read-ControlPlane "cat -- $evidence/build.json" | ConvertFrom-Json
        $rows = @(Read-ControlPlane "cat -- $evidence/results.json" | ConvertFrom-Json | ForEach-Object { $_ })
        $required = if ($gate -eq 'G4') { 6 } else { 7 }
        $passed = @($rows | Where-Object { $_.ok -eq $true } | ForEach-Object { $_.id } | Sort-Object -Unique)
        $expectedIds = @(1..$required | ForEach-Object { '{0}-{1:00}' -f $gate, $_ })
        $cleaned = Read-ControlPlane "if test ! -e $RemoteHome/.r1-live.lock; then echo clean; fi"
        if (-not $precheck.ok -or $precheck.checksum -cne $ExpectedSha -or -not $build.ok -or $rows.Count -ne $required -or ($passed -join ',') -cne ($expectedIds -join ',') -or $cleaned -ne 'clean') {
            throw "$gate evidence, source checksum or cleanup did not pass."
        }
        $results[$gate].result = 'PASS'

        if ($gate -eq 'G4' -and $IncludeR2A1) {
            # The overload probe requires the running G4 API and its log.
            $probe = $results['R2-A1']
            $probe.result = 'FAIL'
            $probeOutput = & ssh @sshArgs -T $IpA "env $RemoteEnv python3 -u $SourceDir/scripts/acceptance/r2-a1-live.py"
            $probeExit = $LASTEXITCODE
            $probeOutput | ForEach-Object { Write-Host $_ }
            $evidenceLine = $probeOutput | Select-String '^Evidence: (.+)$' | Select-Object -Last 1
            if ($evidenceLine) { $probe.evidence = $evidenceLine.Matches[0].Groups[1].Value.Trim() }
            if ($probeExit -ne 0 -or -not $probe.evidence) { throw "R2-A1 overload probe did not pass (exit $probeExit)." }
            $probe.result = 'PASS'
        }
    }
    Write-Host 'PASS: G1 through G5 passed on the selected source snapshot.'
}
finally {
    $results | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $summary -Encoding utf8
    Write-Host "Summary: $summary"
}
