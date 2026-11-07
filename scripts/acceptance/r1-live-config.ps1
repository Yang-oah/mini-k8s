# Shared settings for G4/G5 live acceptance. Set these environment variables before invoking either runner.
$IpA = if ($env:R1_NODE_A) { $env:R1_NODE_A } else { "lihaoyang@10.128.176.165" }
$IpB = if ($env:R1_NODE_B) { $env:R1_NODE_B } else { "lihaoyang@10.128.176.125" }
$KeyA = if ($env:R1_KEY_A) { $env:R1_KEY_A } else { Join-Path $env:USERPROFILE ".ssh\id_ed25519" }
$KeyB = if ($env:R1_KEY_B) { $env:R1_KEY_B } else { Join-Path $env:USERPROFILE ".ssh\id_rsa" }
$RemoteHome = if ($env:R1_REMOTE_HOME) { $env:R1_REMOTE_HOME.TrimEnd('/') } else { "/home/lihaoyang/mini-k8s-g4" }
$RemoteBinPath = if ($env:R1_REMOTE_BIN_PATH) { $env:R1_REMOTE_BIN_PATH } else { "/home/lihaoyang/.local/go/bin:/home/lihaoyang/.local/bin" }
$ApiEndpoint = if ($env:R1_API_ENDPOINT) { $env:R1_API_ENDPOINT.TrimEnd('/') } else { "https://10.128.176.165:8443" }
$SourceDir = if ($env:R1_SOURCE_DIR) { $env:R1_SOURCE_DIR } else { "/home/lihaoyang/mini-k8s-g4/evidence/r1-selector-v10-I2MJLyAh" }
$ExpectedSha = if ($env:R1_EXPECTED_SHA) { $env:R1_EXPECTED_SHA } else { "1a8537f578953716d9c2c9858b2f2421d2ff961c1ea8212b9c3e3991e9e401a8" }
# Values below are interpolated into remote shell commands; reject shell metacharacters and traversal.
foreach ($path in @($RemoteHome, $SourceDir) + ($RemoteBinPath -split ':')) {
    if ($path -notmatch '^/[A-Za-z0-9_./-]+$' -or $path -match '(^|/)\.\.(/|$)') { throw "Invalid remote absolute path: $path" }
}
foreach ($target in @($IpA, $IpB)) {
    if ($target -notmatch '^[A-Za-z0-9_][A-Za-z0-9_.-]*@[A-Za-z0-9][A-Za-z0-9.-]*$') { throw "Invalid SSH target: $target" }
}
if ($ApiEndpoint -notmatch '^https://[A-Za-z0-9.-]+:[0-9]{1,5}$' -or $ExpectedSha -cnotmatch '^[0-9a-f]{64}$') { throw "Invalid API endpoint or SHA-256" }
$RemoteEnv = "R1_SOURCE_DIR=$SourceDir R1_EXPECTED_SHA=$ExpectedSha R1_REMOTE_HOME=$RemoteHome R1_REMOTE_BIN_PATH=$RemoteBinPath R1_API_ENDPOINT=$ApiEndpoint"
