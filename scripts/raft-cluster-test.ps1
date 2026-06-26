# Boots three real Raft node processes (separate JVMs) that talk over HTTP, and proves they elect a
# leader, replicate a command to convergence, survive the leader being killed, and re-elect + reconverge.
$ErrorActionPreference = "Continue"
$jar = "C:\workspace\20_projectG\raft\app\build\libs\app-0.0.1-SNAPSHOT.jar"
$java = "C:\workspace\tools\jdk-21\bin\java.exe"
$env:JAVA_HOME = "C:\workspace\tools\jdk-21"

$ports = [ordered]@{ n0 = 18104; n1 = 18105; n2 = 18106 }
$procs = @{}

function Start-Node($id, $port, $peers) {
  $a = @("-jar", $jar, "--spring.profiles.active=node", "--server.port=$port", "--raft.node.id=$id",
    "--raft.cluster.autorun=false") + $peers
  return Start-Process -FilePath $java -ArgumentList $a -PassThru -WindowStyle Hidden
}

$procs["n0"] = Start-Node "n0" 18104 @("--raft.node.peers.n1=http://localhost:18105", "--raft.node.peers.n2=http://localhost:18106")
$procs["n1"] = Start-Node "n1" 18105 @("--raft.node.peers.n0=http://localhost:18104", "--raft.node.peers.n2=http://localhost:18106")
$procs["n2"] = Start-Node "n2" 18106 @("--raft.node.peers.n0=http://localhost:18104", "--raft.node.peers.n1=http://localhost:18105")

function Get-State($port) {
  try { return Invoke-RestMethod "http://localhost:$port/raft/state" -TimeoutSec 2 } catch { return $null }
}
function Show($label) {
  Write-Output "`n[$label]"
  foreach ($e in $ports.GetEnumerator()) {
    $s = Get-State $e.Value
    if ($s) { "  {0} {1,-9} term={2} commit={3} log=[{4}]" -f $s.id, $s.role, $s.term, $s.commitIndex, ($s.log -join ",") }
    else { "  $($e.Name) (down)" }
  }
}
function Logs() {
  $set = @()
  foreach ($e in $ports.GetEnumerator()) { $s = Get-State $e.Value; if ($s) { $set += ($s.log -join ",") } }
  return $set
}
function Find-Leader() {
  foreach ($e in $ports.GetEnumerator()) { $s = Get-State $e.Value; if ($s -and $s.role -eq "LEADER") { return $s } }
  return $null
}

try {
  # 1) wait for all three up and a leader elected over the real network
  $leader = $null
  for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Milliseconds 700
    $up = 0; foreach ($e in $ports.GetEnumerator()) { if (Get-State $e.Value) { $up++ } }
    $leader = Find-Leader
    if ($up -eq 3 -and $leader) { break }
  }
  if (-not $leader) { Write-Output "FAILED: no leader elected"; return }
  Write-Output "leader elected over HTTP: $($leader.id) (term $($leader.term))"

  # 2) propose 5 commands to the leader, then check every node converged
  $lport = $ports[$leader.id]
  for ($i = 0; $i -lt 5; $i++) { try { Invoke-RestMethod "http://localhost:$lport/raft/propose?command=c$i" -Method Post -TimeoutSec 2 | Out-Null } catch {} }
  Start-Sleep -Seconds 3
  Show "after proposing c0..c4"
  $u = Logs | Select-Object -Unique
  Write-Output "converged across 3 processes: $(($u.Count -eq 1) -and ($u[0].Length -gt 0))"

  # 3) kill the leader; the remaining two must elect a new leader and stay converged
  Write-Output "`n>>> killing leader $($leader.id)"
  Stop-Process -Id $procs[$leader.id].Id -Force -ErrorAction SilentlyContinue
  $ports.Remove($leader.id)
  $newLeader = $null
  for ($i = 0; $i -lt 40; $i++) { Start-Sleep -Milliseconds 700; $newLeader = Find-Leader; if ($newLeader) { break } }
  if (-not $newLeader) { Write-Output "FAILED: no new leader after failover"; return }
  Write-Output "new leader after failover: $($newLeader.id) (term $($newLeader.term))"
  $lport2 = $ports[$newLeader.id]
  for ($i = 5; $i -lt 8; $i++) { try { Invoke-RestMethod "http://localhost:$lport2/raft/propose?command=c$i" -Method Post -TimeoutSec 2 | Out-Null } catch {} }
  Start-Sleep -Seconds 3
  Show "after failover + proposing c5..c7"
  $u2 = Logs | Select-Object -Unique
  Write-Output "survivors converged: $(($u2.Count -eq 1) -and ($u2[0].Length -gt 0))"
}
finally {
  foreach ($p in $procs.Values) { try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {} }
  Write-Output "`n(stopped all node processes)"
}
