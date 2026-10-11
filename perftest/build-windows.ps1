param([string] $Compiler = 'ldc2')

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$archive = Join-Path $root 'actor_torture/.build/mimalloc-v3.5.3-windows-release/lib/mimalloc.lib'
if (-not (Test-Path -LiteralPath $archive)) {
    throw 'Build the Release allocator with actor_torture/run-mimalloc-windows.ps1 first.'
}
$farm = @('antfarm.d', 'antfarm_allocation.d')
$actors = @('actors/package.d', 'actors/actor.d', 'actors/wave.d',
    'actors/private_actor.d', 'actors/mimalloc.d')
Push-Location $root
try {
    foreach ($name in @('throughput', 'dual', 'tail', 'actor_ready', 'actor_churn', 'actor_churn_mimalloc', 'scaling')) {
        $source = if ($name -eq 'actor_churn_mimalloc') { 'actor_churn' } else { $name }
        $args = @('-O2', '-release', '-mscrtlib=libcmt', '-I.', '-Iperftest',
            '-Ithreadpool/source', '-Ifibers/source', "perftest/$source.d") + $farm
        if ($name -in @('tail', 'actor_churn', 'actor_churn_mimalloc', 'scaling')) { $args += '-i' }
        if ($name -in @('actor_ready', 'actor_churn', 'actor_churn_mimalloc')) { $args += $actors }
        if ($name -eq 'actor_churn_mimalloc') { $args += @('-d-version=AntfarmMimallocV3', $archive) }
        $args += "-of=perftest/$name.exe"
        & $Compiler @args
        if ($LASTEXITCODE -ne 0) { throw "Build failed: $name" }
    }
}
finally { Pop-Location }
