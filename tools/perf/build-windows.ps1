param([string] $Compiler = 'ldc2')

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$archive = Join-Path $root 'tests/actors/.build/mimalloc-v3.5.3-windows-release/lib/mimalloc.lib'
if (-not (Test-Path -LiteralPath $archive)) {
    throw 'Build the Release allocator with tests/actors/run-mimalloc-windows.ps1 first.'
}
$farm = @('antfarm/package.d', 'antfarm/allocation.d')
$actors = @('antfarm/actors/package.d', 'antfarm/actors/actor.d', 'antfarm/actors/wave.d',
    'antfarm/actors/private_actor.d', 'antfarm/actors/mimalloc.d')
Push-Location $root
try {
    foreach ($name in @('throughput', 'dual', 'tail', 'actor_ready', 'actor_churn', 'actor_churn_mimalloc', 'scaling', 'offccd', 'c2c')) {
        $source = if ($name -eq 'actor_churn_mimalloc') { 'actor_churn' } else { $name }
        # Library packages must precede the benchmark directory: it also has
        # ant_fibers/actor_wave.d, an executable rather than the library module.
        $args = @('-O2', '-release', '-mscrtlib=libcmt', '-I.',
            '-Ithreadpool/source', '-Iant_fibers/source', '-Itools/perf', "tools/perf/$source.d") + $farm
        if ($name -in @('tail', 'actor_churn', 'actor_churn_mimalloc', 'scaling', 'offccd', 'c2c')) { $args += '-i' }
        if ($name -in @('actor_ready', 'actor_churn', 'actor_churn_mimalloc')) { $args += $actors }
        if ($name -eq 'actor_churn_mimalloc') { $args += @('-d-version=AntfarmMimallocV3', $archive) }
        $args += "-of=tools/perf/$name.exe"
        & $Compiler @args
        if ($LASTEXITCODE -ne 0) { throw "Build failed: $name" }
    }
}
finally { Pop-Location }
