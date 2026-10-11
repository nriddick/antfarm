param(
    [ValidateSet('dmd', 'ldc2')]
    [string] $Compiler = 'ldc2',
    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Debug',
    [string] $CMake = 'cmake'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$buildDir = Join-Path $PSScriptRoot ('.build/mimalloc-v3.5.3-windows-' + $Configuration.ToLowerInvariant())
$libraryDir = Join-Path $buildDir 'lib'
$debugLevel = if ($Configuration -eq 'Debug') { 'FULL' } else { 'OFF' }
$libraryName = if ($Configuration -eq 'Debug') { 'mimalloc-debug.lib' } else { 'mimalloc.lib' }
$binary = Join-Path $buildDir ('actor_torture_' + $Compiler + '.exe')
$previousHugePages = $env:ANTFARM_HUGE_PAGES

Push-Location $repoRoot
try {
    # Pair the allocator with the D toolchains' static release CRT.
    # Keep debug code generation and allocator invariants, without the /MDd CRT.
    & $CMake -S third_party/mimalloc -B $buildDir `
        "-DCMAKE_BUILD_TYPE=$Configuration" '-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded' `
        "-DMI_DEBUG=$debugLevel" -DMI_OVERRIDE=OFF -DMI_BUILD_SHARED=OFF `
        -DMI_BUILD_STATIC=ON -DMI_BUILD_OBJECT=OFF -DMI_BUILD_TESTS=OFF -DMI_OPT_ARCH=OFF `
        "-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY=$libraryDir" `
        "-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG=$libraryDir" `
        "-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE=$libraryDir"
    if ($LASTEXITCODE -ne 0) { throw 'mimalloc configuration failed' }
    & $CMake --build $buildDir --config $Configuration --target mimalloc-static --parallel
    if ($LASTEXITCODE -ne 0) { throw 'mimalloc build failed' }

    $versionFlag = if ($Compiler -eq 'dmd') { '-version=' } else { '-d-version=' }
    $compilerArgs = @('-g', '-I.', '-mscrtlib=libcmt',
        ($versionFlag + 'AntfarmActorTestHooks'),
        ($versionFlag + 'AntfarmMimallocV3'), ($versionFlag + 'AntfarmMimallocReal'),
        'actor_torture/actor_torture.d', 'antfarm.d', 'antfarm_allocation.d',
        'antfarm_templates.d', 'actors/package.d', 'actors/actor.d', 'actors/wave.d',
        'actors/private_actor.d', 'actors/mimalloc.d', (Join-Path $libraryDir $libraryName),
        ('-of=' + $binary))
    & $Compiler @compilerArgs
    if ($LASTEXITCODE -ne 0) { throw 'actor torture compilation failed' }
    $env:ANTFARM_HUGE_PAGES = '0'
    & $binary
    if ($LASTEXITCODE -ne 0) { throw 'actor torture failed' }
}
finally {
    $env:ANTFARM_HUGE_PAGES = $previousHugePages
    Pop-Location
}
