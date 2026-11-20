# The secure mimalloc lanes on Windows x64 (tests/mimalloc_secure/Makefile on
# Linux): the fast archive, the secure archive from
# scripts/build-mimalloc-secure.py (MI_SECURE=CHECKED, no static guard,
# symbols renamed afsec_, /INCLUDE: directives rewritten), then secure_test
# linked with both, with the secure one alone, and without the option.
#
# Not yet run on Windows. What to compare against Linux (v3.7.0):
# - the build script's last line: about 600 symbols renamed (Linux: 603;
#   Windows adds its TLS and CRT callbacks), a nonzero count of linker
#   directives rewritten (16 strings in prim.c name renamed symbols when the
#   TLS callbacks are in the table), 0 unprefixed definitions or references;
#   it stops and lists any defined symbol that is neither a mimalloc name nor
#   a known shared COMDAT constant (pass -Allow to accept one deliberately);
# - every secure_test line PASS, in particular "each of them was released at
#   thread exit", which needs the renamed instance's TLS callbacks.
param(
    [ValidateSet('dmd', 'ldc2')]
    [string] $Compiler = 'ldc2',
    [string] $CMake = 'cmake',
    [string] $Python = 'python',
    [string[]] $Allow = @()
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$fastDir = Join-Path $repoRoot 'tests/actors/.build/mimalloc-v3.7.0-windows-release'
$fastLib = Join-Path $fastDir 'lib/mimalloc.lib'
$secureDir = Join-Path $repoRoot 'tests/actors/.build/mimalloc-v3.7.0-secure-windows-release'
$secureLib = Join-Path $secureDir 'mimalloc-secure.lib'
$outDir = Join-Path $PSScriptRoot '.build'
$crt = '-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded'  # the D toolchains' static release CRT

Push-Location $repoRoot
try {
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
    & $CMake -S third_party/mimalloc -B $fastDir -DCMAKE_BUILD_TYPE=Release $crt `
        -DMI_OVERRIDE=OFF -DMI_BUILD_SHARED=OFF -DMI_BUILD_STATIC=ON -DMI_BUILD_OBJECT=OFF `
        -DMI_BUILD_TESTS=OFF -DMI_OPT_ARCH=OFF `
        "-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY=$fastDir/lib" "-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE=$fastDir/lib"
    if ($LASTEXITCODE -ne 0) { throw 'mimalloc configuration failed' }
    & $CMake --build $fastDir --config Release --target mimalloc-static --parallel
    if ($LASTEXITCODE -ne 0) { throw 'mimalloc build failed' }

    $secureArgs = @('scripts/build-mimalloc-secure.py', '--build-dir', $secureDir,
        '--fast-archive', $fastLib, '--cmake', $CMake, "--cmake-arg=$crt")
    foreach ($a in $Allow) { $secureArgs += "--allow=$a" }
    & $Python @secureArgs
    if ($LASTEXITCODE -ne 0) { throw 'secure mimalloc build failed' }

    $v = if ($Compiler -eq 'dmd') { '-version=' } else { '-d-version=' }
    $sources = @('tests/mimalloc_secure/secure_test.d', 'antfarm/secure_allocation.d', 'antfarm/allocation.d')
    $lanes = @(
        @{ Name = 'two'; Versions = @('AntfarmMimallocV3', 'AntfarmMimallocSecure'); Libs = @($fastLib, $secureLib) },
        @{ Name = 'alone'; Versions = @('AntfarmMimallocSecure'); Libs = @($secureLib) },
        @{ Name = 'off'; Versions = @(); Libs = @() })
    foreach ($lane in $lanes) {
        $binary = Join-Path $outDir ("secure_test_${Compiler}_" + $lane.Name + '.exe')
        $dArgs = @('-g', '-I.', '-mscrtlib=libcmt') + @($lane.Versions | ForEach-Object { $v + $_ }) +
            $sources + $lane.Libs + @("-of=$binary", ('-od=' + (Join-Path $outDir ("obj-${Compiler}-" + $lane.Name))))
        & $Compiler @dArgs
        if ($LASTEXITCODE -ne 0) { throw ('secure_test compilation failed: ' + $lane.Name) }
        & $binary
        if ($LASTEXITCODE -ne 0) { throw ('secure_test failed: ' + $lane.Name) }
    }
}
finally {
    Pop-Location
}
