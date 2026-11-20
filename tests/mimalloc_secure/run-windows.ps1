# The secure mimalloc lanes on Windows x64 (tests/mimalloc_secure/Makefile and
# tests/ant_uv's run-mimalloc-secure on Linux): the fast archive, the secure
# archive from scripts/build-mimalloc-secure.py (MI_SECURE=CHECKED, no static
# guard, symbols renamed afsec_, /INCLUDE: directives rewritten), then
# secure_test with its unittests linked with both, with the secure one alone,
# and without the option; last, the ant_uv suite with network memory and
# libuv's allocator in the secure instance beside the fast one, when the
# pinned libuv of scripts/build-libuv.py is built.
#
# What a run prints (v3.7.0, MSVC 14.51, 2026-11-20):
# - the build script's last line: 607 symbols renamed (Linux: 603; Windows
#   has its TLS and CRT callbacks and TLS slot indices, Linux thread-locals
#   and a pthread key), 16 linker directives rewritten (the
#   /INCLUDE: strings in prim.c that keep those callbacks), 0 unprefixed
#   definitions or references. MSVC builds the fast archive as C++ and the
#   secure one as C (the script passes MI_USE_CXX=OFF); the shared
#   definitions are MSVC's string and floating-point COMDAT constants and
#   three select-any header inlines. The script stops and lists any other
#   defined symbol that is not a mimalloc name (pass -Allow to accept one
#   deliberately);
# - every secure_test line PASS, in particular "each of them was released at
#   thread exit", which needs the renamed instance's TLS callbacks, and the
#   sealed process pages read-only (protection 0x2 0x2 0x4);
# - the ant_uv suite's "network memory: mimalloc-secure, libuv included".
param(
    [ValidateSet('dmd', 'ldc2')]
    [string] $Compiler = 'ldc2',
    [string] $CMake = 'cmake',
    [string] $Python = 'python',
    [string[]] $Allow = @(),
    # A libuv installation (include/uv.h, lib/uv.lib, bin/uv.dll) for the
    # ant_uv lane; default: the pinned build.
    [string] $Libuv = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$fastDir = Join-Path $repoRoot 'tests/actors/.build/mimalloc-v3.7.0-windows-release'
$fastLib = Join-Path $fastDir 'lib/mimalloc.lib'
$secureDir = Join-Path $repoRoot 'tests/actors/.build/mimalloc-v3.7.0-secure-windows-release'
$secureLib = Join-Path $secureDir 'mimalloc-secure.lib'
$outDir = Join-Path $PSScriptRoot '.build'
$crt = '-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded'  # the D toolchains' static release CRT
if (-not $Libuv) { $Libuv = Join-Path $repoRoot '.build/libuv/pinned' }
$previousPath = $env:PATH

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
        $dArgs = @('-g', '-unittest', '-I.', '-mscrtlib=libcmt') + @($lane.Versions | ForEach-Object { $v + $_ }) +
            $sources + $lane.Libs + @("-of=$binary", ('-od=' + (Join-Path $outDir ("obj-${Compiler}-" + $lane.Name))))
        & $Compiler @dArgs
        if ($LASTEXITCODE -ne 0) { throw ('secure_test compilation failed: ' + $lane.Name) }
        & $binary --DRT-testmode=run-main
        if ($LASTEXITCODE -ne 0) { throw ('secure_test failed: ' + $lane.Name) }
    }

    # ant_uv with network memory in the secure instance: receive windows,
    # outboxes, socket handles, and libuv's own allocations through
    # uv_replace_allocator, which reaches uv.dll's allocator from the D image.
    $uvLib = Join-Path $Libuv 'lib/uv.lib'
    if (-not ((Test-Path (Join-Path $Libuv 'include/uv.h')) -and (Test-Path $uvLib) -and
            (Test-Path (Join-Path $Libuv 'bin/uv.dll')))) {
        Write-Output "SKIP ant_uv secure: no libuv installation at $Libuv (python -B scripts/build-libuv.py)"
    }
    else {
        # The layout oracle is C against the real uv.h, as in check-windows.py.
        $layout = Join-Path $outDir 'uv_layout.obj'
        $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
        $vs = & $vswhere -latest -products '*' -property installationPath
        $vcvars = Join-Path $vs 'VC/Auxiliary/Build/vcvars64.bat'
        & cmd.exe /d /c ("call `"$vcvars`" >nul && cl /nologo /O1 /W4 /WX /MT /c tests/ant_uv/layout.c " +
            "/external:I `"$(Join-Path $Libuv 'include')`" /external:W0 `"/Fo$layout`"")
        if ($LASTEXITCODE -ne 0) { throw 'layout oracle compilation failed' }
        $binary = Join-Path $outDir "uv_test_mimalloc_secure_$Compiler.exe"
        $dArgs = @('-g', '-i', '-I.', '-Ithreadpool/source', '-Iant_fibers/source', '-Iant_uv/source',
            '-Iforkjoin/source', '-mscrtlib=libcmt', ($v + 'AntfarmMimallocV3'), ($v + 'AntfarmMimallocSecure'),
            'tests/ant_uv/uv_test.d', $layout, $fastLib, $secureLib, $uvLib,
            "-of=$binary", ('-od=' + (Join-Path $outDir "obj-uv-$Compiler")))
        & $Compiler @dArgs
        if ($LASTEXITCODE -ne 0) { throw 'ant_uv secure compilation failed' }
        $env:PATH = (Join-Path $Libuv 'bin') + [IO.Path]::PathSeparator + $previousPath
        & $binary
        if ($LASTEXITCODE -ne 0) { throw 'ant_uv secure suite failed' }
    }
}
finally {
    $env:PATH = $previousPath
    Pop-Location
}
