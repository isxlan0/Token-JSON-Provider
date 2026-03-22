@echo off
setlocal
goto :main

:build_target
set "TARGET_OS=%~1"
set "TARGET_ARCH=%~2"
set "TARGET_NAME=%~3"
set "OUTPUT_PATH=%CD%\bin\%TARGET_NAME%"

set "GOOS=%TARGET_OS%"
set "GOARCH=%TARGET_ARCH%"
go build -o "%OUTPUT_PATH%" .\cmd\server
set "BUILD_ERROR=%ERRORLEVEL%"
set "GOOS="
set "GOARCH="
exit /b %BUILD_ERROR%

:main
cd /d "%~dp0"

if not exist bin mkdir bin
del /f /q "bin\token-atlas.exe" "bin\token-atlas" >nul 2>nul
if exist "bin\token-atlas.exe" (
echo Failed to remove legacy bin\token-atlas.exe. 1>&2
exit /b 1
)
if exist "bin\token-atlas" (
echo Failed to remove legacy bin\token-atlas. 1>&2
exit /b 1
)
if exist bin\dist (
attrib -r "bin\dist\*" /s /d >nul 2>nul
del /f /q /s "bin\dist\*" >nul 2>nul
for /d %%D in ("bin\dist\*") do rd /s /q "%%~fD" >nul 2>nul
rd /s /q "bin\dist" >nul 2>nul
if exist bin\dist (
echo Failed to remove legacy bin\dist directory. 1>&2
exit /b 1
)
)

call :build_target windows amd64 token-atlas-windows-amd64.exe
if errorlevel 1 exit /b %errorlevel%
call :build_target windows arm64 token-atlas-windows-arm64.exe
if errorlevel 1 exit /b %errorlevel%
call :build_target linux amd64 token-atlas-linux-amd64
if errorlevel 1 exit /b %errorlevel%
call :build_target linux arm64 token-atlas-linux-arm64
if errorlevel 1 exit /b %errorlevel%
call :build_target darwin amd64 token-atlas-macos-amd64
if errorlevel 1 exit /b %errorlevel%
call :build_target darwin arm64 token-atlas-macos-arm64
if errorlevel 1 exit /b %errorlevel%

echo Built platform binaries in %CD%\bin
exit /b 0
