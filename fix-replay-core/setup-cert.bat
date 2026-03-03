@echo off
REM SSL Certificate Setup Script for Corporate Networks

echo ========================================
echo SSL Certificate Setup
echo ========================================
echo.

REM Create certs directory
if not exist "certs" (
    mkdir certs
    echo Created certs directory
)

echo.
echo This script will help you import your corporate SSL certificate.
echo.
echo Prerequisites:
echo 1. Export your corporate certificate as corporate-cert.cer
echo 2. Place it in the fix-replay-core directory
echo.
echo To export certificate on Windows:
echo   - Open certmgr.msc
echo   - Navigate to: Trusted Root Certification Authorities ^> Certificates
echo   - Find your corporate certificate
echo   - Right-click ^> All Tasks ^> Export
echo   - Choose: Base-64 encoded X.509 (.CER)
echo   - Save as: corporate-cert.cer
echo.

REM Check if certificate file exists
if not exist "corporate-cert.cer" (
    echo ERROR: corporate-cert.cer not found in current directory
    echo.
    echo Please export your corporate certificate and place it here, then run this script again.
    pause
    exit /b 1
)

echo Found corporate-cert.cer
echo.

REM Find Java installation
echo Locating Java installation...

REM Try to find java.exe
where java >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Java not found in PATH
    echo.
    echo Please ensure Java is installed and in your PATH
    pause
    exit /b 1
)

REM Get Java home from java.exe location
for /f "tokens=*" %%i in ('where java') do set JAVA_EXE=%%i
for %%i in ("%JAVA_EXE%") do set JAVA_BIN=%%~dpi
for %%i in ("%JAVA_BIN:~0,-1%") do set JAVA_HOME=%%~dpi
set JAVA_HOME=%JAVA_HOME:~0,-1%

echo Found Java at: %JAVA_EXE%
echo Java Home: %JAVA_HOME%

REM Find keytool
set KEYTOOL=%JAVA_HOME%\bin\keytool.exe

if not exist "%KEYTOOL%" (
    echo ERROR: keytool not found at: %KEYTOOL%
    echo.
    echo Trying alternative location...
    set KEYTOOL=keytool
)

echo Found keytool
echo.
echo Importing certificate into Java truststore...
echo.

REM Import certificate
"%KEYTOOL%" -import -trustcacerts -alias corporate-cert ^
  -file corporate-cert.cer ^
  -keystore certs/corporate-truststore.jks ^
  -storepass changeit ^
  -noprompt

if %ERRORLEVEL% equ 0 (
    echo.
    echo ========================================
    echo SUCCESS: Certificate imported!
    echo ========================================
    echo.
    echo Truststore created at: certs/corporate-truststore.jks
    echo.
    echo Next steps:
    echo 1. Edit gradle.properties
    echo 2. Uncomment these lines:
    echo    systemProp.javax.net.ssl.trustStore=certs/corporate-truststore.jks
    echo    systemProp.javax.net.ssl.trustStorePassword=changeit
    echo    systemProp.javax.net.ssl.trustStoreType=JKS
    echo.
    echo 3. Run: gradlew.bat clean build
    echo.
    echo The certificate file and truststore are in .gitignore and won't be committed.
) else (
    echo.
    echo ========================================
    echo ERROR: Failed to import certificate
    echo ========================================
    echo.
    echo Possible issues:
    echo - Invalid certificate file
    echo - Certificate already exists (try deleting certs\corporate-truststore.jks)
    echo - Permission issues
    echo.
    echo Try running this script as Administrator
)

echo.
pause
