# SSL Certificate Setup Script for Corporate Networks
# PowerShell version with automatic Java detection

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "SSL Certificate Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Create certs directory
if (-not (Test-Path "certs")) {
    New-Item -ItemType Directory -Path "certs" | Out-Null
    Write-Host "Created certs directory" -ForegroundColor Green
}

Write-Host ""
Write-Host "This script will help you import your corporate SSL certificate." -ForegroundColor Yellow
Write-Host ""
Write-Host "Prerequisites:" -ForegroundColor Yellow
Write-Host "1. Export your corporate certificate as corporate-cert.cer"
Write-Host "2. Place it in the fix-replay-core directory"
Write-Host ""
Write-Host "To export certificate on Windows:" -ForegroundColor Yellow
Write-Host "  - Open certmgr.msc"
Write-Host "  - Navigate to: Trusted Root Certification Authorities > Certificates"
Write-Host "  - Find your corporate certificate"
Write-Host "  - Right-click > All Tasks > Export"
Write-Host "  - Choose: Base-64 encoded X.509 (.CER)"
Write-Host "  - Save as: corporate-cert.cer"
Write-Host ""

# Check if certificate file exists
if (-not (Test-Path "corporate-cert.cer")) {
    Write-Host "ERROR: corporate-cert.cer not found in current directory" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please export your corporate certificate and place it here, then run this script again." -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Found corporate-cert.cer" -ForegroundColor Green
Write-Host ""

# Find Java installation
Write-Host "Locating Java installation..." -ForegroundColor Cyan

$javaExe = $null
$keytoolPath = $null

# Try to find java.exe
try {
    $javaExe = (Get-Command java -ErrorAction Stop).Source
    Write-Host "Found Java at: $javaExe" -ForegroundColor Green
    
    # Get Java home directory
    $javaHome = Split-Path -Parent (Split-Path -Parent $javaExe)
    Write-Host "Java Home: $javaHome" -ForegroundColor Green
    
    # Find keytool
    $keytoolPath = Join-Path $javaHome "bin\keytool.exe"
    
    if (-not (Test-Path $keytoolPath)) {
        Write-Host "ERROR: keytool not found at: $keytoolPath" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "Found keytool at: $keytoolPath" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Java not found in PATH" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please ensure Java is installed and in your PATH" -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "Importing certificate into Java truststore..." -ForegroundColor Cyan
Write-Host ""

# Import certificate
$certFile = "corporate-cert.cer"
$keystoreFile = "certs\corporate-truststore.jks"
$alias = "corporate-cert"
$storepass = "changeit"

$arguments = @(
    "-import",
    "-trustcacerts",
    "-alias", $alias,
    "-file", $certFile,
    "-keystore", $keystoreFile,
    "-storepass", $storepass,
    "-noprompt"
)

try {
    $process = Start-Process -FilePath $keytoolPath -ArgumentList $arguments -Wait -NoNewWindow -PassThru
    
    if ($process.ExitCode -eq 0) {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "SUCCESS: Certificate imported!" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
        Write-Host ""
        Write-Host "Truststore created at: $keystoreFile" -ForegroundColor Green
        Write-Host ""
        Write-Host "Next steps:" -ForegroundColor Yellow
        Write-Host "1. Edit gradle.properties"
        Write-Host "2. Uncomment these lines:"
        Write-Host "   systemProp.javax.net.ssl.trustStore=certs/corporate-truststore.jks" -ForegroundColor Cyan
        Write-Host "   systemProp.javax.net.ssl.trustStorePassword=changeit" -ForegroundColor Cyan
        Write-Host "   systemProp.javax.net.ssl.trustStoreType=JKS" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "3. Run: .\gradlew.bat clean build" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "The certificate file and truststore are in .gitignore and won't be committed." -ForegroundColor Gray
    } else {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Red
        Write-Host "ERROR: Failed to import certificate" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        Write-Host ""
        Write-Host "Exit code: $($process.ExitCode)" -ForegroundColor Red
        Write-Host ""
        Write-Host "Possible issues:" -ForegroundColor Yellow
        Write-Host "- Invalid certificate file"
        Write-Host "- Certificate already exists (try deleting $keystoreFile)"
        Write-Host "- Permission issues"
    }
} catch {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "ERROR: Failed to run keytool" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "Error: $_" -ForegroundColor Red
}

Write-Host ""
Read-Host "Press Enter to exit"
