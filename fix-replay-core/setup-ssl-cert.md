# SSL Certificate Setup for Corporate Networks

This guide helps you configure SSL certificates for local development while keeping GitLab CI compatibility.

## Problem

Corporate networks often use SSL interception (MITM proxy) which causes:
- `PKIX path building failed` errors
- `unable to find valid certification path to requested target` errors
- Failed downloads from Maven Central, Gradle services, etc.

## Solution Options

### Option 1: Export and Use Corporate Certificate (Recommended)

This approach works locally and doesn't affect GitLab CI.

#### Step 1: Export Corporate Certificate

**Windows:**
```cmd
REM Open Certificate Manager
certmgr.msc

REM Navigate to: Trusted Root Certification Authorities > Certificates
REM Find your corporate certificate (usually company name)
REM Right-click > All Tasks > Export
REM Choose: Base-64 encoded X.509 (.CER)
REM Save as: corporate-cert.cer
```

**Or use PowerShell:**
```powershell
# List certificates
Get-ChildItem -Path Cert:\CurrentUser\Root | Where-Object {$_.Subject -like "*YourCompany*"}

# Export certificate (replace thumbprint)
$cert = Get-ChildItem -Path Cert:\CurrentUser\Root\<THUMBPRINT>
Export-Certificate -Cert $cert -FilePath corporate-cert.cer
```

#### Step 2: Create Java Truststore

```cmd
cd fix-replay-core
mkdir certs

REM Import certificate into new truststore
keytool -import -trustcacerts -alias corporate-cert ^
  -file corporate-cert.cer ^
  -keystore certs/corporate-truststore.jks ^
  -storepass changeit ^
  -noprompt
```

#### Step 3: Configure Gradle

Edit `fix-replay-core/gradle.properties`:
```properties
systemProp.javax.net.ssl.trustStore=certs/corporate-truststore.jks
systemProp.javax.net.ssl.trustStorePassword=changeit
systemProp.javax.net.ssl.trustStoreType=JKS
```

#### Step 4: Add to .gitignore

The `certs/` directory should NOT be committed (already in .gitignore):
```
# In root .gitignore
fix-replay-core/certs/
*.cer
*.jks
```

### Option 2: Use System Truststore

Configure Gradle to use the system's Java truststore:

**Find Java truststore location:**
```cmd
REM Windows
echo %JAVA_HOME%\lib\security\cacerts

REM Or
where java
REM Then navigate to: <JAVA_HOME>\lib\security\cacerts
```

**Configure in gradle.properties:**
```properties
systemProp.javax.net.ssl.trustStore=C:/Program Files/Java/jdk-17/lib/security/cacerts
systemProp.javax.net.ssl.trustStorePassword=changeit
```

### Option 3: Import Certificate into Java's Default Truststore

**Windows (Run as Administrator):**
```cmd
cd %JAVA_HOME%\lib\security

REM Backup original
copy cacerts cacerts.backup

REM Import corporate certificate
keytool -import -trustcacerts -alias corporate-cert ^
  -file C:\path\to\corporate-cert.cer ^
  -keystore cacerts ^
  -storepass changeit ^
  -noprompt
```

**Verify:**
```cmd
keytool -list -keystore cacerts -storepass changeit | findstr corporate
```

### Option 4: Configure Proxy (If Applicable)

If your network uses a proxy, configure in `gradle.properties`:
```properties
systemProp.http.proxyHost=proxy.company.com
systemProp.http.proxyPort=8080
systemProp.https.proxyHost=proxy.company.com
systemProp.https.proxyPort=8080

# If proxy requires authentication
systemProp.http.proxyUser=username
systemProp.http.proxyPassword=password
systemProp.https.proxyUser=username
systemProp.https.proxyPassword=password
```

## GitLab CI Compatibility

### Approach 1: Environment-Specific Configuration (Recommended)

Use different configurations for local vs CI:

**Local: `gradle.properties`** (not committed or in .gitignore)
```properties
systemProp.javax.net.ssl.trustStore=certs/corporate-truststore.jks
systemProp.javax.net.ssl.trustStorePassword=changeit
```

**CI: `.gitlab-ci.yml`** (uses default truststore)
```yaml
verify:
  image: eclipse-temurin:17
  script:
    - cd fix-replay-core
    - ./gradlew clean build
```

GitLab runners typically don't have corporate SSL interception, so they work without custom certificates.

### Approach 2: Conditional Configuration

Create `gradle.properties` with conditional logic:

```properties
# Only apply custom truststore if file exists
systemProp.javax.net.ssl.trustStore=${user.dir}/certs/corporate-truststore.jks
systemProp.javax.net.ssl.trustStorePassword=changeit
```

In CI, the file won't exist, so it falls back to default.

## Testing

After configuration, test the build:

```cmd
cd fix-replay-core

REM Clean build
.\gradlew.bat clean build

REM Verify dependencies download
.\gradlew.bat dependencies --configuration compileClasspath
```

## Troubleshooting

### Still Getting SSL Errors?

1. **Verify certificate is correct:**
   ```cmd
   keytool -list -keystore certs/corporate-truststore.jks -storepass changeit
   ```

2. **Check if multiple certificates needed:**
   Some corporate networks use certificate chains. Export and import all certificates in the chain.

3. **Verify Java version:**
   ```cmd
   java -version
   .\gradlew.bat --version
   ```
   Ensure they match.

4. **Enable SSL debugging:**
   ```cmd
   set GRADLE_OPTS=-Djavax.net.debug=ssl:handshake
   .\gradlew.bat build
   ```

### Certificate Expired?

Corporate certificates may expire. Re-export and import the new certificate.

## Security Notes

- **Never commit certificates or truststores** to version control
- **Never commit passwords** in plain text
- Use environment variables for sensitive data in CI/CD
- The `changeit` password is Java's default - consider changing it for production

## Quick Setup Script

Create `fix-replay-core/setup-cert.bat`:

```batch
@echo off
echo SSL Certificate Setup for Corporate Networks
echo.

if not exist "certs" mkdir certs

echo Step 1: Place your corporate-cert.cer in the current directory
echo Step 2: Press any key to import it into truststore
pause

if not exist "corporate-cert.cer" (
    echo ERROR: corporate-cert.cer not found
    exit /b 1
)

keytool -import -trustcacerts -alias corporate-cert ^
  -file corporate-cert.cer ^
  -keystore certs/corporate-truststore.jks ^
  -storepass changeit ^
  -noprompt

if %ERRORLEVEL% equ 0 (
    echo.
    echo SUCCESS: Certificate imported
    echo.
    echo Next steps:
    echo 1. Edit gradle.properties and uncomment the trustStore lines
    echo 2. Run: gradlew.bat clean build
) else (
    echo.
    echo ERROR: Failed to import certificate
)

pause
```

## Alternative: Disable SSL Verification (NOT RECOMMENDED)

Only for local development/testing:

```properties
# gradle.properties
systemProp.javax.net.ssl.trustAll=true
org.gradle.unsafe.disable-ssl-validation=true
```

**WARNING:** This disables all SSL verification and should NEVER be used in production or committed to version control.
