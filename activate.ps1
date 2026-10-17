$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$LocalJdk = Join-Path $ProjectRoot ".java-env\jdk-24"

if (Test-Path "$LocalJdk\bin\java.exe") {
    $env:JAVA_HOME = $LocalJdk
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
} elseif (Test-Path $LocalJdk) {
    $JdkHome = Get-ChildItem -Path $LocalJdk -Directory | Where-Object { Test-Path "$($_.FullName)\bin\java.exe" } | Select-Object -First 1
    if ($JdkHome) {
        $env:JAVA_HOME = $JdkHome.FullName
    } else {
        $env:JAVA_HOME = $LocalJdk
    }
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
}

if (Test-Path "C:\tools\apache-maven-3.9.9\bin") {
    $env:PATH = "C:\tools\apache-maven-3.9.9\bin;$env:PATH"
}

Write-Host "[lazarus-java24] Activated isolated Java 24 environment" -ForegroundColor Green
java -version
