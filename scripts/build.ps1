param([string]$Compiler = "")
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
python -m PyInstaller --noconfirm --clean --windowed --onedir --name WANBatchStudio --collect-data tzdata --hidden-import PIL.ImageTk --hidden-import websocket --add-data "LICENSE;." --add-data "README.txt;." app.py
if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed" }
python scripts/collect_licenses.py
if ($LASTEXITCODE -ne 0) { throw "License collection failed" }
if (-not $Compiler) {
    $candidates = @("$env:LOCALAPPDATA\Programs\Inno Setup 6\ISCC.exe", "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe")
    $Compiler = $candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    if (-not $Compiler) { $Compiler = (Get-Command iscc.exe -ErrorAction Stop).Source }
}
& $Compiler packaging\installer.iss
if ($LASTEXITCODE -ne 0) { throw "Installer compilation failed" }
$installer = Get-Item "release\WAN-Batch-Studio-Setup-1.0.0-x64.exe"
$hash = (Get-FileHash -LiteralPath $installer.FullName -Algorithm SHA256).Hash.ToLower()
[IO.File]::WriteAllText((Join-Path $installer.DirectoryName "SHA256SUMS.txt"), "$hash  $($installer.Name)`n")
Write-Output "Installer: $($installer.FullName)"
