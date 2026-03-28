# Creates .venv and installs runtime + dev dependencies (Windows / PowerShell).
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

$Py = Join-Path $Root ".venv\Scripts\python.exe"
& $Py -m pip install --upgrade pip
& $Py -m pip install -r requirements.txt
& $Py -m pip install -r requirements-dev.txt

Write-Host "Done. Activate: .\.venv\Scripts\Activate.ps1"
