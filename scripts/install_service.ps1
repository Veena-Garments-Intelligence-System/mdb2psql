# Install MDB Sync as a Windows Service
# This script uses NSSM (Non-Sucking Service Manager) to install the worker.
# RUN THIS SCRIPT AS ADMINISTRATOR.

# --- CONFIGURATION ---
$NSSM = "C:\tools\nssm.exe" # Path to nssm.exe (UPDATE THIS IF NEEDED)
$SERVICE_NAME = "MDBSyncService"

# --- AUTOMATIC PATH DETECTION ---
$BASE_DIR = Resolve-Path "$PSScriptRoot\.."
$PYTHON_PATH = "$BASE_DIR\.venv\Scripts\python.exe"
$LOG_FILE = "$BASE_DIR\logs\service.log"

if (-not (Test-Path $NSSM)) {
    Write-Error "NSSM not found at $NSSM. Please install it and update the script."
    exit 1
}

if (-not (Test-Path $PYTHON_PATH)) {
    Write-Error "Virtual environment not found at $PYTHON_PATH. Please run 'python -m venv .venv' first."
    exit 1
}

# Ensure logs directory exists
if (-not (Test-Path "$BASE_DIR\logs")) {
    New-Item -ItemType Directory -Path "$BASE_DIR\logs"
}

Write-Host "Installing service $SERVICE_NAME..." -ForegroundColor Cyan

# Remove existing service if it exists
& $NSSM stop $SERVICE_NAME 2>$null
& $NSSM remove $SERVICE_NAME confirm 2>$null

# Install the service
& $NSSM install $SERVICE_NAME $PYTHON_PATH "-m src.mdb_sync.main"
& $NSSM set $SERVICE_NAME AppDirectory $BASE_DIR
& $NSSM set $SERVICE_NAME AppStdout $LOG_FILE
& $NSSM set $SERVICE_NAME AppStderr $LOG_FILE
& $NSSM set $SERVICE_NAME Start SERVICE_AUTO_START

# Configure Service Recovery Settings via sc.exe (Requirement 11)
# Restarts service after 60s (60000ms) on 1st, 2nd, and subsequent failures. Reset failure count after 1 day (86400s)
& sc.exe failure $SERVICE_NAME reset= 86400 actions= restart/60000/restart/60000/restart/60000

# Start the service
& $NSSM start $SERVICE_NAME

Write-Host "Successfully installed and started $SERVICE_NAME." -ForegroundColor Green
Write-Host "Logs are being written to: $LOG_FILE"
