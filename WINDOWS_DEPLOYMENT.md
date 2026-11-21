# Windows Deployment Guide

To run the MDB Sync Platform as a persistent background service on Windows that automatically restarts on reboot, follow these steps.

## Prerequisites

1.  **Python 3.12+:** [Download from python.org](https://www.python.org/downloads/windows/).
2.  **Microsoft Access Database Engine:** 
    *   If you have Office 64-bit, you likely already have it.
    *   Otherwise, download the [Microsoft Access Database Engine 2016 Redistributable (64-bit)](https://www.microsoft.com/en-us/download/details.aspx?id=54920).
3.  **NSSM (Non-Sucking Service Manager):** 
    *   Download from [nssm.cc/download](https://nssm.cc/download). 
    *   Extract `nssm.exe` (from the `win64` folder) to a known location, e.g., `C:\tools\nssm.exe`.

## 1. Setup Environment

1.  Clone the repository to your target folder (e.g., `C:\vgis\dbupdater`).
2.  Open PowerShell in that folder.
3.  Create and activate a virtual environment:
    ```powershell
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    pip install -r requirements.txt
    ```
4.  Configure your `.env` file. **Crucial for Windows:**
    ```env
    POSTGRES_URI=postgresql://user:password@localhost:5432/mdb_sync
    MDB_PATH=C:\Path\To\Your\Billing.mdb
    MDB_DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}
    LOG_LEVEL=info
    ```
5.  Initialize the PostgreSQL schema (first-time deployment):
    ```powershell
    python scripts\init_db.py
    ```

## 2. Install as a Windows Service

We use the provided script `scripts/install_service.ps1`. 

1.  Open `scripts/install_service.ps1` in an editor.
2.  Update the `$NSSM` variable path if necessary.
3.  Run PowerShell as **Administrator**.
4.  Execute the script:
    ```powershell
    Set-ExecutionPolicy RemoteSigned -Scope Process
    .\scripts\install_service.ps1
    ```

## 3. Managing the Service

*   **Check Status:** `nssm status MDBSyncService`
*   **Start:** `nssm start MDBSyncService`
*   **Stop:** `nssm stop MDBSyncService`
*   **Edit Configuration:** `nssm edit MDBSyncService`
*   **View Logs:** Check `logs/service.log` in the project folder.

## Troubleshooting

### Driver Issues
If you get an error "Data source name not found", run this in PowerShell to see available drivers:
```powershell
(New-Object System.Data.Odbc.OdbcEnumerator).GetNames() | Select-String "Access"
```
Ensure the name exactly matches what you put in `MDB_DRIVER` in `.env`.

### Database Access
Ensure the user account running the service (default is `LocalSystem`) has read/write permissions to the `.mdb` file and the project folder. 
