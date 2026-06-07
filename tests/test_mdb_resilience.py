import pytest
import pyodbc
from unittest.mock import MagicMock, patch
from src.mdb_sync.infrastructure.mdb.repository import MDBRepository

@patch("time.sleep", return_value=None)
@patch("pyodbc.connect")
def test_mdb_retry_on_hy000(mock_connect, mock_sleep):
    repo = MDBRepository()
    repo.conn_str = "DRIVER={Test};DBQ=test.mdb"
    
    # Mock connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    # First attempt: connection success, execute failure with HY000
    # Second attempt: connection success, fetchone failure with HY000
    # Third attempt: full success
    
    mock_cursor.execute.side_effect = [
        pyodbc.Error("HY000", "The driver did not supply an error!"),
        None,
        None
    ]
    
    mock_cursor.fetchone.side_effect = [
        pyodbc.Error("HY000", "Fetch failure"),
        ("val1",),
        None
    ]
    
    mock_cursor.description = [("col1",)]
    
    mock_connect.return_value = mock_conn
    
    # We need to wrap it because execute_query_yield is a generator
    results = list(repo.execute_query_yield("SELECT * FROM Test"))
    
    assert len(results) == 1
    assert results[0]["col1"] == "val1"
    
    # connect should be called 3 times due to retries
    assert mock_connect.call_count == 3

@patch("time.sleep", return_value=None)
@patch("pyodbc.connect")
def test_mdb_max_retries_exhausted(mock_connect, mock_sleep):
    repo = MDBRepository()
    
    # Always fail with HY000
    mock_connect.side_effect = pyodbc.Error("HY000", "Persistent failure")
    
    with pytest.raises(pyodbc.Error) as excinfo:
        list(repo.execute_query_yield("SELECT * FROM Test"))
    
    assert "HY000" in str(excinfo.value)
    # 3 attempts
    assert mock_connect.call_count == 3
