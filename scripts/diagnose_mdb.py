import pyodbc
from src.mdb_sync.config import settings

def check_table(table_name):
    print(f"\nChecking table: {table_name}")
    try:
        with pyodbc.connect(settings.mdb_connection_string, readonly=True) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
                columns = [column[0] for column in cursor.description]
                print(f"Columns: {columns}")
                row = cursor.fetchone()
                if row:
                    print(f"Sample row: {dict(zip(columns, row))}")
                else:
                    print("No rows found.")
            except Exception as e:
                print(f"Error reading table {table_name}: {e}")
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    check_table("ReturnGoods")
    check_table("ReturnsGood_master")
