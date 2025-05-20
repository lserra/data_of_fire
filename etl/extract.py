import duckdb
import pandas as pd


def extract_data():
    """This script extracts data from a CSV file and loads it into a DuckDB database."""

    # Connect to DuckDB
    con = duckdb.connect(
        database="./data/dw/fire_incidents.duckdb",
        read_only=False,
    )

    # Load the CSV into Pandas DataFrame
    df = pd.read_csv("./data/source/fire_incidents_20250520.csv")

    # Store DataFrame into DuckDB
    con.execute("DROP TABLE IF EXISTS fire_incidents")
    con.execute("CREATE TABLE fire_incidents AS SELECT * FROM df")

    # Verify loading success
    records = con.execute("SELECT COUNT(*) FROM fire_incidents").fetchall()
    print(f"====> Total Records Has Been Loaded: {records[0][0]}")

    # Close the connection
    con.close()


if __name__ == "__main__":
    extract_data()
