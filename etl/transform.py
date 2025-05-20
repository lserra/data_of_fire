import duckdb
import pandas as pd


def transform_data():
    """
    Transform the data in DuckDB.
    This function connects to the DuckDB database, loads the data,
    performs transformations, and saves the transformed data back to DuckDB.
    """

    # Connect to DuckDB
    con = duckdb.connect(
        database="./data/dw/fire_incidents.duckdb",
        read_only=False,
    )

    # Load data from DuckDB into pandas DataFrame
    df = con.execute("SELECT * FROM fire_incidents").df()

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Convert dates to proper format
    df["Incident Date"] = pd.to_datetime(df["Incident Date"])

    # Handle missing values (fill with defaults or drop)
    df = df.fillna({"Estimated Property Loss": 0, "Estimated Contents Loss": 0})

    # Load transformed data back into DuckDB
    con.execute("CREATE OR REPLACE TABLE fire_incidents AS SELECT * FROM df")

    # Verify the updates
    records = con.execute("SELECT COUNT(*) FROM fire_incidents").fetchall()
    print(f"====> Total Records Has Been Updated: {records[0][0]}")


if __name__ == "__main__":
    transform_data()
