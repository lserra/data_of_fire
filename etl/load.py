import duckdb
import pandas as pd


def load_data():
    """
    This script loads data from a CSV file into a DuckDB database,
    adds a new column for incident_month, and creates an index for faster searches.
    """
    # Connect to DuckDB
    con = duckdb.connect(
        database="./data/dw/fire_incidents.duckdb",
        read_only=False,
    )

    # Load data from fire_incidents table
    df = con.execute("SELECT * FROM fire_incidents").df()

    # Add a new column for incident_month (truncated to month)
    df["incident_month"] = (
        pd.to_datetime(df["Incident Date"]).dt.to_period("M").dt.to_timestamp()
    )

    # Save as a new table in DuckDB
    con.execute(
        "CREATE OR REPLACE TABLE fire_incidents_partitioned AS SELECT * FROM df"
    )

    # Create index for faster searches (DuckDB SQL, not pandas)
    con.execute(
        "CREATE INDEX idx_fire_district ON fire_incidents_partitioned(neighborhood_district)"
    )

    # Close the connection
    con.close()


if __name__ == "__main__":
    load_data()
