import duckdb
import pandas as pd

""""
-- This SQL query retrieves the average response time for each battalion from the fire_incidents_partitioned table.
-- The results are grouped by battalion and ordered by average response time in ascending order.
-- The output will show the battalion and the corresponding average response time.
-- The query is executed in the context of a DuckDB database.

SELECT Battalion, AVG(Arrival DtTm - Alarm DtTm) AS avg_response_time
FROM fire_incidents_partitioned
GROUP BY Battalion
ORDER BY avg_response_time;
"""


def responde_time_analysis():
    """
    This script connects to a DuckDB database, retrieves data from a table,
    and calculates the average response time for each battalion.
    """

    # Connect to DuckDB
    con = duckdb.connect(database="./data/dw/fire_incidents.duckdb", read_only=True)

    # Load data from fire_incidents_partitioned table
    df = con.execute("SELECT * FROM fire_incidents_partitioned").df()

    # Ensure datetime columns are in datetime format
    df["Alarm DtTm"] = pd.to_datetime(df["Alarm DtTm"])
    df["Arrival DtTm"] = pd.to_datetime(df["Arrival DtTm"])

    # Calculate response time in seconds (or as timedelta)
    df["response_time"] = (df["Arrival DtTm"] - df["Alarm DtTm"]).dt.total_seconds()

    # Group by 'battalion' and calculate average response time
    result = (
        df.groupby("Battalion")["response_time"]
        .mean()
        .reset_index(name="avg_response_time")
        .sort_values("avg_response_time")
    )

    # Print the results
    print("====> Average response time by battalion:")
    print("===================================")
    print(result.head(10))  # Display top 10 battalions by average response time
    print("===================================")

    # Close the connection
    con.close()


if __name__ == "__main__":
    responde_time_analysis()
