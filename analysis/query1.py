import duckdb
import pandas as pd

"""
-- This SQL query retrieves the number of incidents per month from the fire_incidents_partitioned table.
-- The results are grouped by incident_month and ordered by incident_month.
-- The output will show the month and the corresponding count of incidents.
-- The query is executed in the context of a DuckDB database.

SELECT incident_month, COUNT(*) 
FROM fire_incidents_partitioned 
GROUP BY incident_month 
ORDER BY incident_month;
"""


def incident_trends_over_time():
    """
    This script connects to a DuckDB database, retrieves data from a table,
    and performs a group by operation to count incidents per month.
    """

    # Connect to DuckDB
    con = duckdb.connect(database="./data/dw/fire_incidents.duckdb", read_only=True)

    # Load data from fire_incidents_partitioned table
    df = con.execute("SELECT * FROM fire_incidents_partitioned").df()

    # Group by 'incident_month' and count occurrences
    result = (
        df.groupby("incident_month")
        .size()
        .reset_index(name="count")
        .sort_values("incident_month")
    )

    # Print the results
    print("====> Incident trends over time:")
    print("===================================")
    print(result.head())
    print("===================================")

    # Close the connection
    con.close()


if __name__ == "__main__":
    incident_trends_over_time()
