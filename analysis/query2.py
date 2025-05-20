import duckdb
import pandas as pd

""""
-- This SQL query retrieves the number of incidents per neighborhood district from the fire_incidents_partitioned table.
-- The results are grouped by neighborhood_district and ordered by count in descending order.
-- The output will show the neighborhood district and the corresponding count of incidents.
-- The query is executed in the context of a DuckDB database.

SELECT neighborhood_district, COUNT(*)
FROM fire_incidents_partitioned
GROUP BY neighborhood_district
ORDER BY count DESC;
"""


def neighborhood_incident_counts():
    """
    This script connects to a DuckDB database, retrieves data from a table,
    and performs a group by operation to count incidents per neighborhood district.
    """

    # Connect to DuckDB
    con = duckdb.connect(database="./data/dw/fire_incidents.duckdb", read_only=True)

    # Load data from fire_incidents_partitioned table
    df = con.execute("SELECT * FROM fire_incidents_partitioned").df()

    # Group by 'neighborhood_district' and count occurrences
    result = (
        df.groupby("neighborhood_district")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    # Print the results
    print("====> Neighborhood incident counts:")
    print("===================================")
    print(result.head(10))  # Display top 10 neighborhoods by incident count
    print("===================================")

    # Close the connection
    con.close()


if __name__ == "__main__":
    neighborhood_incident_counts()
