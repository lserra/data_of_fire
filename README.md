# Data of Fire

This is an interesting challenge for data engineering and thank you so much for this opportunity!

## Thoughts

To approach this, I'll need to ensure data integrity, efficient query performance, and a well-structured model. Here's how I could tackle it:

### Data Pipeline and Warehousing Strategy

#### Extract, Transform, Load (ETL) or ELT Approach

- Use an ETL tool or scripting language (like Python with Pandas or Spark) to extract data from the source.

- Consider an ELT approach if I'm leveraging cloud-based data warehouses like BigQuery or Snowflake.

#### Daily Data Sync

- Use a scheduling tool like Apache Airflow or database-native scheduling (like Snowflake Tasks) to ensure the dataset is updated daily in the warehouse.

- Implement change data capture (CDC) if updates occur frequently.

### Data Model Design

To support efficient dynamic queries across time period, district, and battalion, I should design a schema with the following considerations:

- **Fact Table**: Contains fire incidents with timestamps, district IDs, and battalion IDs.

- **Dimension Tables**:

  - **Time Dimension**: Stores date, month, year, and relevant time hierarchies.

  - **District Dimension**: Stores district-level attributes and mapping.

  - **Battalion Dimension**: Stores battalion details.

A **star schema** or **snowflake schema** would work well depending on query complexity.

### Optimization for Query Performance

- **Partitioning & Clustering**: Partition tables by date and cluster by district and battalion to optimize aggregation.

- **Indexing**: Use appropriate indexing strategies depending on the database engine (e.g., B-tree indexes in PostgreSQL, clustering keys in Snowflake).

- **Materialized Views**: If query performance is critical, create pre-aggregated views for rapid retrieval.

## Reality

**RESTRICTION**: My free time available to this challenge was very short.

So, for this reason, I decided **DO NOT** create a star schema or snowflake schema, or apply technics like CDC, or use Apache Airflow or DBT tools, simply to optimize my developing time.
I am using here, just Python, Pandas, DuckDB, and Docker.
Also, I am NOT using here, environment variables for credentials and configurations.

## Why DuckDB?

There is no DBMS server software to install, update and maintain. DuckDB does not run as a separate process, but completely embedded within a host process. For the analytical use cases that DuckDB targets, this has the additional advantage of high-speed data transfer to and from the database. In some cases, DuckDB can process foreign data without copying. For example, the DuckDB Python package can run queries directly on Pandas data without ever importing or copying any data.
For more information about DuckDB, please visit [duckdb.org](https://duckdb.org/why_duckdb).

## The repository's structure

```text
DATA_OF_FIRE/
├── analisys/
│ |  └── query1.py
│ |  └── query2.py
│ |  └── query3.py
├── data/
│ |  └── dw/
│ |     ├── fire_incidents.duckdb
│ |  └── source/
│ |     ├── fire_incidents_20250520.csv
├── etl/
│ |  ├── extract.py
│ |  ├── load.py
│ |  ├── transform.py
├── quality/
│ |  ├── pre_load_check.py
├── .dockerignore
├── .gitignore
├── dockerfile
├── LICENSE
├── README.md
├── requirements.txt
└── run.py
└── start_app.sh
```

## ETL Process Overview

All scripts can be find out in the `etl` folder.

- Extract (`extract.py`): This script extracts data from a CSV file and loads it into a DuckDB database.

- Transform (`transform.py`): Clean, normalize, filter, and enhance the data.

- Load (`load.py`): Store the structured data efficiently in DuckDB.

## Final Considerations

A few thoughs for all additional requirements add excellent structure and robustness to this data pipeline in the real world. Here's how I can effectively implement each:

1-Modularization of Script (ETL Separation)

- Extraction Module: Separate file/function to fetch raw data from the source.

- Transformation Module: Cleans, transforms, and prepares data before ingestion.

- Loading Module: Handles data insertion into the data warehouse.

**I would consider using Python with Airflow DAGs to orchestrate these tasks.**

2-Data Quality Checks

- Pre-load checks: Validate schema conformity, null values, and data accuracy.

- Post-load checks: Ensure data integrity, completeness, and consistency.

**I would consider using tools like Great Expectations or implement custom SQL validation queries.**

3-Secure Credentials & Configurations

- Store sensitive credentials in environment variables or use AWS Secrets Manager / Azure Key Vault for security.

**Avoid hardcoding credentials in the script—use a .env file with libraries like dotenv.**

4-Deduplication Logic

- Implement primary keys & constraints at the database level.

- Use ROW_NUMBER(), DISTINCT, or GROUP BY to eliminate duplicate entries before insertion.

**I would consider using hashing techniques to identify duplicate records efficiently.**

5-Filtering Data by Date

- Load only new or updated records using WHERE date_column >= CURRENT_DATE - INTERVAL 'X' DAY.

**I would consider using partitioning strategies for time-based data.**

6-Conflict Handling

- ON CONFLICT DO NOTHING: Useful for ignoring duplicates without failure.

- ON CONFLICT UPDATE: Ensures updates for existing records instead of re-insertion.

**I would consider using `MERGE` statements for better control over update/insert conditions.**

## How does it work?

To run Dockerfile and start the application, follow these steps from the project root directory (where dockerfile is located):

- Build the Docker image:

```bash
docker build -t data_of_fire .
```

- Run the Docker container:

```bash
docker run --rm -it data_of_fire
```

This will execute the `start_app.sh` script after container has been created.

## Expected Results

```bash
=========================== [ DATA  OF  FIRE ] ===================================
==================================================================================

>>> STARTING APPLICATION . . .
====> Missing columns: No missing columns
====> Extra columns: {'Item First Ignited', 'Fire Fatalities', 'Automatic Extinguishing Sytem Type', 'Suppression Units', 'Automatic Extinguishing Sytem Failure Reason', 'Suppression Personnel', 'Alarm DtTm', 'Action Taken Other', 'Mutual Aid', 'Detector Alerted Occupants', 'Exposure Number', 'Fire Spread', 'Detector Type', 'Fire Injuries', 'Detectors Present', 'Ignition Cause', 'Floor of Fire Origin', 'ID', 'Area of Fire Origin', 'Civilian Injuries', 'Detector Failure Reason', 'First Unit On Scene', 'Estimated Contents Loss', 'Number of floors with minimum damage', 'Property Use', 'Number of Alarms', 'Other Units', 'Box', 'Ignition Factor Secondary', 'Call Number', 'Close DtTm', 'Number of floors with heavy damage', 'Automatic Extinguishing Sytem Perfomance', 'Other Personnel', 'Structure Type', 'Number of Sprinkler Heads Operating', 'Automatic Extinguishing System Present', 'neighborhood_district', 'Action Taken Secondary', 'Structure Status', 'Number of floors with extreme damage', 'Detector Operation', 'data_as_of', 'Ignition Factor Primary', 'Arrival DtTm', 'point', 'No Flame Spread', 'Supervisor District', 'Detector Effectiveness', 'EMS Personnel', 'Estimated Property Loss', 'Civilian Fatalities', 'Action Taken Primary', 'data_loaded_at\t\t\t\t', 'EMS Units', 'Station Area', 'Number of floors with significant damage', 'Human Factors Associated with Ignition', 'Heat Source'}
====> Type mismatches: {'zipcode': 'int64'}
====> Total Records Has Been Loaded: 199
====> Total Records Has Been Updated: 199
====> Incident trends over time:
===================================
  incident_month  count
0     2023-03-01    199
===================================
====> Neighborhood incident counts:
===================================
             neighborhood_district  count
29                 Sunset/Parkside     18
15                         Mission     15
0            Bayview Hunters Point     15
30                      Tenderloin     11
4                        Excelsior     10
5   Financial District/South Beach     10
2              Castro/Upper Market      9
20      Oceanview/Merced/Ingleside      9
28                 South of Market      8
17                        Nob Hill      8
===================================
====> Average response time by battalion:
===================================
  Battalion  avg_response_time
3       B04         244.476190
1       B02         275.681818
8       B09         277.958333
2       B03         290.500000
5       B06         322.050000
0       B01         336.000000
7       B08         344.200000
4       B05         349.500000
6       B07         399.769231
9       B10         424.650000
===================================
```
