from analysis.query1 import incident_trends_over_time
from analysis.query2 import neighborhood_incident_counts
from analysis.query3 import responde_time_analysis
from etl.extract import extract_data
from etl.load import load_data
from etl.transform import transform_data
from quality.pre_load_check import validate_schema


# This script orchestrates the ETL process and runs the analysis queries.
# It extracts data from the source, transforms it, loads it into a DuckDB database,
# and then performs analysis on the loaded data.
# The analysis includes counting incidents by neighborhood district,
# calculating average response times by battalion, and counting incidents by battalion.
# The results of the analysis are printed to the console.
# The script is designed to be run as a standalone program.
# It imports necessary modules for data extraction, transformation, loading,
# and analysis.
def main():
    """
    Main function to run the ETL process and analysis queries.
    """
    # Step 1: Pre-load check
    # Run the validation function
    missing_cols, extra_cols, mismatched_types = validate_schema()

    # Print results
    if not missing_cols and not extra_cols and not mismatched_types:
        print("====> Schema validation passed.")
    else:
        print(
            f"====> Missing columns: {missing_cols if missing_cols else 'No missing columns'}"
        )
        print(
            f"====> Extra columns: {extra_cols if extra_cols else 'No extra columns'}"
        )
        print(
            f"====> Type mismatches: {mismatched_types if mismatched_types else 'No type mismatches'}"
        )

    # Step 2: Extract data
    extract_data()

    # Step 3: Transform data
    transform_data()

    # Step 4: Load data
    load_data()

    # Step 5: Run analysis queries
    incident_trends_over_time()
    neighborhood_incident_counts()
    responde_time_analysis()


if __name__ == "__main__":
    main()
