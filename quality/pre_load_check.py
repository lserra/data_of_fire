import pandas as pd

expected_schema = {
    "Incident Number": "INT",
    "Incident Date": "TIMESTAMP",
    "Address": "TEXT",
    "City": "TEXT",
    "zipcode": "TEXT",
    "Battalion": "TEXT",
    "Primary Situation": "TEXT",
}


# Load CSV into Pandas DataFrame
df = pd.read_csv("./data/source/fire_incidents_20250520.csv")


def validate_schema():
    """Check if the DataFrame matches the expected schema"""
    actual_columns = set(df.columns)
    expected_columns = set(expected_schema.keys())

    # Check for missing or extra columns
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns

    # Check data types
    type_mismatches = {}
    for col, expected_type in expected_schema.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if "int" in actual_type and expected_type != "INT":
                type_mismatches[col] = actual_type
            elif "float" in actual_type and expected_type != "NUMERIC":
                type_mismatches[col] = actual_type
            elif "datetime" in actual_type and expected_type != "TIMESTAMP":
                type_mismatches[col] = actual_type

    return missing_columns, extra_columns, type_mismatches


if __name__ == "__main__":
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
