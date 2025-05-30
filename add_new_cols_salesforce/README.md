# Avro Schema Merger and Iceberg DDL Generator

This utility script allows you to merge Avro schema files and generate Iceberg DDL statements to update tables with new columns. It's particularly useful for maintaining synchronized bronze and silver tables in a data lake environment.

## Features

- Merges two Avro schema files, preserving the original JSON formatting
- Identifies new fields in the updated schema
- Generates Iceberg DDL statements to add new columns to both bronze and silver tables
- Maintains column order with ALTER COLUMN statements
- Creates ready-to-run PySpark scripts

## Requirements

- Python 3.6+
- PySpark environment with Iceberg support (for running the generated DDL)

No external Python packages are required beyond the standard library.

## Installation

Clone this repository and ensure the script is executable:

```bash
git clone <repository-url>
cd <repository-directory>
chmod +x merge_schemas.py
```

## Usage

```bash
python merge_schemas.py --old_schema <old_schema_path> --new_schema <new_schema_path> --db_name <database_name> --entity_name <entity_name>
```

### Required Arguments

- `--old_schema`: Path to the old schema file
- `--new_schema`: Path to the new schema file
- `--db_name`: Database name (e.g., "dtd_pad26")
- `--entity_name`: Entity/table name (e.g., "outfunds__funding_program__c")

### Optional Arguments

- `--output_schema`: Path to output the merged schema (default: "merged_schema.json")
- `--output_ddl`: Path to output the DDL statements (default: "iceberg_ddl.py")

## Example

```bash
python merge_schemas.py --old_schema old_schema.json --new_schema new_schema.json --db_name dtd_pad26 --entity_name outfunds__funding_program__c
```

This will:
1. Merge the schemas and output to `merged_schema.json`
2. Generate Iceberg DDL statements and save them to `iceberg_ddl.py`

## Input Schema Requirements

The script expects Avro schema files with the following characteristics:

1. Both schemas must be valid JSON files
2. The old schema must contain the fields `dl_event_tms` and `dl_ingestion_tms`
3. New fields will be inserted before these timestamp fields

## Output Files

### Merged Schema (JSON)

The merged schema preserves the original formatting and structure of the old schema while incorporating new fields.

### Iceberg DDL Script (Python)

The generated Python script:
- Includes proper SparkSession initialization
- Contains TRUNCATE TABLE statements for both bronze and silver tables
- Has ALTER TABLE statements to add new columns
- Includes ALTER COLUMN statements to position columns correctly
- Contains informative comments and progress logging

## Running the Generated DDL

The generated `iceberg_ddl.py` script can be executed in a Spark environment with Iceberg support:

```bash
spark-submit iceberg_ddl.py
```

## Limitations

- The script assumes that both bronze and silver tables follow the same naming convention: `{database}.{bronze|silver}_{entity}`
- Schema changes are limited to adding new columns; removing or modifying existing columns is not supported
- The script does not handle complex nested structures or maps in Avro schemas

## Troubleshooting

- **ValueError: Fields 'dl_event_tms' and 'dl_ingestion_tms' must be present in the old schema**: The old schema must contain these timestamp fields.
- **JSON decode errors**: Check that your schema files are valid JSON.
- **Insertion point errors**: If the script can't find where to insert new fields, verify the timestamp fields are correctly named.