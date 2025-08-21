import json
import argparse
import datetime
from copy import deepcopy

def merge_avro_schemas(old_schema_path, new_schema_path, output_path=None):
    """
    Merge two Avro schema files while preserving original formatting.
    
    Args:
        old_schema_path: Path to the old schema file
        new_schema_path: Path to the new schema file
        output_path: Path to output the merged schema
        
    Returns:
        tuple: (merged schema dict, list of new fields, name of the last field before timestamp fields)
    """
    # Read schema files
    with open(old_schema_path, 'r') as f:
        old_schema = json.load(f)
        f.seek(0)
        old_schema_text = f.read()
    
    with open(new_schema_path, 'r') as f:
        new_schema = json.load(f)
    
    # Extract field names for comparison
    old_field_names = [field['name'] for field in old_schema.get('fields', [])]
    
    # Validate required fields
    if 'dl_event_tms' not in old_field_names or 'dl_ingestion_tms' not in old_field_names:
        raise ValueError("Fields 'dl_event_tms' and 'dl_ingestion_tms' must be present in the old schema.")
    
    # Find new fields
    old_field_names_set = set(old_field_names)
    new_fields = [field for field in new_schema.get('fields', []) 
                 if field['name'] not in old_field_names_set]
    
    # Find insertion point
    event_tms_index = old_field_names.index('dl_event_tms')
    ingestion_tms_index = old_field_names.index('dl_ingestion_tms')
    insert_index = min(event_tms_index, ingestion_tms_index)
    last_field_before_timestamps = old_field_names[insert_index - 1] if insert_index > 0 else None
    
    # If there are no new fields, return the original schema
    if not new_fields:
        if output_path:
            with open(output_path, 'w') as f:
                f.write(old_schema_text)
        return old_schema, [], last_field_before_timestamps
    
    # Create a merged schema object for data processing
    merged_schema = deepcopy(old_schema)
    merged_schema['fields'] = (
        merged_schema['fields'][:insert_index] + 
        new_fields + 
        merged_schema['fields'][insert_index:]
    )
    
    # Preserve formatting by manipulating the text representation
    # Find the field to insert before
    search_field = f'"name": "dl_{min(event_tms_index, ingestion_tms_index) == event_tms_index and "event" or "ingestion"}_tms"'
    
    # Find position in text
    lines = old_schema_text.split('\n')
    insert_line = None
    field_indent = '        '  # Default indentation
    
    # Find the line with the timestamp field and the indentation
    for i, line in enumerate(lines):
        if search_field in line:
            # Look for the start of this field (the opening brace)
            for j in range(i, 0, -1):
                if '{' in lines[j] and '"name":' not in lines[j]:
                    insert_line = j
                    # Extract indentation
                    field_indent_match = lines[j].split('{')[0]
                    if field_indent_match:
                        field_indent = field_indent_match
                    break
            break
    
    if insert_line is None:
        raise ValueError(f"Could not find the insertion point in the schema text.")
    
    # Format new fields to match old schema style
    formatted_fields = []
    for field in new_fields:
        # Create field text with proper formatting
        field_text = f'{field_indent}{{\n'
        field_text += f'{field_indent}    "name": "{field["name"]}",\n'
        field_text += f'{field_indent}    "type":\n'
        field_text += f'{field_indent}    [\n'
        
        for i, type_val in enumerate(field["type"]):
            if isinstance(type_val, str):
                field_text += f'{field_indent}        "{type_val}"'
            else:
                field_text += f'{field_indent}        {json.dumps(type_val)}'
                
            if i < len(field["type"]) - 1:
                field_text += ',\n'
            else:
                field_text += '\n'
                
        field_text += f'{field_indent}    ]\n'
        field_text += f'{field_indent}}}'
        
        formatted_fields.append(field_text)
    
    # Insert formatted fields into the schema
    pre_insert = lines[:insert_line]
    post_insert = lines[insert_line:]
    
    # Add comma to each field except the last one
    formatted_fields_with_commas = []
    for i, field_text in enumerate(formatted_fields):
        if i < len(formatted_fields) - 1:
            formatted_fields_with_commas.append(field_text + ',')
        else:
            # If this is the last new field, add comma if the next line is a field
            if '"name":' in ''.join(post_insert[:3]):
                formatted_fields_with_commas.append(field_text + ',')
            else:
                formatted_fields_with_commas.append(field_text)
    
    # Join the parts together
    merged_lines = pre_insert + formatted_fields_with_commas + post_insert
    merged_text = '\n'.join(merged_lines)
    
    # Output merged schema
    if output_path:
        with open(output_path, 'w') as f:
            f.write(merged_text)
    
    return merged_schema, new_fields, last_field_before_timestamps

def map_avro_to_iceberg_type(avro_type):
    """Map Avro data types to Iceberg SQL data types."""
    # Handle union types (e.g., ["string", "null"])
    if isinstance(avro_type, list):
        non_null_types = [t for t in avro_type if t != "null"]
        if len(non_null_types) == 1:
            return map_avro_to_iceberg_type(non_null_types[0])
        return "STRING"  # Default for complex unions
    
    # Type mapping
    type_map = {
        "string": "STRING",
        "boolean": "BOOLEAN",
        "double": "DOUBLE",
        "long": "BIGINT",
        "int": "INT"
    }
    
    if avro_type in type_map:
        return type_map[avro_type]
    
    # Handle logical types
    if isinstance(avro_type, dict):
        if avro_type.get("type") == "long" and avro_type.get("logicalType") == "timestamp-millis":
            return "TIMESTAMP"
    
    # Default fallback
    return "STRING"

def generate_iceberg_ddl(db_name, entity_name, new_fields, last_field_before_timestamps):
    """Generate Iceberg DDL statements for bronze and silver tables."""
    bronze_table = f"{db_name}.bronze_{entity_name}"
    silver_table = f"{db_name}.silver_{entity_name}"
    
    # Format column definitions
    add_columns_str = ",\n    ".join([
        f"{field['name']} {map_avro_to_iceberg_type(field['type'])}" 
        for field in new_fields
    ])
    
    # Create script header
    new_columns_list = '\n'.join([
        f'- {field["name"]} ({map_avro_to_iceberg_type(field["type"])})'
        for field in new_fields
    ])
    
    header = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Iceberg Schema Update Script for {entity_name}
==============================================

This script updates the schema for both bronze and silver tables 
by adding new columns identified during schema comparison.

Database: {db_name}
Entity: {entity_name}
Tables:
- {bronze_table}
- {silver_table}

New columns:
{new_columns_list}

Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
'''

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \\
    .appName("Iceberg Schema Update - {entity_name}") \\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \\
    .config("spark.sql.catalog.spark_catalog.type", "hive") \\
    .getOrCreate()

print("Starting Iceberg schema update for {entity_name}...")

# First, truncate tables to clear existing data
print("Truncating tables...")
"""
    
    # Create DDL statements
    truncate_statements = f"""spark.sql("TRUNCATE TABLE {bronze_table}")
spark.sql("TRUNCATE TABLE {silver_table}")
"""
    
    # Bronze table DDLs
    bronze_ddl = f"""
# Bronze table DDLs
print("Adding new columns to bronze table...")
spark.sql(\"\"\"
ALTER TABLE {bronze_table}
ADD COLUMNS (
    {add_columns_str}
)
\"\"\")

# Position each new column in bronze table
print("Positioning new columns in bronze table...")
"""
    
    # Bronze column positioning
    bronze_positioning = ""
    prev_field = last_field_before_timestamps
    for field in new_fields:
        bronze_positioning += f"""spark.sql(\"\"\"
ALTER TABLE {bronze_table} ALTER COLUMN {field['name']} AFTER {prev_field}
\"\"\")
"""
        prev_field = field['name']
    
    # Silver table DDLs
    silver_ddl = f"""
# Silver table DDLs
print("Adding new columns to silver table...")
spark.sql(\"\"\"
ALTER TABLE {silver_table}
ADD COLUMNS (
    {add_columns_str}
)
\"\"\")

# Position each new column in silver table
print("Positioning new columns in silver table...")
"""
    
    # Silver column positioning
    silver_positioning = ""
    prev_field = last_field_before_timestamps
    for field in new_fields:
        silver_positioning += f"""spark.sql(\"\"\"
ALTER TABLE {silver_table} ALTER COLUMN {field['name']} AFTER {prev_field}
\"\"\")
"""
        prev_field = field['name']
    
    # Completion message
    footer = """
print("Schema update completed successfully!")
"""
    
    # Combine all sections
    return header + truncate_statements + bronze_ddl + bronze_positioning + silver_ddl + silver_positioning + footer

def main():
    """Main function to execute the script."""
    parser = argparse.ArgumentParser(description='Merge Avro schemas and generate Iceberg DDL statements.')
    parser.add_argument('--old_schema', required=True, help='Path to the old schema file')
    parser.add_argument('--new_schema', required=True, help='Path to the new schema file')
    parser.add_argument('--db_name', required=True, help='Database name')
    parser.add_argument('--entity_name', required=True, help='Entity/table name')
    parser.add_argument('--output_schema', help='Path to output the merged schema')
    parser.add_argument('--output_ddl', help='Path to output the DDL statements')
    args = parser.parse_args()
    
    # Set default output file names with entity_name prefix and proper directory structure
    if not args.output_schema:
        args.output_schema = f'output/merged_schemas/{args.entity_name}_merged_schema.json'
    if not args.output_ddl:
        args.output_ddl = f'output/ddl/{args.entity_name}_iceberg_ddl.py'
    
    try:
        # Ensure output directories exist
        import os
        os.makedirs('output/merged_schemas', exist_ok=True)
        os.makedirs('output/ddl', exist_ok=True)
        
        # Merge the schema files
        merged_schema, new_fields, last_field = merge_avro_schemas(
            args.old_schema, 
            args.new_schema, 
            args.output_schema
        )
        print(f"Schema merge completed successfully! Output written to {args.output_schema}")
        
        # Generate Iceberg DDL statements
        if new_fields:
            print(f"\nGenerating Iceberg DDL statements for {len(new_fields)} new columns...")
            iceberg_ddl = generate_iceberg_ddl(
                args.db_name, 
                args.entity_name, 
                new_fields, 
                last_field
            )
            
            # Output DDL to file
            with open(args.output_ddl, 'w') as f:
                f.write(iceberg_ddl)
            print(f"Iceberg DDL statements written to '{args.output_ddl}'")
            
            # Print new fields
            print("\nNew fields found:")
            for field in new_fields:
                print(f"  - {field['name']} ({map_avro_to_iceberg_type(field['type'])})")
        else:
            print("No new fields found, no DDL statements generated.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()