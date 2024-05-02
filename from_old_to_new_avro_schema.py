import json

# Define the paths to the workspace
path = '<>'

# Define the paths to the old and new schema files
old_schema_path = path + 'old_schema.json'
new_schema_path = path + 'new_schema.json'

# Read the old schema from the file
with open(old_schema_path, 'r') as file:
    old_schema = json.load(file)

# Read the new schema from the file
with open(new_schema_path, 'r') as file:
    new_schema = json.load(file)

# Extract field names from both schemas
old_field_names = {field['name'].lower() for field in old_schema['fields']}
new_fields = new_schema['fields']

# Identify and append missing fields from the new schema to the old schema
fields_to_add = [field for field in new_fields if field['name'].lower() not in old_field_names]
old_schema['fields'].extend(fields_to_add)

# Define the output file path for the updated schema
updated_schema_path = path + 'updated_schema.json'

# Write the updated schema to the specified file
with open(updated_schema_path, 'w') as file:
    json.dump(old_schema, file)

# Print fields to be added in the specified format
for field in fields_to_add:
    field_name = field['name']
    # Typically, the type is listed as a list; we take the first type that is not 'null'
    field_type = next(t for t in field['type'] if t != 'null')
    print(f"{field_name} {field_type}")    