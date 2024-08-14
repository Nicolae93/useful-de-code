import json

def avro_type_to_hive_type(avro_type):
    if isinstance(avro_type, list):
        avro_type = next(t for t in avro_type if t != "null")

    type_mapping = {
        "string": "STRING",
        "int": "INT",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "bytes": "BINARY",
        "fixed": "BINARY"
    }

    if isinstance(avro_type, str) and avro_type in type_mapping:
        return type_mapping[avro_type]
    elif isinstance(avro_type, dict):
        if avro_type['type'] == "record":
            fields = avro_type['fields']
            nested_fields = ', '.join([f"{field['name']}:{avro_type_to_hive_type(field['type'])}" for field in fields])
            return f"STRUCT<{nested_fields}>"
        elif avro_type['type'] == "array":
            return f"ARRAY<{avro_type_to_hive_type(avro_type['items'])}>"
        elif avro_type['type'] == "map":
            return f"MAP<STRING, {avro_type_to_hive_type(avro_type['values'])}>"
        elif avro_type['type'] == "enum":
            return "STRING"
    else:
        raise ValueError(f"Unsupported Avro type: {avro_type}")
    
def generate_hive_table_from_avro(avro_schema_str):
    avro_schema = json.loads(avro_schema_str)
    fields = avro_schema["fields"]

    hive_fields = [f"{field['name']} {avro_type_to_hive_type(field['type'])}" for field in fields]
    hive_fields_str = ',\n    '.join(hive_fields)

    return f"""CREATE EXTERNAL TABLE IF NOT EXISTS {avro_schema['name']} (
    {hive_fields_str}
) 
STORED BY ICEBERG
TBLPROPERTIES ("format-version" = '2')"""

if __name__ == "__main__":
    # Replace with your actual Avro schema as a string
    avro_schema_str = ''
    print(generate_hive_table_from_avro(avro_schema_str))
