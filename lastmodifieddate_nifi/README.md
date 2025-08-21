# DateTime Statistics Calculator

Comprehensive statistical analysis for timestamp fields in JSON data.

## Usage

```bash
python3 comprehensive_statistics.py <json_file> <timestamp_field>
python3 comprehensive_statistics.py <json_file> <timestamp_field> --show-outliers --output-format iso
```

### Parameters

- `json_file`: Path to the input JSON file
- `timestamp_field`: Name of the timestamp field to analyze (e.g., "LastModifiedDate", "CreatedDate", "UpdatedAt")

### Examples

```bash
python3 comprehensive_statistics.py data.json LastModifiedDate
python3 comprehensive_statistics.py salesforce_data.json CreatedDate --show-outliers
python3 comprehensive_statistics.py events.json UpdatedAt --output-format iso
```

## Statistics Calculated

Mean, Median, Mode, Range, Min/Max, Count, Sum, Quartiles, IQR, Outliers