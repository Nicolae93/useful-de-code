#!/usr/bin/env python3
"""
Comprehensive statistical analysis script for LastModifiedDate values from JSON files.
Calculates: Mean, Median, Mode, Range, Min/Max, Count, Sum, Quartiles, IQR, and Outliers.
Compatible with PySpark environments and data lake workflows.
"""

import json
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
import argparse
from collections import Counter
from statistics import mode as stats_mode, StatisticsError


def parse_iso_date(date_string: str) -> datetime:
    """
    Parse ISO 8601 date string to datetime object.
    
    Args:
        date_string: ISO 8601 formatted date string
        
    Returns:
        datetime: Parsed datetime object
    """
    # Handle the timezone format (+0000) by replacing with Z
    if date_string.endswith('+0000'):
        date_string = date_string[:-5] + 'Z'
    
    # Parse the datetime
    return datetime.fromisoformat(date_string.replace('Z', '+00:00'))


def load_json_data(file_path: str) -> List[Dict[str, Any]]:
    """
    Load JSON data from file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        List[Dict]: Loaded JSON data
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in file '{file_path}': {e}")
        sys.exit(1)


def extract_dates(data: List[Dict[str, Any]]) -> List[datetime]:
    """
    Extract and parse LastModifiedDate values from JSON data.
    
    Args:
        data: List of dictionaries containing LastModifiedDate
        
    Returns:
        List[datetime]: List of parsed datetime objects
    """
    dates = []
    for i, record in enumerate(data):
        if 'LastModifiedDate' not in record:
            print(f"Warning: Record {i} missing 'LastModifiedDate' field")
            continue
        
        try:
            date_obj = parse_iso_date(record['LastModifiedDate'])
            dates.append(date_obj)
        except Exception as e:
            print(f"Warning: Could not parse date in record {i}: {e}")
            continue
    
    return dates


class DateTimeStatistics:
    """
    Comprehensive statistics calculator for datetime data.
    """
    
    def __init__(self, dates: List[datetime]):
        """
        Initialize with list of datetime objects.
        
        Args:
            dates: List of datetime objects to analyze
        """
        if not dates:
            raise ValueError("No dates provided for statistical analysis")
        
        self.dates = sorted(dates)
        self.count = len(dates)
        self.timestamps = [dt.timestamp() for dt in self.dates]
        
    def calculate_mean(self) -> datetime:
        """Calculate mean datetime."""
        mean_timestamp = sum(self.timestamps) / self.count
        return datetime.fromtimestamp(mean_timestamp, tz=self.dates[0].tzinfo)
    
    def calculate_median(self) -> datetime:
        """Calculate median datetime."""
        n = self.count
        if n % 2 == 1:
            return self.dates[n // 2]
        else:
            mid1 = self.dates[n // 2 - 1]
            mid2 = self.dates[n // 2]
            timestamp1 = mid1.timestamp()
            timestamp2 = mid2.timestamp()
            median_timestamp = (timestamp1 + timestamp2) / 2
            return datetime.fromtimestamp(median_timestamp, tz=mid1.tzinfo)
    
    def calculate_mode(self) -> List[datetime]:
        """Calculate mode(s) - most frequent datetime(s)."""
        try:
            # Count occurrences by converting to strings to handle timezone comparison
            date_strs = [dt.isoformat() for dt in self.dates]
            counter = Counter(date_strs)
            max_count = max(counter.values())
            
            if max_count == 1:
                return []  # No mode if all values occur once
            
            # Get all dates that occur with maximum frequency
            mode_strs = [date_str for date_str, count in counter.items() if count == max_count]
            return [parse_iso_date(date_str) for date_str in mode_strs]
            
        except (StatisticsError, ValueError):
            return []
    
    def calculate_range(self) -> timedelta:
        """Calculate range (max - min)."""
        return self.dates[-1] - self.dates[0]
    
    def calculate_quartiles(self) -> Tuple[datetime, datetime, datetime]:
        """
        Calculate quartiles (Q1, Q2, Q3).
        
        Returns:
            Tuple of (Q1, Q2, Q3) datetime objects
        """
        def percentile(data: List[float], p: float) -> float:
            """Calculate percentile of sorted data."""
            n = len(data)
            index = p * (n - 1)
            lower = int(index)
            upper = min(lower + 1, n - 1)
            weight = index - lower
            return data[lower] * (1 - weight) + data[upper] * weight
        
        q1_timestamp = percentile(self.timestamps, 0.25)
        q2_timestamp = percentile(self.timestamps, 0.5)  # Median
        q3_timestamp = percentile(self.timestamps, 0.75)
        
        tz = self.dates[0].tzinfo
        return (
            datetime.fromtimestamp(q1_timestamp, tz=tz),
            datetime.fromtimestamp(q2_timestamp, tz=tz),
            datetime.fromtimestamp(q3_timestamp, tz=tz)
        )
    
    def calculate_iqr(self) -> timedelta:
        """Calculate Interquartile Range (Q3 - Q1)."""
        q1, _, q3 = self.calculate_quartiles()
        return q3 - q1
    
    def calculate_outliers(self) -> Tuple[List[datetime], List[datetime]]:
        """
        Calculate outliers using IQR method.
        
        Returns:
            Tuple of (lower_outliers, upper_outliers)
        """
        q1, _, q3 = self.calculate_quartiles()
        iqr = q3 - q1
        iqr_seconds = iqr.total_seconds()
        
        # Outlier boundaries
        lower_bound = q1 - timedelta(seconds=1.5 * iqr_seconds)
        upper_bound = q3 + timedelta(seconds=1.5 * iqr_seconds)
        
        lower_outliers = [dt for dt in self.dates if dt < lower_bound]
        upper_outliers = [dt for dt in self.dates if dt > upper_bound]
        
        return lower_outliers, upper_outliers
    
    def calculate_sum(self) -> float:
        """Calculate sum of all timestamps."""
        return sum(self.timestamps)


def format_duration(td: timedelta) -> str:
    """Format timedelta in human-readable format."""
    total_seconds = int(td.total_seconds())
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days} days")
    if hours > 0:
        parts.append(f"{hours} hours")
    if minutes > 0:
        parts.append(f"{minutes} minutes")
    if seconds > 0 or not parts:
        parts.append(f"{seconds} seconds")
    
    return ", ".join(parts)


def main():
    """Main function to process input file and compute comprehensive statistics."""
    parser = argparse.ArgumentParser(
        description='Calculate comprehensive statistics for LastModifiedDate from JSON file'
    )
    parser.add_argument('input_file', help='Path to the input JSON file')
    parser.add_argument('--output-format', choices=['iso', 'timestamp', 'readable'], 
                       default='readable', help='Output format for dates')
    parser.add_argument('--show-outliers', action='store_true', 
                       help='Show detailed outlier information')
    
    args = parser.parse_args()
    
    # Load and process data
    print(f"Loading data from: {args.input_file}")
    data = load_json_data(args.input_file)
    
    print(f"Found {len(data)} records")
    dates = extract_dates(data)
    
    if not dates:
        print("Error: No valid dates found in the input file")
        sys.exit(1)
    
    print(f"Successfully parsed {len(dates)} dates\n")
    
    try:
        stats = DateTimeStatistics(dates)
        
        # Format function based on output format
        def format_date(dt: datetime) -> str:
            if args.output_format == 'iso':
                return dt.isoformat()
            elif args.output_format == 'timestamp':
                return str(int(dt.timestamp()))
            else:  # readable
                return dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        # Calculate and display all statistics
        print("=" * 60)
        print("COMPREHENSIVE DATETIME STATISTICS")
        print("=" * 60)
        
        # Basic statistics
        print(f"Count (n):           {stats.count:,}")
        print(f"Sum (timestamps):    {stats.calculate_sum():,.0f}")
        print(f"Minimum:             {format_date(stats.dates[0])}")
        print(f"Maximum:             {format_date(stats.dates[-1])}")
        print(f"Range:               {format_duration(stats.calculate_range())}")
        
        print()
        
        # Central tendency
        print(f"Mean (x̄):            {format_date(stats.calculate_mean())}")
        print(f"Median (x̃):          {format_date(stats.calculate_median())}")
        
        modes = stats.calculate_mode()
        if modes:
            print(f"Mode:                {len(modes)} value(s)")
            for i, mode_date in enumerate(modes[:5]):  # Show first 5 modes
                print(f"  Mode {i+1}:          {format_date(mode_date)}")
            if len(modes) > 5:
                print(f"  ... and {len(modes) - 5} more")
        else:
            print("Mode:                No mode (all values unique)")
        
        print()
        
        # Quartiles
        q1, q2, q3 = stats.calculate_quartiles()
        print("QUARTILES:")
        print(f"  Q1 (25th percentile): {format_date(q1)}")
        print(f"  Q2 (50th percentile): {format_date(q2)}")
        print(f"  Q3 (75th percentile): {format_date(q3)}")
        
        iqr = stats.calculate_iqr()
        print(f"Interquartile Range:  {format_duration(iqr)}")
        
        print()
        
        # Outliers
        lower_outliers, upper_outliers = stats.calculate_outliers()
        total_outliers = len(lower_outliers) + len(upper_outliers)
        print(f"Outliers:             {total_outliers} ({total_outliers/stats.count*100:.1f}%)")
        print(f"  Lower outliers:     {len(lower_outliers)}")
        print(f"  Upper outliers:     {len(upper_outliers)}")
        
        if args.show_outliers and total_outliers > 0:
            print("\nOUTLIER DETAILS:")
            if lower_outliers:
                print("Lower outliers (earliest):")
                for outlier in lower_outliers[:10]:  # Show first 10
                    print(f"  {format_date(outlier)}")
                if len(lower_outliers) > 10:
                    print(f"  ... and {len(lower_outliers) - 10} more")
            
            if upper_outliers:
                print("Upper outliers (latest):")
                for outlier in upper_outliers[:10]:  # Show first 10
                    print(f"  {format_date(outlier)}")
                if len(upper_outliers) > 10:
                    print(f"  ... and {len(upper_outliers) - 10} more")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"Error calculating statistics: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()