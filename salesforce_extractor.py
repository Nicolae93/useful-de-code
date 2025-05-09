"""
Salesforce Object Local Extraction Script

This script extracts data from a specified Salesforce object and saves it locally
in both CSV and Parquet formats, using OAuth refresh token for authentication.

Dependencies:
- simple-salesforce
- pandas
- pyarrow
- requests
"""

import os
import json
import logging
from datetime import datetime
import pandas as pd
from simple_salesforce import Salesforce
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from typing import Dict, List, Optional, Any, Union
import csv
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SalesforceOAuth:
    """Handles Salesforce OAuth authentication using refresh token"""
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.auth_domain = "test" if sandbox else "login"
        self.access_token = None
        self.instance_url = None
    
    def get_access_token(self) -> Dict[str, str]:
        """Get a new access token using the refresh token"""
        try:
            token_url = f"https://{self.auth_domain}.salesforce.com/services/oauth2/token"
            
            payload = {
                'grant_type': 'refresh_token',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token
            }
            
            response = requests.post(token_url, data=payload)
            response.raise_for_status()
            
            result = response.json()
            self.access_token = result['access_token']
            self.instance_url = result['instance_url']
            
            logger.info(f"Successfully obtained new access token")
            return {
                'access_token': self.access_token,
                'instance_url': self.instance_url
            }
        except Exception as e:
            logger.error(f"Failed to get access token: {str(e)}")
            raise

class SalesforceExtractor:
    """Handles extraction of Salesforce objects and saving to local directory"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the extractor with configuration from a JSON file or environment variables"""
        self.config = self._get_config(config_path)
        self.sf_object = self.config.get('sf_object')
        self.output_dir = self.config.get('output_dir', 'salesforce_data')
        self.query_fields = self.config.get('query_fields', '*')
        self.where_clause = self.config.get('where_clause', '')
        self.batch_size = int(self.config.get('batch_size', 10000))
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize Salesforce client
        self._init_salesforce()

    def _get_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Retrieve configuration from JSON file or environment variables"""
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load configuration from {config_path}: {str(e)}")
                raise
        else:
            # Use environment variables if no config file provided
            # Handle query fields from environment variables
            query_fields = os.environ.get('SF_QUERY_FIELDS', '*')
            if query_fields != '*' and ',' in query_fields:
                query_fields = [field.strip() for field in query_fields.split(',')]
                
            return {
                'sf_object': os.environ.get('SF_OBJECT'),
                'output_dir': os.environ.get('OUTPUT_DIR', 'salesforce_data'),
                'query_fields': query_fields,
                'where_clause': os.environ.get('SF_WHERE_CLAUSE', ''),
                'batch_size': os.environ.get('BATCH_SIZE', '10000'),
                'client_id': os.environ.get('SF_CLIENT_ID'),
                'client_secret': os.environ.get('SF_CLIENT_SECRET'),
                'refresh_token': os.environ.get('SF_REFRESH_TOKEN'),
                'sandbox': os.environ.get('SF_SANDBOX', 'False').lower() in ('true', '1', 't')
            }

    def _init_salesforce(self) -> None:
        """Initialize Salesforce connection using OAuth refresh token"""
        try:
            # Get OAuth credentials
            oauth = SalesforceOAuth(
                client_id=self.config['client_id'],
                client_secret=self.config['client_secret'],
                refresh_token=self.config['refresh_token'],
                sandbox=self.config.get('sandbox', False)
            )
            
            # Get access token
            auth_result = oauth.get_access_token()
            
            # Initialize Salesforce client with session ID (access token)
            self.sf = Salesforce(
                instance_url=auth_result['instance_url'],
                session_id=auth_result['access_token']
            )
            
            logger.info(f"Successfully connected to Salesforce using OAuth")
        except Exception as e:
            logger.error(f"Failed to initialize Salesforce connection: {str(e)}")
            raise

    def extract_data(self, incremental: bool = True) -> List[Dict[str, Any]]:
        """Extract data from Salesforce object"""
        try:
            # Build the SOQL query
            fields = self.query_fields if isinstance(self.query_fields, str) else ', '.join(self.query_fields)
            query = f"SELECT {fields} FROM {self.sf_object}"
            
            # Add where clause for incremental load if specified
            if incremental and self.where_clause:
                query += f" WHERE {self.where_clause}"
            
            logger.info(f"Executing SOQL query: {query}")
            
            # For large datasets, use the bulk API
            if self.config.get('use_bulk', False):
                logger.info("Using Bulk API for data extraction")
                bulk_job = self.sf.bulk.__getattr__(self.sf_object).query(query)
                records = []
                for batch in bulk_job:
                    records.extend(batch)
            else:
                # Execute query with built-in pagination for larger result sets
                result = self.sf.query_all(query)
                records = result['records']
            
            # Remove Salesforce metadata attributes from records
            for record in records:
                if 'attributes' in record:
                    del record['attributes']
            
            logger.info(f"Retrieved {len(records)} records from Salesforce")
            return records
        except Exception as e:
            logger.error(f"Failed to extract data from Salesforce: {str(e)}")
            raise

    def save_locally(self, records: List[Dict[str, Any]]) -> Dict[str, str]:
        """Save the extracted data locally in both CSV and Parquet format"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Generate output filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_basename = f"{self.sf_object}_{timestamp}"
            
            # Create object-specific directory
            object_dir = os.path.join(self.output_dir, self.sf_object)
            os.makedirs(object_dir, exist_ok=True)
            
            # Paths for files
            csv_path = os.path.join(object_dir, f"{output_basename}.csv")
            parquet_path = os.path.join(object_dir, f"{output_basename}.parquet")
            metadata_path = os.path.join(object_dir, f"{output_basename}_metadata.json")
            
            # Save as CSV
            df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
            
            # Save as Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_path)
            
            # Write metadata
            metadata = {
                "object": self.sf_object,
                "record_count": len(records),
                "extraction_timestamp": datetime.now().isoformat(),
                "csv_path": csv_path,
                "parquet_path": parquet_path
            }
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Successfully saved data to {csv_path} and {parquet_path}")
            return {
                "csv_path": csv_path,
                "parquet_path": parquet_path,
                "metadata_path": metadata_path
            }
        except Exception as e:
            logger.error(f"Failed to save data locally: {str(e)}")
            raise

    def save_locally_in_batches(self, records: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Save the extracted data locally in batches for large datasets"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create object-specific directory
            object_dir = os.path.join(self.output_dir, self.sf_object)
            os.makedirs(object_dir, exist_ok=True)
            
            csv_paths = []
            parquet_paths = []
            
            # Process in batches
            total_records = len(records)
            for i in range(0, total_records, self.batch_size):
                batch_num = i // self.batch_size + 1
                batch = records[i:i + self.batch_size]
                
                # Convert batch to DataFrame
                df = pd.DataFrame(batch)
                
                # Generate filenames for this batch
                csv_path = os.path.join(object_dir, f"{self.sf_object}_{timestamp}_batch{batch_num}.csv")
                parquet_path = os.path.join(object_dir, f"{self.sf_object}_{timestamp}_batch{batch_num}.parquet")
                
                # Save as CSV
                df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
                csv_paths.append(csv_path)
                
                # Save as Parquet
                table = pa.Table.from_pandas(df)
                pq.write_table(table, parquet_path)
                parquet_paths.append(parquet_path)
                
                logger.info(f"Saved batch {batch_num} ({len(batch)} records) locally")
            
            # Write metadata
            metadata_path = os.path.join(object_dir, f"{self.sf_object}_{timestamp}_metadata.json")
            metadata = {
                "object": self.sf_object,
                "record_count": total_records,
                "extraction_timestamp": datetime.now().isoformat(),
                "batch_count": len(csv_paths),
                "csv_paths": csv_paths,
                "parquet_paths": parquet_paths
            }
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Successfully saved all data in {len(csv_paths)} batches")
            return {
                "csv_paths": csv_paths,
                "parquet_paths": parquet_paths,
                "metadata_path": metadata_path
            }
        except Exception as e:
            logger.error(f"Failed to save data locally in batches: {str(e)}")
            raise

    def run(self, incremental: bool = True) -> Dict[str, Any]:
        """Run the complete extraction process"""
        try:
            logger.info(f"Starting extraction of Salesforce object {self.sf_object}")
            
            # Extract data from Salesforce
            records = self.extract_data(incremental)
            
            # Save data locally
            if len(records) > self.batch_size:
                result = self.save_locally_in_batches(records)
                output_files = result
            else:
                result = self.save_locally(records)
                output_files = result
            
            logger.info(f"Completed extraction of Salesforce object {self.sf_object}")
            return {
                "status": "success",
                "object": self.sf_object,
                "record_count": len(records),
                "output_files": output_files,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Extraction process failed: {str(e)}")
            return {
                "status": "failed",
                "object": self.sf_object,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

def create_config_file(config_path: str) -> None:
    """Create a sample configuration file"""
    sample_config = {
        "sf_object": "Account",
        "output_dir": "salesforce_data",
        "query_fields": ["Id", "Name", "Industry", "Type", "CreatedDate", "LastModifiedDate"],
        "where_clause": "LastModifiedDate > 2023-01-01T00:00:00Z",
        "batch_size": 100000,
        "use_bulk": True,
        "client_id": "your_connected_app_client_id",
        "client_secret": "your_connected_app_client_secret",
        "refresh_token": "your_oauth_refresh_token",
        "sandbox": False
    }
    
    with open(config_path, 'w') as f:
        json.dump(sample_config, f, indent=2)
    
    print(f"Created sample configuration file at {config_path}")
    print("\nTo set up OAuth authentication:")
    print("1. Create a Connected App in Salesforce")
    print("2. Enable OAuth settings with proper callback URL")
    print("3. Grant 'Access and manage your data' scope")
    print("4. Use the Web Server OAuth flow to get your initial refresh token")
    print("5. Update this config file with your client ID, client secret, and refresh token")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract Salesforce object data locally using OAuth')
    parser.add_argument('--config', help='Path to configuration JSON file')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')
    parser.add_argument('--create-config', action='store_true', help='Create a sample configuration file')
    parser.add_argument('--config-path', default='salesforce_config.json', help='Path for the sample configuration file')
    parser.add_argument('--object', help='Salesforce object to extract (overrides config file)')
    parser.add_argument('--client-id', help='Connected App Client ID (overrides config file)')
    parser.add_argument('--client-secret', help='Connected App Client Secret (overrides config file)')
    parser.add_argument('--refresh-token', help='OAuth Refresh Token (overrides config file)')
    parser.add_argument('--sandbox', action='store_true', help='Use Salesforce Sandbox instance (overrides config file)')
    parser.add_argument('--output-dir', help='Local directory to save extracted data (overrides config file)')
    parser.add_argument('--fields', help='Comma-separated list of fields to extract (overrides config file)')
    
    args = parser.parse_args()
    
    if args.create_config:
        create_config_file(args.config_path)
    else:
        # Load config
        config_data = {}
        if args.config:
            with open(args.config, 'r') as f:
                config_data = json.load(f)
        
        # Override with command line arguments if provided
        if args.object:
            config_data['sf_object'] = args.object
        if args.client_id:
            config_data['client_id'] = args.client_id
        if args.client_secret:
            config_data['client_secret'] = args.client_secret
        if args.refresh_token:
            config_data['refresh_token'] = args.refresh_token
        if args.sandbox:
            config_data['sandbox'] = True
        if args.output_dir:
            config_data['output_dir'] = args.output_dir
        if args.fields:
            config_data['query_fields'] = [field.strip() for field in args.fields.split(',')]
        
        # Write temporarily updated config if command line args were provided
        temp_config = None
        if any([args.object, args.client_id, args.client_secret, args.refresh_token, args.sandbox, args.output_dir, args.fields]):
            import tempfile
            temp_config = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
            json.dump(config_data, temp_config)
            temp_config.close()
            config_path = temp_config.name
        else:
            config_path = args.config
        
        try:
            # Run extractor
            extractor = SalesforceExtractor(config_path=config_path)
            result = extractor.run(incremental=not args.full_load)
            
            print(json.dumps(result, indent=2))
            
            if 'output_files' in result and result['status'] == 'success':
                print("\nExtraction completed successfully!")
                if 'csv_path' in result['output_files']:
                    print(f"CSV file: {result['output_files']['csv_path']}")
                if 'parquet_path' in result['output_files']:
                    print(f"Parquet file: {result['output_files']['parquet_path']}")
                if 'csv_paths' in result['output_files']:
                    print(f"CSV files: {len(result['output_files']['csv_paths'])} batches")
                    for i, path in enumerate(result['output_files']['csv_paths'], 1):
                        print(f"  Batch {i}: {path}")
        finally:
            # Clean up temporary config file if created
            if temp_config:
                os.unlink(temp_config.name)