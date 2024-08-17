import argparse
import boto3
import os
import tempfile
from dotenv import load_dotenv
import cdsapi

# Load environment variables (only needed if running locally with a .env file)
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "geltonas.tech"

# Ensure AWS credentials and region are correctly set
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
    raise ValueError("AWS credentials or region are not set properly.")

# Set up argument parser
parser = argparse.ArgumentParser(description='Process satellite CO2 data.')
parser.add_argument('--sensor_and_algorithm', nargs='+', required=True, 
                    help='List of sensor and algorithm combinations.')
parser.add_argument('--processing_level', nargs='+', required=True, 
                    help='List of processing levels.')
parser.add_argument('--variables', nargs='+', required=True, 
                    help='List of variables to retrieve.')
parser.add_argument('--years', nargs='+', required=True, 
                    help='List of years to process.')

args = parser.parse_args()

client = cdsapi.Client()

# Convert arguments to proper formats
sensor_and_algorithm_list = args.sensor_and_algorithm
processing_level_list = args.processing_level
variables = {var: var for var in args.variables}
years = args.years

# Process each variable and year in batches
for var, var_name in variables.items():
    for year in years:
        request = {
            'processing_level': processing_level_list,
            'variable': [var],
            'sensor_and_algorithm': sensor_and_algorithm_list,
            'year': [year],
            'month': [],  # Remove month and day
            'day': [],
            'version': ['latest'],
            'data_format': 'zip'
        }

        # Temporary File method
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name
                print(f"Retrieving data for {var_name} in {year}...")

                # Retrieve data and save it to the temporary file
                response = client.retrieve("satellite-carbon-dioxide", request)
                response.download(temp_file_path)
                
                # Check if file is empty
                if os.path.getsize(temp_file_path) > 0:
                    # Upload the temporary file to S3
                    s3_client = boto3.client(
                        's3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_REGION
                    )

                    s3_key = f"{year}/{var_name}.zip"
                    s3_client.upload_file(temp_file_path, BUCKET_NAME, s3_key)
                    print(f"File uploaded to S3 bucket {BUCKET_NAME} with key {s3_key}")
                else:
                    print(f"No data retrieved for {var_name} in {year}. No folder created.")

        except Exception as e:
            print(f"Error processing {var_name} for {year}: {e}")

        finally:
            # Clean up the temporary file
            try:
                os.remove(temp_file_path)
            except:
                pass
