import boto3
import os
import tempfile
import time
from dotenv import load_dotenv
import cdsapi

#Load environment variables (only needed if running locally with a .env file)
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "maize-climate-data-store"

# Ensure AWS credentials and region are correctly set
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
    raise ValueError("AWS credentials or region are not set properly.")

# Define dataset, variables, and years
dataset = "sis-agroproductivity-indicators"
variables = ['crop_development_stage', 'total_above_ground_production', 'total_weight_storage_organs']
years = ['2019', '2020', '2021', '2022', '2023']

client = cdsapi.Client()

def wait_for_job_to_complete(client, request):
    """Wait until the job is completed before attempting to download."""
    while True:
        try:
            client.retrieve(dataset, request)
            break  # Exit the loop if the job is complete and ready for download
        except Exception as e:
            if "Result not ready, job is running" in str(e):
                print("Job is still running, waiting for 10 seconds before checking again...")
                time.sleep(100)  # wait for 100 seconds before checking again
            else:
                raise  # Raise any other exceptions

def upload_to_s3(temp_file_path, s3_client, bucket_name, s3_key):
    """Upload file to S3."""
    if os.path.getsize(temp_file_path) > 0:
        s3_client.upload_file(temp_file_path, bucket_name, s3_key)
        print(f"File uploaded to S3 bucket {bucket_name} with key {s3_key}")
    else:
        print("Temporary file is empty. No data was retrieved.")

# Method 2: Temporary File in Batches by Year
try:
    for var in variables:
        for year in years:
            request = {
                'product_family': ['crop_productivity_indicators'],
                'variable': [var],
                'crop_type': ['maize'],
                'year': year,
                'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
                'day': ['10', '20', '28', '30', '31'],
                'growing_season': ['1st_season_per_campaign'],
                'harvest_year': year,
                'data_format': 'zip'
            }

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name
                print(f"Attempting to retrieve data for {var} in year {year} to temporary file: {temp_file_path}")

                # Wait for the job to complete before downloading
                wait_for_job_to_complete(client, request)

                # Retrieve data and download it directly to the temporary file
                response = client.retrieve(dataset, request)
                response.download(temp_file_path)
                print(f"Data download completed for {var} in year {year}. File saved to: {temp_file_path}")

                # Upload the file to S3
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name=AWS_REGION
                )

                s3_key = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
                upload_to_s3(temp_file_path, s3_client, BUCKET_NAME, s3_key)

except Exception as e:
    print(f"Error: {e}")

finally:
    # Clean up the temporary file
    try:
        os.remove(temp_file_path)
    except:
        pass
