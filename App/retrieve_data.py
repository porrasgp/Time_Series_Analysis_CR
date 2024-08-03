import cdsapi
import boto3
import os

# Initialize CDS API client
client = cdsapi.Client()

# Define the dataset and request parameters
dataset = "sis-agroproductivity-indicators"
request = {
    'product_family': ['crop_productivity_indicators'],
    'variable': ['total_above_ground_production'],
    'crop_type': ['maize'],
    'year': '2023',
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['10', '20', '28', '30', '31'],
    'growing_season': ['1st_season_per_campaign'],
    'harvest_year': '2023'
}

# Retrieve the dataset
output_filename = "agroproductivity_maize_2023.nc"
client.retrieve(dataset, request).download(output_filename)

# AWS S3 configuration
s3_bucket_name = 'your-s3-bucket-name'
s3_key = f'path/in/bucket/{output_filename}'  # Customize the path as needed

# Initialize the S3 client
s3_client = boto3.client('s3')

# Upload the file to S3
try:
    s3_client.upload_file(output_filename, s3_bucket_name, s3_key)
    print(f"File {output_filename} uploaded to s3://{s3_bucket_name}/{s3_key}")
except Exception as e:
    print(f"Error uploading file: {e}")

# Clean up the local file if needed
os.remove(output_filename)
import cdsapi
import boto3
import os

# Initialize CDS API client
client = cdsapi.Client()

# Define the dataset and request parameters
dataset = "sis-agroproductivity-indicators"
request = {
    'product_family': ['crop_productivity_indicators'],
    'variable': ['total_above_ground_production'],
    'crop_type': ['maize'],
    'year': '2023',
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['10', '20', '28', '30', '31'],
    'growing_season': ['1st_season_per_campaign'],
    'harvest_year': '2023'
}

# Retrieve the dataset
output_filename = "agroproductivity_maize_2023.nc"
client.retrieve(dataset, request).download(output_filename)

# AWS S3 configuration
s3_bucket_name = 'your-s3-bucket-name'
s3_key = f'path/in/bucket/{output_filename}'  # Customize the path as needed

# Initialize the S3 client
s3_client = boto3.client('s3')

# Upload the file to S3
try:
    s3_client.upload_file(output_filename, s3_bucket_name, s3_key)
    print(f"File {output_filename} uploaded to s3://{s3_bucket_name}/{s3_key}")
except Exception as e:
    print(f"Error uploading file: {e}")

# Clean up the local file if needed
os.remove(output_filename)
