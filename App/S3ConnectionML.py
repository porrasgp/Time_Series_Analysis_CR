import os
import boto3
import pandas as pd
import netCDF4 as nc
from datetime import datetime

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = 'maize-climate-data-store'

def process_file(file_path, year, variable):
    """Process NetCDF file and convert to pandas DataFrame."""
    # Open the NetCDF file
    dataset = nc.Dataset(file_path)
    
    # Extract data
    data = dataset.variables[variable][:]
    time = dataset.variables['time'][:]
    # Convert to DataFrame (Example for a single variable)
    df = pd.DataFrame(data, columns=[variable])
    
    # Adding a column for time if needed
    df['time'] = pd.to_datetime(time, unit='D', origin='1900-01-01')
    
    return df

def upload_to_s3(df, year, variable):
    """Upload DataFrame to S3."""
    # Convert DataFrame to CSV
    csv_data = df.to_csv(index=False)
    
    # Define S3 key
    s3_key = f"{year}/{variable}.csv"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_data,
        ContentType='text/csv'
    )
    print(f"Uploaded {s3_key} to S3")

def main():
    base_directory = '/path/to/your/files'  # Update with the path to your files
    for file_name in os.listdir(base_directory):
        if file_name.endswith('.nc'):
            file_path = os.path.join(base_directory, file_name)
            
            # Extract year and variable from the file name (Example pattern)
            parts = file_name.split('_')
            year = parts[2]
            variable = parts[1]  # Update if necessary
            
            # Process file and upload to S3
            df = process_file(file_path, year, variable)
            upload_to_s3(df, year, variable)

if __name__ == "__main__":
    main()
