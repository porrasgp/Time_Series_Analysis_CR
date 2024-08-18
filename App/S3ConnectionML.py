import boto3
import xarray as xr
import zipfile
import os
import tempfile
from pathlib import Path

def process_netcdf_files(input_bucket, output_bucket, years):
    s3_client = boto3.client('s3')

    for year in years:
        year_path = f"crop_productivity_indicators/{year}/"
        processed_path = f"processed_data/{year}/"

        # List files in the S3 bucket for the given year
        response = s3_client.list_objects_v2(Bucket=input_bucket, Prefix=year_path)
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.zip')]

        # Process each file
        for file_key in files:
            print(f"Processing {file_key}...")
            with tempfile.TemporaryDirectory() as tmpdir:
                local_zip = os.path.join(tmpdir, 'data.zip')
                s3_client.download_file(input_bucket, file_key, local_zip)
                
                with zipfile.ZipFile(local_zip, 'r') as zip_ref:
                    zip_ref.extractall(tmpdir)
                
                # Process each NetCDF file
                combined_dataset = None
                for netcdf_file in Path(tmpdir).glob('*.nc'):
                    print(f"Processing NetCDF file {netcdf_file}...")
                    dataset = xr.open_dataset(netcdf_file)
                    
                    # Combine datasets if needed
                    if combined_dataset is None:
                        combined_dataset = dataset
                    else:
                        combined_dataset = xr.concat([combined_dataset, dataset], dim='time')

                if combined_dataset is not None:
                    # Save the combined dataset to S3
                    combined_nc_file = os.path.join(tmpdir, f"combined_{year}.nc")
                    combined_dataset.to_netcdf(combined_nc_file)
                    
                    s3_client.upload_file(combined_nc_file, output_bucket, f"{processed_path}combined_{year}.nc")
                    print(f"Uploaded combined NetCDF to {output_bucket}/{processed_path}combined_{year}.nc")

def main():
    input_bucket = 'maize-climate-data-store'
    output_bucket = 'maize-climate-data-processed'
    years = ['2023']  # List of years to process

    process_netcdf_files(input_bucket, output_bucket, years)

if __name__ == "__main__":
    main()
