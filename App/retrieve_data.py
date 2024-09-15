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

client = cdsapi.Client()

# Define the years for each sensor and variable
years = {
    'airs_nlis': ['2003', '2004', '2005', '2006', '2007'],
    'iasi_metop_a_nlis': ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021'],
    'iasi_metop_b_nlis': ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'],
    'iasi_metop_c_nlis': ['2019', '2020', '2021', '2022'],
    'tanso_fts_srfp': ['2019', '2020', '2021', '2022']
}

# Define the variables to retrieve
variables = {
    'mid_tropospheric_columns_of_atmospheric_carbon_dioxide': 'MidTropospheric_CO2'
}

# Define sensor and versions
sensor_and_versions = {
    'airs_nlis': '3.0',
    'iasi_metop_a_nlis': '10.1',
    'iasi_metop_b_nlis': '10.1',
    'iasi_metop_c_nlis': '10.1',
    'tanso_fts_srfp': '2.0.0'
}

# Define months and days as arrays
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
days = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']

# Process each variable, sensor, and years in batches
for var, var_name in variables.items():
    for sensor, version in sensor_and_versions.items():
        sensor_years = years.get(sensor, [])
        if not sensor_years:
            continue

        request = {
            'processing_level': ['level_2'],  # Adjust level if needed
            'variable': [var],
            'sensor_and_algorithm': [sensor],
            'year': sensor_years,  # Use all years for this sensor
            'month': months,
            'day': days,
            'version': [version],
            'data_format': 'zip'
        }

        # Temporary File method
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name
                print(f"Retrieving data for {var_name} with {sensor} for years {sensor_years}...")

                # Retrieve data and save it to the temporary file
                response = client.retrieve("satellite-carbon-dioxide", request)  # Use dataset name here
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

                    s3_key = f"{var_name}/{sensor}/{','.join(sensor_years)}.zip"
                    s3_client.upload_file(temp_file_path, BUCKET_NAME, s3_key)
                    print(f"File uploaded to S3 bucket {BUCKET_NAME} with key {s3_key}")
                else:
                    print(f"No data retrieved for {var_name} with {sensor} for years {sensor_years}. No folder created.")

        except Exception as e:
            print(f"Error processing {var_name} with {sensor} for years {sensor_years}: {e}")

        finally:
            # Clean up the temporary file
            try:
                os.remove(temp_file_path)
            except:
                pass
