import os
import boto3
import zipfile
import pandas as pd
import numpy as np
from netCDF4 import Dataset
from dotenv import load_dotenv
from io import StringIO

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "maize-climate-data-store"
PROCESSED_PREFIX = "processed_data"

# Crear cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def download_and_extract_from_s3(s3_key):
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
            temp_file_path = temp_file.name
        
        with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
            zip_ref.extractall('/tmp/')
        
        return temp_file_path
    except Exception as e:
        print(f"Error al procesar {s3_key}: {e}")
        return None

def list_netcdf_variables(file_path):
    try:
        with Dataset(file_path, 'r') as nc:
            return list(nc.variables.keys())
    except Exception as e:
        print(f"Error leyendo el archivo NetCDF {file_path}: {e}")
        return []

def read_netcdf_with_chunks(file_path, variable_name, chunk_size=1000):
    data = []
    
    try:
        with Dataset(file_path, 'r') as nc:
            if variable_name in nc.variables:
                var_data = nc.variables[variable_name]
                for i in range(0, var_data.shape[0], chunk_size):
                    chunk = var_data[i:i+chunk_size].flatten()
                    data.append(chunk)
            else:
                print(f"Advertencia: '{variable_name}' no encontrado en {file_path}")
    except Exception as e:
        print(f"Error leyendo la variable {variable_name} del archivo {file_path}: {e}")
    
    return np.concatenate(data) if data else np.array([])

def process_netcdf(file_path, year, variable_name):
    data = read_netcdf_with_chunks(file_path, variable_name)
    if len(data) > 0:
        df = pd.DataFrame({
            'Year': year,
            'Variable': variable_name,
            'Data': data
        })
        return df
    else:
        return pd.DataFrame()

def upload_to_s3(df, year, variable_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_key = f'{PROCESSED_PREFIX}/{year}/{variable_name}_processed.csv'
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Datos procesados subidos a {s3_key}")

def process_year_folder(year, variable):
    s3_prefix = f'crop_productivity_indicators/{year}/{variable}_year_{year}.zip'
    temp_zip_path = download_and_extract_from_s3(s3_prefix)
    
    if temp_zip_path:
        files = [f for f in os.listdir('/tmp/') if f.endswith('.nc')]
        for file_name in files:
            file_path = os.path.join('/tmp/', file_name)
            print(f"Procesando {file_name}...")
            file_variables = list_netcdf_variables(file_path)
            
            for variable_name in file_variables:
                df = process_netcdf(file_path, year, variable_name)
                if not df.empty:
                    upload_to_s3(df, year, variable_name)

if __name__ == '__main__':
    variables = {
        "Crop Development Stage (DVS)": "crop_development_stage",
        "Total Above Ground Production (TAGP)": "total_above_ground_production",
        "Total Weight Storage Organs (TWSO)": "total_weight_storage_organs"
    }
    years = ["2019", "2020", "2021", "2022", "2023"]

    for year in years:
        for var in variables.values():
            process_year_folder(year, var)

    print("Procesamiento completado.")
