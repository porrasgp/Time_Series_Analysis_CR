import os
import tempfile
import zipfile
import pandas as pd
import numpy as np
import xarray as xr
from netCDF4 import Dataset
from dotenv import load_dotenv

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "maize-climate-data-store"

# Crear cliente S3
import boto3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def download_and_extract_zip_from_s3(s3_prefix, extract_to='/tmp'):
    """Descargar y extraer archivos ZIP desde S3."""
    objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_prefix)
    
    if 'Contents' in objects:
        for obj in objects['Contents']:
            s3_key = obj['Key']
            if s3_key.endswith('.zip'):
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
                    temp_file_path = temp_file.name
                
                # Extraer el archivo ZIP
                with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)
                
                print(f"Archivo {s3_key} descargado y extraído en {extract_to}")
    else:
        print(f"No se encontraron objetos en {s3_prefix}")

def extract_and_load_nc_data_by_chunks(file_path, variable_name):
    """Leer archivos NetCDF y extraer datos específicos usando xarray."""
    if not os.path.exists(file_path):
        print(f"Archivo {file_path} no encontrado.")
        return None

    ds = xr.open_dataset(file_path)
    if variable_name in ds.variables:
        data = ds[variable_name].values.flatten()
        return data
    else:
        print(f"Advertencia: '{variable_name}' no encontrado en {file_path}")
        return None

def process_year_data(year):
    """Procesar y combinar datos para un año específico."""
    variables = {
        "crop_development_stage": "DVS",
        "total_above_ground_production": "TAGP",
        "total_weight_storage_organs": "TWSO"
    }
    
    base_directory = '/tmp'
    zip_files = {
        "crop_development_stage": f"crop_productivity_indicators/{year}/crop_development_stage_year_{year}.zip",
        "total_above_ground_production": f"crop_productivity_indicators/{year}/total_above_ground_production_year_{year}.zip",
        "total_weight_storage_organs": f"crop_productivity_indicators/{year}/total_weight_storage_organs_year_{year}.zip"
    }
    
    # Descargar y extraer los tres archivos ZIP
    for key, zip_file in zip_files.items():
        download_and_extract_zip_from_s3(zip_file)
    
    # Cargar los datos de los tres ZIPs
    data = {}
    for key, var_name in variables.items():
        zip_file_name = [f for f in os.listdir(base_directory) if f.startswith(f"Maize_{var_name}_C3S-glob-agric_{year}_1_{year}-") and f.endswith('.nc')]
        if zip_file_name:
            file_path = os.path.join(base_directory, zip_file_name[0])
            data[key] = extract_and_load_nc_data_by_chunks(file_path, var_name)
    
    # Verificar y combinar los datos
    if all(key in data for key in variables.keys()):
        combined_data = pd.DataFrame({
            "crop_development_stage": data["crop_development_stage"],
            "total_above_ground_production": data["total_above_ground_production"],
            "total_weight_storage_organs": data["total_weight_storage_organs"]
        })
        print(f"Datos combinados para el año {year}:")
        print(combined_data.head())
    else:
        print(f"Datos incompletos para el año {year}.")

def main():
    years = ["2023"]
    
    for year in years:
        process_year_data(year)

if __name__ == "__main__":
    main()
