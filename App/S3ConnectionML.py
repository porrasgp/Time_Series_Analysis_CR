import os
import boto3
import zipfile
import tempfile
import pandas as pd
import numpy as np
import xarray as xr
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "maize-climate-data-store"

# Crear cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Función para descargar y extraer archivos ZIP desde S3
def download_and_extract_from_s3(s3_prefix, extract_to='/tmp'):
    try:
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
                    time.sleep(2)  # Agregar tiempo de espera
        else:
            print(f"No se encontraron objetos en {s3_prefix}")
    except Exception as e:
        print(f"Error al descargar o extraer archivo: {e}")

# Función para procesar un archivo NetCDF y actualizar el DataFrame
def process_netcdf_file(file_path):
    try:
        data_list = []
        ds = xr.open_dataset(file_path)
        for variable_name in ds.data_vars:
            data = ds[variable_name].values.flatten()
            year = file_path.split('_')[2]
            df = pd.DataFrame({
                'Year': year,
                'Variable': variable_name,
                'Data': list(data)  # Convertir el array a una lista para ser compatible con DataFrame
            })
            data_list.append(df)
        return pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()
    except Exception as e:
        print(f"Error al procesar {file_path}: {e}")
        return pd.DataFrame()

# Función para procesar todos los archivos NetCDF desde S3
def process_netcdf_from_s3(data_dir='/tmp'):
    try:
        files = [f for f in os.listdir(data_dir) if f.endswith('.nc')]
        data_df = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_netcdf_file, os.path.join(data_dir, file_name)) for file_name in files]
            for future in as_completed(futures):
                result_df = future.result()
                data_df = pd.concat([data_df, result_df], ignore_index=True)
        return data_df
    except Exception as e:
        print(f"Error al procesar archivos desde {data_dir}: {e}")
        return pd.DataFrame()

# Variables y años
variables = {
    "Crop Development Stage (DVS)": "crop_development_stage",
    "Total Above Ground Production (TAGP)": "total_above_ground_production",
    "Total Weight Storage Organs (TWSO)": "total_weight_storage_organs"
}
years = ["2019", "2020", "2021", "2022", "2023"]

# Descargar y extraer archivos desde S3
for var in variables.values():
    for year in years:
        s3_prefix = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
        download_and_extract_from_s3(s3_prefix)

# Procesar los archivos NetCDF y organizar los datos
data_df = process_netcdf_from_s3()

# Mostrar la descripción estadística de los datos
print(data_df.describe())

# Imprimir una vista previa de los datos
print(data_df.head())
