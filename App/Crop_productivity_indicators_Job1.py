import boto3
import os
import tempfile
import time
import zipfile
import pandas as pd
import numpy as np
from netCDF4 import Dataset
from dotenv import load_dotenv
import cdsapi

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
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

# Crear cliente CDS API
client = cdsapi.Client()

def wait_for_job_to_complete(client, request):
    """Esperar hasta que el trabajo esté completo antes de intentar descargar."""
    while True:
        try:
            client.retrieve(dataset, request)
            break  # Salir del bucle si el trabajo está completo y listo para descargar
        except Exception as e:
            if "Result not ready, job is running" in str(e):
                print("El trabajo aún está en curso, esperando 100 segundos antes de verificar nuevamente...")
                time.sleep(100)  # Esperar 100 segundos antes de verificar nuevamente
            else:
                raise  # Lanzar cualquier otra excepción

def upload_to_s3(temp_file_path, s3_client, bucket_name, s3_key):
    """Subir archivo a S3."""
    if os.path.getsize(temp_file_path) > 0:
        s3_client.upload_file(temp_file_path, bucket_name, s3_key)
        print(f"Archivo subido al bucket S3 {bucket_name} con la clave {s3_key}")
    else:
        print("El archivo temporal está vacío. No se recuperaron datos.")

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

def read_netcdf_with_chunks(file_path, variable_name, chunk_size=1000):
    """Leer archivos NetCDF en chunks y extraer datos específicos."""
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
    except FileNotFoundError:
        print(f"Archivo {file_path} no encontrado.")
    
    if len(data) > 0:
        return np.concatenate(data)
    else:
        return np.array([])  # Retornar un array vacío si no se encuentra la variable

# Variables y años
variables = {
    "Crop Development Stage (DVS)": "crop_development_stage",
    "Total Above Ground Production (TAGP)": "total_above_ground_production",
    "Total Weight Storage Organs (TWSO)": "total_weight_storage_organs"
}
years = ["2019", "2020", "2021", "2022", "2023"]

# Descargar y extraer archivos
for var in variables.values():
    for year in years:
        s3_prefix = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
        download_and_extract_zip_from_s3(s3_prefix)

# Verificar los archivos extraídos
print("Archivos extraídos en /tmp:")
print(os.listdir('/tmp'))

# Procesar los datos y cargar en un DataFrame
all_data = pd.DataFrame()

for var_name, var_key in variables.items():
    for year in years:
        file_name = f"{var_key}_year_{year}.nc"
        file_path = f"/tmp/{file_name}"
        if os.path.isfile(file_path):
            var_data = read_netcdf_with_chunks(file_path, var_name)
            if var_data.size > 0:
                all_data[var_name] = var_data
            else:
                print(f"No se encontraron datos para '{var_name}' en {file_path}")
        else:
            print(f"Archivo {file_name} no encontrado en {file_path}")

# Verificar si los datos se han cargado correctamente
if all_data.empty:
    print("No se han cargado datos en el DataFrame.")
else:
    print(all_data.head())
    print(all_data.describe())
