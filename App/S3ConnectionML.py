import os
import boto3
import tempfile
import zipfile
import xarray as xr
import numpy as np
from dotenv import load_dotenv

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

def extract_and_load_nc_data_by_chunks(zip_file_path, variable_name):
    """Cargar los datos de archivos NetCDF desde un archivo ZIP en fragmentos."""
    extracted_files = []
    
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith('.nc'):
                extracted_files.append(file_name)
                zip_ref.extract(file_name, '/tmp')

    # Procesar archivos NetCDF en fragmentos
    data = []
    for file_name in extracted_files:
        file_path = os.path.join('/tmp', file_name)
        try:
            ds = xr.open_dataset(file_path)
            data.append(ds)
            print(f"Datos del archivo {file_name} ({variable_name}):")
            print(ds)
            print("\nVariables disponibles:")
            for data_var in ds.data_vars:
                print(f"{data_var}:")
                print(f" - Dimensiones: {ds[data_var].dims}")
                print(f" - Shape: {ds[data_var].shape}")
                print(f" - Valores no nulos: {np.count_nonzero(~np.isnan(ds[data_var].values))}")
                print(f" - Datos de muestra:\n{ds[data_var].values.flatten()[:10]}")
        except FileNotFoundError:
            print(f"Archivo {file_path} no encontrado.")
    
    return data

def main():
    # Ajustar las rutas y nombres de archivo según corresponda
    base_directory = '/tmp'
    zip_files = {
        "Crop Development stage": "crop_development_stage_year_2023.zip",
        "Total Above ground production": "total_above_ground_production_year_2023.zip",
        "Total weight storage organs": "total_weight_storage_organs_year_2023.zip"
    }

    # Descargar y procesar archivos ZIP
    for var_name, zip_file in zip_files.items():
        zip_file_path = os.path.join(base_directory, zip_file)
        download_and_extract_zip_from_s3(f'crop_productivity_indicators/2023/{zip_file}', base_directory)
        data = extract_and_load_nc_data_by_chunks(zip_file_path, var_name)

if __name__ == "__main__":
    main()
