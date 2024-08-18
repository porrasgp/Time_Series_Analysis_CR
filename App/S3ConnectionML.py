import boto3
import zipfile
import io
import os
import xarray as xr
import numpy as np
from dotenv import load_dotenv
import h5netcdf
import netCDF4

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

def download_zip_from_s3(s3_key):
    """Descargar un archivo ZIP desde S3 en memoria."""
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    return io.BytesIO(response['Body'].read())

def extract_and_load_nc_data_by_chunks(zip_file, variable_name):
    """Extraer y cargar archivos NetCDF desde un archivo ZIP en fragmentos."""
    extracted_files = []
    with zipfile.ZipFile(zip_file) as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith('.nc'):
                extracted_files.append(file_name)
                with zip_ref.open(file_name) as file:
                    try:
                        ds = xr.open_dataset(file)
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
                        print(f"Archivo {file_name} no encontrado.")

def main():
    # Ajustar los nombres de archivo ZIP y sus claves en S3
    zip_files = {
        "Crop Development stage": "crop_productivity_indicators/2019/crop_development_stage_year_2019.zip",
        "Total Above ground production": "crop_productivity_indicators/2019/total_above_ground_production_year_2019.zip",
        "Total weight storage organs": "crop_productivity_indicators/2019/total_weight_storage_organs_year_2019.zip"
    }

    # Descargar y procesar archivos ZIP
    for var_name, s3_key in zip_files.items():
        zip_file = download_zip_from_s3(s3_key)
        extract_and_load_nc_data_by_chunks(zip_file, var_name)

if __name__ == "__main__":
    main()
