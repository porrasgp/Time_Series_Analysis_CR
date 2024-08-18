import os
import boto3
import tempfile
import time
import zipfile
import xarray as xr
import numpy as np
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

def read_netcdf_with_xarray(file_path):
    """Leer archivos NetCDF con xarray."""
    try:
        ds = xr.open_dataset(file_path)
        return ds
    except FileNotFoundError:
        print(f"Archivo {file_path} no encontrado.")
        return None

def process_files_for_year(year, variables):
    """Descargar, extraer y procesar archivos NetCDF para un año específico."""
    for var in variables:
        s3_prefix = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
        download_and_extract_zip_from_s3(s3_prefix)

def main():
    # Ajustar las variables con los nombres correspondientes
    variables = {
        'DVS': 'crop_development_stage',
        'TAGP': 'total_above_ground_production',
        'TWSO': 'total_weight_storage_organs'
    }
    years = ["2023"]

    # Procesar en lotes por año
    for year in years:
        process_files_for_year(year, variables.values())

    # Verificar los archivos extraídos
    extracted_files = os.listdir('/tmp')
    print("Archivos extraídos en /tmp:")
    print(extracted_files)

    # Procesar y explorar los archivos NetCDF con xarray
    for short_var, long_var in variables.items():
        for year in years:
            # Buscar archivos NetCDF específicos en el directorio temporal usando los nombres extraídos
            file_prefix = f"Maize_{short_var}_C3S-glob-agric_{year}_1_{year}-"
            matching_files = [f for f in extracted_files if f.startswith(file_prefix) and f.endswith('.nc')]

            if matching_files:
                for file in matching_files:
                    full_path = os.path.join('/tmp', file)
                    ds = read_netcdf_with_xarray(full_path)
                    if ds is not None:
                        print(f"Datos del archivo {file} ({long_var}):")
                        print(ds)
                        print("\nVariables disponibles:")
                        for data_var in ds.data_vars:
                            print(f"{data_var}:")
                            print(f" - Dimensiones: {ds[data_var].dims}")
                            print(f" - Shape: {ds[data_var].shape}")
                            print(f" - Valores no nulos: {np.count_nonzero(~np.isnan(ds[data_var].values))}")
                            print(f" - Datos de muestra:\n{ds[data_var].values.flatten()[:10]}")
                    else:
                        print(f"No se pudieron cargar los datos para '{long_var}' en {full_path}")
            else:
                print(f"Archivo para '{long_var}' en el año {year} no encontrado en /tmp")

if __name__ == "__main__":
    main()

