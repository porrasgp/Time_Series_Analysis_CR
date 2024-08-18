import os
import boto3
import zipfile
import tempfile
import xarray as xr
import numpy as np
from dotenv import load_dotenv

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

def download_and_extract_from_s3(s3_prefix, extract_to='/tmp'):
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

def check_netcdf_content(file_path):
    with xr.open_dataset(file_path) as ds:
        print(f"Variables en {file_path}:")
        for var in ds.data_vars:
            data = ds[var].values
            print(f"{var}:")
            print(f" - Shape: {data.shape}")
            print(f" - Non-NaN Values: {np.count_nonzero(~np.isnan(data))}")
            print(f" - Sample Data:\n{data.flatten()[:10]}")  # Imprime una muestra de los datos

def process_netcdf_files(data_dir='/tmp'):
    datasets = []
    files = [f for f in os.listdir(data_dir) if f.endswith('.nc')]
    
    for file_name in files:
        file_path = os.path.join(data_dir, file_name)
        print(f"Procesando {file_name}...")
        check_netcdf_content(file_path)  # Verifica el contenido del archivo NetCDF
        ds = xr.open_dataset(file_path)
        datasets.append(ds)
    
    return datasets

def merge_datasets(datasets):
    if not datasets:
        print("No hay datasets para combinar.")
        return None
    
    combined_ds = xr.concat(datasets, dim='time')
    print("Dataset combinado:")
    print(combined_ds)
    return combined_ds

def print_dataset_summary(ds):
    if ds is None:
        print("Dataset vacío.")
        return
    
    print("Resumen del Dataset Combinado:")
    print(ds)
    print("\nVariables disponibles:")
    for var in ds.data_vars:
        print(f"{var}:")
        data = ds[var].values
        print(f" - Shape: {data.shape}")
        print(f" - Non-NaN Values: {np.count_nonzero(~np.isnan(data))}")
        print(f" - Sample Data:\n{data.flatten()[:10]}")

def upload_to_s3(ds, s3_prefix):
    # Guardar el dataset combinado en un archivo NetCDF temporal
    with tempfile.NamedTemporaryFile(delete=False, suffix='.nc') as temp_file:
        ds.to_netcdf(temp_file.name)
        temp_file_path = temp_file.name
    
    # Subir el archivo NetCDF a S3
    s3_client.upload_file(temp_file_path, BUCKET_NAME, s3_prefix)
    print(f"Archivo NetCDF subido a S3 en {s3_prefix}")

def process_year_folder(year):
    variables = [
        "crop_development_stage",
        "total_above_ground_production",
        "total_weight_storage_organs"
    ]
    
    for var in variables:
        s3_prefix = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
        download_and_extract_from_s3(s3_prefix)
        
        # Procesar los archivos NetCDF
        datasets = process_netcdf_files(data_dir='/tmp')
        
        # Combinar los datasets
        combined_ds = merge_datasets(datasets)
        
        # Imprimir el resumen del dataset combinado
        if combined_ds:
            print_dataset_summary(combined_ds)
            
            # Subir el dataset combinado a S3
            s3_upload_prefix = f'processed_data/{year}/{var}_processed.nc'
            upload_to_s3(combined_ds, s3_upload_prefix)
        
        # Limpiar archivos temporales
        for file in os.listdir('/tmp'):
            file_path = os.path.join('/tmp', file)
            if os.path.isfile(file_path):
                os.remove(file_path)

# Variables y años
years = ["2023"]

# Procesar los datos para cada año
for year in years:
    process_year_folder(year)

print("Procesamiento completado.")
