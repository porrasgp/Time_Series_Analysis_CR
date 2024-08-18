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

def process_netcdf_files(data_dir='/tmp'):
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.nc')]
    print(f"Procesando archivos: {files}...")
    
    # Leer múltiples archivos NetCDF usando xarray
    ds = xr.open_mfdataset(files, combine='by_coords', chunks={'time': 10})
    return ds

def print_dataset_summary(ds):
    print("Resumen del Dataset Combinado:")
    print(ds)
    print("\nVariables disponibles:")
    for var in ds.data_vars:
        print(f"{var}:")
        data = ds[var].values
        print(f" - Shape: {data.shape}")
        print(f" - Non-NaN Values: {np.count_nonzero(~np.isnan(data))}")
        # Imprimir una muestra de los datos
        print(f" - Sample Data:\n{data.flatten()[:10]}")

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
        ds = process_netcdf_files(data_dir='/tmp')
        
        # Imprimir el resumen del dataset combinado
        if ds:
            print_dataset_summary(ds)
        
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
