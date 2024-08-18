import os
import boto3
import zipfile
import tempfile
import pandas as pd
import numpy as np
from netCDF4 import Dataset
from dotenv import load_dotenv
from threading import Thread

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

# Builder Pattern
class DataFrameBuilder:
    def __init__(self):
        self.data = []

    def add_data(self, year, variable_name, lat, lon, time, values):
        # Verificar si todas las listas tienen la misma longitud
        if not (len(lat) == len(lon) == len(time) == len(values)):
            print(f"Advertencia: Las longitudes de los datos no coinciden para {variable_name} del año {year}.")
            return
        
        df = pd.DataFrame({
            'Year': year,
            'Variable': variable_name,
            'Latitude': lat,
            'Longitude': lon,
            'Time': time,
            'Values': values
        })
        self.data.append(df)

    def get_dataframe(self):
        return pd.concat(self.data, ignore_index=True)

# Función para descargar y extraer archivos ZIP desde S3
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

# Función para listar variables disponibles en un archivo NetCDF
def list_netcdf_variables(file_path):
    try:
        with Dataset(file_path, 'r') as nc:
            variables = list(nc.variables.keys())
            print(f"Variables en {file_path}: {variables}")
    except RuntimeError as e:
        print(f"Error al abrir el archivo NetCDF {file_path}: {e}")
        variables = []
    
    return variables

# Función para leer datos de un archivo NetCDF
def read_netcdf_data(file_path, variable_name):
    lat, lon, time, values = [], [], [], []
    
    try:
        with Dataset(file_path, 'r') as nc:
            if variable_name in nc.variables:
                var_data = nc.variables[variable_name]
                lat = nc.variables['lat'][:].flatten()
                lon = nc.variables['lon'][:].flatten()
                time = nc.variables['time'][:].flatten()
                values = var_data[:].flatten()
            else:
                print(f"Advertencia: '{variable_name}' no encontrado en {file_path}")
    except RuntimeError as e:
        print(f"Error al abrir el archivo NetCDF {file_path}: {e}")
    
    return lat, lon, time, values

# Función para procesar archivos NetCDF y construir el DataFrame
def process_netcdf_from_s3(data_dir='/tmp', builder=None):
    files = [f for f in os.listdir(data_dir) if f.endswith('.nc')]
    
    for file_name in files:
        file_path = os.path.join(data_dir, file_name)
        print(f"Procesando {file_name}...")
        file_variables = list_netcdf_variables(file_path)
        
        year = file_name.split('_')[2]
        
        for variable_name in file_variables:
            if variable_name in ['TAGP', 'DVS', 'TWSO']:
                lat, lon, time, values = read_netcdf_data(file_path, variable_name)
                builder.add_data(year, variable_name, lat, lon, time, values)

# Función para procesar una carpeta de datos de un año
def process_year_data(year, variables):
    for var in variables:
        s3_prefix = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
        download_and_extract_from_s3(s3_prefix, extract_to=f'/tmp/{year}')
        
        builder = DataFrameBuilder()
        process_netcdf_from_s3(data_dir=f'/tmp/{year}', builder=builder)
        
        df = builder.get_dataframe()
        if not df.empty:
            df.to_csv(f'/tmp/{year}/{year}_data.csv', index=False)
            print(f"Datos del año {year} procesados y guardados en /tmp/{year}/{year}_data.csv")
        else:
            print(f"No se encontraron datos procesables para el año {year}")

# Variables y años
variables = ["crop_development_stage", "total_above_ground_production", "total_weight_storage_organs"]
years = ["2019", "2020", "2021", "2022", "2023"]

# Crear y ejecutar hilos para procesar los datos de cada año
threads = []
for year in years:
    thread = Thread(target=process_year_data, args=(year, variables))
    thread.start()
    threads.append(thread)

# Esperar a que todos los hilos terminen
for thread in threads:
    thread.join()

print("Procesamiento completado.")
