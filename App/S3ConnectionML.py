import os
import threading
import boto3
import zipfile
import tempfile
import pandas as pd
import numpy as np
from netCDF4 import Dataset
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

class DataFrameBuilder:
    def __init__(self):
        self.data_list = []

    def add_data(self, year, variable_name, data):
        df = pd.DataFrame({
            'Year': [year] * len(data),
            'Variable': [variable_name] * len(data),
            'Data': data
        })
        self.data_list.append(df)

    def build(self):
        if not self.data_list:
            return pd.DataFrame()  # Return empty DataFrame if no data
        return pd.concat(self.data_list, ignore_index=True)

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

def list_netcdf_variables(file_path):
    with Dataset(file_path, 'r') as nc:
        variables = list(nc.variables.keys())
        print(f"Variables en {file_path}: {variables}")
    return variables

def read_netcdf_with_chunks(file_path, variable_name, chunk_size=1000):
    data = []

    with Dataset(file_path, 'r') as nc:
        if variable_name in nc.variables:
            var_data = nc.variables[variable_name]
            for i in range(0, var_data.shape[0], chunk_size):
                chunk = var_data[i:i+chunk_size].flatten()
                data.append(chunk)
        else:
            print(f"Advertencia: '{variable_name}' no encontrado en {file_path}")

    if len(data) > 0:
        return np.concatenate(data)
    else:
        return np.array([])  # Retorna un array vacío si no se encuentra la variable

def process_netcdf_files(data_dir='/tmp', builder=None):
    files = [f for f in os.listdir(data_dir) if f.endswith('.nc')]

    for file_name in files:
        file_path = os.path.join(data_dir, file_name)
        print(f"Procesando {file_name}...")
        file_variables = list_netcdf_variables(file_path)

        for variable_name in file_variables:
            data = read_netcdf_with_chunks(file_path, variable_name)
            if len(data) > 0:
                year = file_name.split('_')[2]
                builder.add_data(year, variable_name, data)

def upload_to_s3(df, s3_prefix):
    # Save the DataFrame to a CSV file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        df.to_csv(temp_file.name, index=False)
        temp_file_path = temp_file.name

    # Upload the CSV file to S3
    s3_client.upload_file(temp_file_path, BUCKET_NAME, s3_prefix)
    print(f"Archivo CSV subido a S3 en {s3_prefix}")

def process_year_folder(year):
    variables = [
        "crop_development_stage",
        "total_above_ground_production",
        "total_weight_storage_organs"
    ]

    for var in variables:
        s3_prefix = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
        download_and_extract_from_s3(s3_prefix)

        # Crear una instancia de DataFrameBuilder
        builder = DataFrameBuilder()

        # Procesar archivos NetCDF
        process_netcdf_files(data_dir='/tmp', builder=builder)

        # Construir el DataFrame
        data_df = builder.build()

        # Subir el DataFrame a S3
        if not data_df.empty:
            s3_upload_prefix = f'processed_data/{year}/{var}_processed.csv'
            upload_to_s3(data_df, s3_upload_prefix)

        # Limpiar archivos temporales
        for file in os.listdir('/tmp'):
            file_path = os.path.join('/tmp', file)
            if os.path.isfile(file_path):
                os.remove(file_path)

def worker(year):
    print(f"Inicio del procesamiento para el año {year}")
    process_year_folder(year)
    print(f"Fin del procesamiento para el año {year}")

# Variables y años
years = ["2019", "2020", "2021", "2022", "2023"]

# Crear y lanzar los hilos
threads = []
for year in years:
    thread = threading.Thread(target=worker, args=(year,))
    threads.append(thread)
    thread.start()

# Esperar a que todos los hilos terminen
for thread in threads:
    thread.join()

print("Procesamiento completado.")
