import os
import boto3
import zipfile
import tempfile
import numpy as np
import pandas as pd
from netCDF4 import Dataset
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "maize-climate-data-store"

# Crear el cliente de S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Función para listar y descargar todos los objetos en una carpeta
def download_all_from_s3(s3_prefix, extract_to='/tmp'):
    objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_prefix)
    
    if 'Contents' in objects:
        for obj in objects['Contents']:
            s3_key = obj['Key']
            if s3_key.endswith('.zip'):
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
                    temp_file_path = temp_file.name
                
                # Listar contenido del archivo ZIP antes de extraer
                with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                    print(f"Contenido de {s3_key}:")
                    for file_info in zip_ref.infolist():
                        print(f" - {file_info.filename}")
                    
                    # Extraer archivos NetCDF
                    for file_info in zip_ref.infolist():
                        if file_info.filename.endswith('.nc'):
                            zip_ref.extract(file_info, extract_to)
                            print(f"Archivo {file_info.filename} extraído en {extract_to}")
    else:
        print(f"No se encontraron objetos en {s3_prefix}")

# Descargar y extraer todos los archivos en la carpeta especificada
s3_prefix = 'crop_productivity_indicators/'
download_all_from_s3(s3_prefix)

# Verificar los archivos extraídos
print("Archivos extraídos en /tmp:")
print(os.listdir('/tmp'))

# Función para leer archivos NetCDF y cargar datos específicos por chunks
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

# Variables y archivos extraídos a procesar
variables = {
    "Crop Development Stage (DVS)": "crop_development_stage_year_2019.nc",
    "Total Above Ground Production (TAGP)": "total_above_ground_production_year_2019.nc",
    "Total Weight Storage Organs (TWSO)": "total_weight_storage_organs_year_2019.nc"
}

# Procesar los datos y cargar en un DataFrame
all_data = pd.DataFrame()

for var_name, file_name in variables.items():
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

    # Dividir los datos en conjunto de entrenamiento y prueba
    if "Total Above Ground Production (TAGP)" in all_data.columns and "Total Weight Storage Organs (TWSO)" in all_data.columns:
        # Asegúrate de que las columnas necesarias existan
        train_data, test_data = train_test_split(all_data, test_size=0.2, random_state=42)

        # Entrenar un modelo de regresión lineal
        model = LinearRegression
