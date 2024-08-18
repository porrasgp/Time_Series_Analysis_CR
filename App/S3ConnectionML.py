import os
import boto3
import zipfile
import tempfile
import pandas as pd
import numpy as np
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

# Crear cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Función para subir un archivo a S3
def upload_to_s3(file_path, s3_client, bucket, key):
    with open(file_path, 'rb') as data:
        s3_client.upload_fileobj(data, bucket, key)
    print(f"Archivo subido a S3 en {key}")

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
                
                # Extraer el archivo ZIP
                with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)
                
                print(f"Archivo {s3_key} descargado y extraído en {extract_to}")
    else:
        print(f"No se encontraron objetos en {s3_prefix}")

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
        download_all_from_s3(s3_prefix)

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

    # Dividir los datos en conjunto de entrenamiento y prueba
    if "Total Above Ground Production (TAGP)" in all_data.columns and "Total Weight Storage Organs (TWSO)" in all_data.columns:
        # Asegúrate de que las columnas necesarias existan
        train_data, test_data = train_test_split(all_data, test_size=0.2, random_state=42)

        # Entrenar un modelo de regresión lineal
        model = LinearRegression()
        model.fit(train_data[["Total Above Ground Production (TAGP)", "Total Weight Storage Organs (TWSO)"]], train_data["Crop Development Stage (DVS)"])

        # Realizar predicciones
        predictions = model.predict(test_data[["Total Above Ground Production (TAGP)", "Total Weight Storage Organs (TWSO)"]])

        # Evaluar el modelo
        mse = mean_squared_error(test_data["Crop Development Stage (DVS)"], predictions)
        r2 = r2_score(test_data["Crop Development Stage (DVS)"], predictions)

        print(f"MSE: {mse}")
        print(f"R^2: {r2}")

        # Graficar los resultados
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x=test_data["Crop Development Stage (DVS)"], y=predictions, label="Predicciones")
        sns.lineplot(x=test_data["Crop Development Stage (DVS)"], y=test_data["Crop Development Stage (DVS)"], color='red', label="Valor Real")
        plt.xlabel("Crop Development Stage (DVS)")
        plt.ylabel("Predicciones")
        plt.title("Predicciones vs. Valores Reales")
        plt.legend()
        plt.show()
    else:
        print("Las columnas necesarias para el modelo no están presentes en los datos.")
