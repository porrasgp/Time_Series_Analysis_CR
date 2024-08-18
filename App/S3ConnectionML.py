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

def download_and_extract_zip_from_s3(s3_key, extract_to='/tmp'):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name
        s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
    
    with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    # Remove the temporary file
    os.remove(temp_file_path)
    
    # List extracted files
    extracted_files = [os.path.join(extract_to, file) for file in os.listdir(extract_to) if file.endswith('.nc')]
    return extracted_files

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

# Variables y años a procesar
variables = {
    "Crop Development Stage (DVS)": "crop_development_stage_year_{}.zip",
    "Total Above Ground Production (TAGP)": "total_above_ground_production_year_{}.zip",
    "Total Weight Storage Organs (TWSO)": "total_weight_storage_organs_year_{}.zip"
}

years = ['2019', '2020', '2021', '2022', '2023']

# Procesar los datos por año y cargar en un DataFrame
all_data = pd.DataFrame()

for year in years:
    for var_name, zip_pattern in variables.items():
        s3_key = f'crop_productivity_indicators/{year}/{zip_pattern.format(year)}'
        extracted_files = download_and_extract_zip_from_s3(s3_key)
        
        for file_path in extracted_files:
            # Determinar el nombre de la variable basándonos en el nombre del archivo NetCDF
            if "crop_development_stage" in s3_key:
                variable_name = "Crop Development Stage (DVS)"
            elif "total_above_ground_production" in s3_key:
                variable_name = "Total Above Ground Production (TAGP)"
            elif "total_weight_storage_organs" in s3_key:
                variable_name = "Total Weight Storage Organs (TWSO)"
            else:
                continue
            
            var_data = read_netcdf_with_chunks(file_path, variable_name)
            if var_data.size > 0:
                if var_name in all_data.columns:
                    all_data[var_name] = np.concatenate((all_data[var_name], var_data))
                else:
                    all_data[var_name] = var_data

# Verificar si los datos se han cargado correctamente
print(all_data.head())
print(all_data.describe())

# Dividir los datos en conjunto de entrenamiento y prueba
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
