import os
import boto3
import zipfile
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score
from dotenv import load_dotenv

# Cargar variables de entorno (solo necesario si se ejecuta localmente con un archivo .env)
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "geltonas.tech"
ZIP_FILE_KEY = "CO2.zip"

# Descargar y descomprimir archivo desde S3
def download_and_extract_zip(file_key, extract_to='/tmp'):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    # Descargar el archivo ZIP
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    zip_file_path = os.path.join(extract_to, 'temp_file.zip')
    
    with open(zip_file_path, 'wb') as zip_file:
        zip_file.write(response['Body'].read())
    
    # Descomprimir el archivo ZIP
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    # Listar archivos descomprimidos
    extracted_files = os.listdir(extract_to)
    return [os.path.join(extract_to, file) for file in extracted_files if file.endswith('.nc')]

# Imprimir información sobre las variables del archivo NetCDF
def print_netcdf_info(file_path):
    with Dataset(file_path, 'r') as nc_file:
        print("Variables disponibles en el archivo NetCDF:")
        for var_name in nc_file.variables:
            print(f"Variable: {var_name}")

# Cargar datos desde un archivo NetCDF
def load_data_from_netcdf(file_path):
    with Dataset(file_path, 'r') as nc_file:
        # Imprimir variables disponibles para depuración
        print_netcdf_info(file_path)
        
        # Acceder a las variables
        time = nc_file.variables['time'][:]
        xco2 = nc_file.variables['XCO2'][:]  # Cambiar 'XCO2' si es necesario
        lat = nc_file.variables['latitude'][:]
        lon = nc_file.variables['longitude'][:]
        
        # Crear un DataFrame
        df = pd.DataFrame({
            'time': time,
            'XCO2': xco2,
            'latitude': lat,
            'longitude': lon
        })
        
    return df

# Visualizar los datos
def plot_data(df):
    plt.figure(figsize=(12, 6))
    plt.scatter(df['longitude'], df['latitude'], c=df['XCO2'], cmap='viridis', s=10)
    plt.colorbar(label='XCO2 (ppm)')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('CO2 Concentration by Location')
    plt.show()

# Entrenar un modelo SVM
def train_model(df):
    # Seleccionar características y objetivo
    X = df[['latitude', 'longitude']]
    y = df['XCO2']
    
    # Dividir los datos en conjunto de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Crear y entrenar el modelo
    model = SVC(kernel='linear')
    model.fit(X_train, y_train)
    
    # Hacer predicciones
    y_pred = model.predict(X_test)
    
    # Evaluar el modelo
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"Accuracy Score: {accuracy:.2f}")
    
    return model

# Ruta del archivo ZIP en S3
zip_file_key = ZIP_FILE_KEY

# Descargar y extraer archivo ZIP
extracted_files = download_and_extract_zip(zip_file_key)
if extracted_files:
    for file in extracted_files:
        if file.endswith('.nc'):
            # Cargar y procesar datos del archivo NetCDF
            df = load_data_from_netcdf(file)
            
            # Visualizar datos
            plot_data(df)
            
            # Entrenar y evaluar modelo
            model = train_model(df)
else:
    print("No se encontraron archivos NetCDF en el archivo ZIP.")
