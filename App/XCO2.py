import os
import boto3
import numpy as np
import numba
import pandas as pd
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score

# Cargar variables de entorno (solo necesario si se ejecuta localmente con un archivo .env)
if not os.getenv("GITHUB_ACTIONS"):
    from dotenv import load_dotenv
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "geltonas.tech"

# Cargar datos desde un archivo NetCDF en S3
def load_data_from_s3(file_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    data = response['Body'].read()
    
    with open('/tmp/temp_file.nc', 'wb') as temp_file:
        temp_file.write(data)
    
    with Dataset('/tmp/temp_file.nc', 'r') as nc_file:
        # Acceder a las variables
        time = nc_file.variables['time'][:]
        xco2 = nc_file.variables['XCO2'][:]
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

# Ruta del archivo NetCDF en S3
file_key = 'path_to_your_file.nc'

# Cargar y procesar datos
df = load_data_from_s3(file_key)

# Visualizar datos
plot_data(df)

# Entrenar y evaluar modelo
model = train_model(df)
