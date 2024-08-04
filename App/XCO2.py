import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# Cargar datos desde un archivo NetCDF
def load_data(file_path):
    with Dataset(file_path, 'r') as nc_file:
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

# Entrenar un modelo de regresión lineal
def train_model(df):
    # Seleccionar características y objetivo
    X = df[['latitude', 'longitude']]
    y = df['XCO2']
    
    # Dividir los datos en conjunto de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Crear y entrenar el modelo
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Hacer predicciones
    y_pred = model.predict(X_test)
    
    # Evaluar el modelo
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print(f"Mean Squared Error: {mse:.2f}")
    print(f"R^2 Score: {r2:.2f}")
    
    return model

# Ruta al archivo NetCDF
file_path = 'path_to_your_file.nc'

# Cargar y procesar datos
df = load_data(file_path)

# Visualizar datos
plot_data(df)

# Entrenar y evaluar modelo
model = train_model(df)
