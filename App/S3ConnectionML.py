import pandas as pd
import numpy as np
from netCDF4 import Dataset
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns

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
    "Crop Development Stage (DVS)": "crop_development_stage_year_2019.zip",
    "Total Above Ground Production (TAGP)": "total_above_ground_production_year_2019.zip",
    "Total Weight Storage Organs (TWSO)": "total_weight_storage_organs_year_2019.zip"
}

# Procesar los datos y cargar en un DataFrame
all_data = pd.DataFrame()

for var_name, zip_name in variables.items():
    file_path = f"/tmp/{zip_name}"  # Ubicación del archivo extraído
    var_data = read_netcdf_with_chunks(file_path, var_name)
    if var_data.size > 0:
        all_data[var_name] = var_data if var_name not in all_data else np.concatenate((all_data[var_name], var_data))

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
