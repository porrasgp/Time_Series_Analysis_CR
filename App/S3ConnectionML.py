import os
import boto3
import zipfile
import tempfile
import pandas as pd
import numpy as np
from netCDF4 import Dataset
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def download_and_extract_from_s3(s3_prefix):
    with tempfile.TemporaryDirectory() as temp_dir:
        objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_prefix)
        
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_key = obj['Key']
                if s3_key.endswith('.zip'):
                    try:
                        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                            s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
                            temp_file_path = temp_file.name
                        
                        # Extraer el archivo ZIP
                        with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                            zip_ref.extractall(temp_dir)
                        
                        print(f"Archivo {s3_key} descargado y extraído en {temp_dir}")
                    except Exception as e:
                        print(f"Error al procesar {s3_key}: {e}")
        else:
            print(f"No se encontraron objetos en {s3_prefix}")

        # Retornar el directorio temporal para el procesamiento posterior
        return temp_dir

def list_netcdf_variables(file_path):
    try:
        with Dataset(file_path, 'r') as nc:
            return list(nc.variables.keys())
    except Exception as e:
        print(f"Error leyendo el archivo NetCDF {file_path}: {e}")
        return []

def read_netcdf_with_chunks(file_path, variable_name, chunk_size=1000):
    data = []
    
    try:
        with Dataset(file_path, 'r') as nc:
            if variable_name in nc.variables:
                var_data = nc.variables[variable_name]
                for i in range(0, var_data.shape[0], chunk_size):
                    chunk = var_data[i:i+chunk_size].flatten()
                    data.append(chunk)
            else:
                print(f"Advertencia: '{variable_name}' no encontrado en {file_path}")
    except Exception as e:
        print(f"Error leyendo la variable {variable_name} del archivo {file_path}: {e}")
    
    return np.concatenate(data) if data else np.array([])

def process_netcdf_from_s3(temp_dir):
    data_list = []
    
    try:
        files = [f for f in os.listdir(temp_dir) if f.endswith('.nc')]
        
        for file_name in files:
            file_path = os.path.join(temp_dir, file_name)
            print(f"Procesando {file_name}...")
            file_variables = list_netcdf_variables(file_path)
            
            for variable_name in file_variables:
                data = read_netcdf_with_chunks(file_path, variable_name)
                if len(data) > 0:
                    year = file_name.split('_')[2]
                    df = pd.DataFrame({
                        'Year': year,
                        'Variable': variable_name,
                        'Data': data
                    })
                    data_list.append(df)
                    
        if data_list:
            combined_df = pd.concat(data_list, ignore_index=True)
            output_file = os.path.join(temp_dir, "processed_data.csv")
            combined_df.to_csv(output_file, index=False)
            print(f"Datos procesados guardados en {output_file}")
            return combined_df
        else:
            print("No se generaron datos.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error procesando los archivos NetCDF en {temp_dir}: {e}")
        return pd.DataFrame()

def process_year_folder(year, variable):
    s3_prefix = f'crop_productivity_indicators/{year}/{variable}_year_{year}.zip'
    
    temp_dir = download_and_extract_from_s3(s3_prefix)
    if temp_dir:
        return process_netcdf_from_s3(temp_dir)

if __name__ == '__main__':
    variables = {
        "Crop Development Stage (DVS)": "crop_development_stage",
        "Total Above Ground Production (TAGP)": "total_above_ground_production",
        "Total Weight Storage Organs (TWSO)": "total_weight_storage_organs"
    }
    years = ["2019", "2020", "2021", "2022", "2023"]

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(process_year_folder, year, var) for year in years for var in variables.values()]
        for future in as_completed(futures):
            try:
                future.result()  # Obtener el resultado para manejar cualquier excepción
            except Exception as e:
                print(f"Error al procesar una carpeta: {e}")

    print("Procesamiento completado.")
