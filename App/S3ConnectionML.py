import os
import boto3
import tempfile
import zipfile
import xarray as xr
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
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

def download_and_extract_zip_from_s3(s3_key, extract_to='/tmp'):
    """Descargar y extraer un archivo ZIP desde S3."""
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
        temp_file_path = temp_file.name

    # Extraer el archivo ZIP
    with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    print(f"Archivo {s3_key} descargado y extraído en {extract_to}")
    return os.listdir(extract_to)

def process_netcdf(file_path):
    """Leer el archivo NetCDF, añadir una columna y eliminar filas con valores NaN."""
    try:
        ds = xr.open_dataset(file_path, engine='netcdf4')
        print(f"Datos del archivo {file_path}:")
        print(ds)

        # Convertir el dataset a un DataFrame de pandas
        df = ds.to_dataframe().reset_index()

        # Añadir una columna adicional (ejemplo: una constante o cálculo)
        df['new_column'] = df['TWSO'].mean()  # Ejemplo de nueva columna con valor medio

        # Eliminar filas con valores NaN
        df = df.dropna()

        print(f"Datos procesados del archivo {file_path}:")
        print(df.head())

        return df

    except FileNotFoundError:
        print(f"Archivo {file_path} no encontrado.")
        return None
    except Exception as e:
        print(f"Error al procesar el archivo {file_path}: {e}")
        return None

def main():
    # Ajustar los nombres de archivo ZIP y sus claves en S3
    zip_files = {
        "Crop Development stage": "crop_productivity_indicators/2019/crop_development_stage_year_2019.zip",
        "Total Above ground production": "crop_productivity_indicators/2019/total_above_ground_production_year_2019.zip",
        "Total weight storage organs": "crop_productivity_indicators/2019/total_weight_storage_organs_year_2019.zip"
    }

    # Procesar cada archivo ZIP
    for key, s3_key in zip_files.items():
        print(f"Procesando {key}...")
        extracted_files = download_and_extract_zip_from_s3(s3_key)
        
        for file_name in extracted_files:
            if file_name.endswith('.nc'):
                full_path = os.path.join('/tmp', file_name)
                df = process_netcdf(full_path)
                
                if df is not None:
                    # Guardar DataFrame procesado si es necesario
                    output_csv_path = full_path.replace('.nc', '_processed.csv')
                    df.to_csv(output_csv_path, index=False)
                    print(f"Datos procesados guardados en {output_csv_path}")
                else:
                    print(f"No se pudieron procesar los datos del archivo {file_name}")

if __name__ == "__main__":
    main()
