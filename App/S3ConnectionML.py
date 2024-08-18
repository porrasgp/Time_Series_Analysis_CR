import os
import io
import boto3
import zipfile
import pandas as pd
import xarray as xr
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

# Función para descargar y procesar archivos ZIP desde S3 en memoria
def download_and_process_zip(s3_key):
    try:
        # Descargar archivo ZIP a memoria
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        zip_file = io.BytesIO(obj['Body'].read())

        data_list = []
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.nc'):
                    with zip_ref.open(file_name) as file:
                        # Procesar archivo NetCDF directamente
                        try:
                            ds = xr.open_dataset(file, engine='netcdf4')
                            for variable_name in ds.data_vars:
                                data = ds[variable_name].to_dataframe().reset_index()
                                data['Variable'] = variable_name
                                data['File'] = file_name
                                data_list.append(data)
                        except Exception as e:
                            print(f"Error al procesar archivo NetCDF {file_name}: {e}")

        return pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()

    except Exception as e:
        print(f"Error al descargar o procesar archivo: {e}")
        return pd.DataFrame()

# Función para procesar todos los archivos NetCDF desde S3
def process_netcdf_from_s3(variables, years):
    all_data_df = pd.DataFrame()
    for var in variables.values():
        for year in years:
            s3_prefix = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
            df = download_and_process_zip(s3_prefix)
            all_data_df = pd.concat([all_data_df, df], ignore_index=True)
    return all_data_df

# Variables y años
variables = {
    "Crop Development Stage (DVS)": "crop_development_stage",
    "Total Above Ground Production (TAGP)": "total_above_ground_production",
    "Total Weight Storage Organs (TWSO)": "total_weight_storage_organs"
}
years = ["2019", "2020", "2021", "2022", "2023"]

# Procesar archivos desde S3
data_df = process_netcdf_from_s3(variables, years)

# Mostrar la descripción estadística de los datos
print(data_df.describe())

# Imprimir una vista previa de los datos
print(data_df.head())
