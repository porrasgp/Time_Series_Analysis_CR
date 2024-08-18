import os
import boto3
import tempfile
import zipfile

# Cargar variables de entorno
from dotenv import load_dotenv
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
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
                        temp_file_path = temp_file.name
                    
                    # Extraer el archivo ZIP
                    with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                    
                    print(f"Archivo {s3_key} descargado y extraído en {temp_dir}")
        else:
            print(f"No se encontraron objetos en {s3_prefix}")

# Prueba simplificada
s3_prefix = 'crop_productivity_indicators/2019/crop_development_stage_year_2019.zip'
download_and_extract_from_s3(s3_prefix)
print("Prueba completada.")
