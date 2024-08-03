import os
import cdsapi

dataset = "sis-agroproductivity-indicators"
request = {
    'product_family': ['crop_productivity_indicators'],
    'variable': ['total_above_ground_production'],
    'crop_type': ['maize'],
    'year': '2023',
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['10', '20', '28', '30', '31'],
    'growing_season': ['1st_season_per_campaign'],
    'harvest_year': '2023'
}

client = cdsapi.Client()
folder_name = 'App/Data'
zip_file_path = os.path.join(folder_name, 'data.zip')

# Ensure the directory exists
os.makedirs(folder_name, exist_ok=True)

print(f"Attempting to save data to: {zip_file_path}")

try:
    # Retrieve and download data
    client.retrieve(dataset, request).download(zip_file_path)
    print(f"Data download completed. File saved to: {zip_file_path}")
except Exception as e:
    print(f"Error downloading data: {e}")
