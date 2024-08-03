import cdsapi
import os

# Explicitly set the CDS API config path
config_path = os.path.join(os.path.dirname(__file__), '.cdsapirc')
os.environ['CDSAPI_CONFIG'] = config_path

# Define the dataset and request parameters
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

# Initialize the CDS API client
client = cdsapi.Client()

# Define the folder where the data will be saved
folder_name = './Data'
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

# Define the path to save the downloaded file
file_path = os.path.join(folder_name, 'output.nc')

# Retrieve the data and save it to the specified file
client.retrieve(dataset, request).download(file_path)

print(f"Data has been saved to {file_path}")
