import cdsapi
import os
import boto3
import numpy as np
import numba
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score
from dotenv import load_dotenv
import cdstoolbox as ct

# Constants
ECV = 'satellite-carbon-dioxide'
VARIABLE = 'xco2'
DISPLAY_UNITS = 'ppm'
LEGEND_TITLE = 'Parts per million'
VERSIONS = ['3', '3.1', '4.1', '4.2', '4.3'][::-1]
VERSION_END_YEARS = {
    '3': 2016,
    '3.1': 2017,
    '4.1': 2018,
    '4.2': 2019,
    '4.3': 2019,
}
BASELINE_2000, YEAR_CHANGE_RATE, C_RANGE = 360, 2, 15
PALETTE = "eccharts_rainbow_purple_red_15"
NCOLOURS = 15
VERSION_YEARS = {
    v: [str(x) for x in range(2003, VERSION_END_YEARS[v]+1)]
    for v in VERSIONS
}

# Create a client instance
client = ct.Client()

# Retrieve data function
def retrieve_data(ecv, variable, version, years):
    return client.retrieve(
        ecv,
        {
            'processing_level': 'level_2',
            'variable': variable,
            'sensor_and_algorithm': 'sciamachy_wfmd',
            'year': years,
            'month': [
                '01', '02', '03', '04', '05', '06',
                '07', '08', '09', '10', '11', '12'
            ],
            'day': [
                '01', '02', '03', '04', '05', '06',
                '07', '08', '09', '10', '11', '12',
                '13', '14', '15', '16', '17', '18',
                '19', '20', '21', '22', '23', '24',
                '25', '26', '27', '28', '29', '30', '31'
            ],
            'version': version,
            'format': 'zip'
        }
    )

# Loop through versions and retrieve data
for version in VERSIONS:
    years = VERSION_YEARS[version]
    data = retrieve_data(ECV, VARIABLE, version, years)
    # Save the data
    with open(f'data_{version}.zip', 'wb') as f:
        f.write(data.content)
    print(f'Data for version {version} retrieved and saved.')

# End of script
