import cdsapi
import xarray as xr
import matplotlib.pyplot as plt
import numpy as np

# Constants
ECV = 'satellite-carbon-dioxide'
VARIABLE = 'xco2'
VERSION = '4.3'  # Example version
YEAR = '2012'    # Example year
MONTH = '01'     # Example month

# Initialize CDS API client
c = cdsapi.Client()

# Retrieve data
def retrieve_data(version, year, month):
    c.retrieve(
        ECV,
        {
            'processing_level': 'level_3',
            'variable': VARIABLE,
            'sensor_and_algorithm': 'merged_obs4mips',
            'version': version,
            'year': year,
            'month': month,
            'format': 'netcdf'
        },
        f'data_{version}_{year}_{month}.nc'
    )

# Call function to retrieve data
retrieve_data(VERSION, YEAR, MONTH)

# Load data with xarray
def plot_data(filepath):
    data = xr.open_dataset(filepath)
    variable_data = data[VARIABLE].isel(time=0)  # Select the first time step for simplicity

    # Plotting
    plt.figure(figsize=(10, 5))
    plt.title(f'{VARIABLE.upper()} - {YEAR}-{MONTH}')
    variable_data.plot()
    plt.colorbar(label='CO2 concentration (ppm)')
    plt.show()

# Plot retrieved data
plot_data(f'data_{VERSION}_{YEAR}_{MONTH}.nc')
