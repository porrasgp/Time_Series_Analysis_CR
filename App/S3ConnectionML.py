import os
import zipfile
import pandas as pd
import netCDF4 as nc

def extract_zip(zip_path, extract_to):
    """Extract all .nc files from a .zip archive."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    return [os.path.join(extract_to, file) for file in zip_ref.namelist() if file.endswith('.nc')]

def process_file(file_path):
    """Process NetCDF file and convert to pandas DataFrame."""
    # Open the NetCDF file
    dataset = nc.Dataset(file_path)
    
    # Assuming the variable names are consistent across files, update if needed
    variables = [var for var in dataset.variables if var not in ['time']]
    data_frames = []
    
    for variable in variables:
        data = dataset.variables[variable][:]
        time = dataset.variables['time'][:]
        
        # Convert to DataFrame (Example for a single variable)
        df = pd.DataFrame(data, columns=[variable])
        df['time'] = pd.to_datetime(time, unit='D', origin='1900-01-01')
        
        data_frames.append(df)
    
    return pd.concat(data_frames, axis=1)

def process_zip(zip_path, year, variable):
    """Process a .zip file containing .nc files."""
    temp_dir = 'temp_nc_files'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    nc_files = extract_zip(zip_path, temp_dir)
    
    for file_path in nc_files:
        df = process_file(file_path)
        print(f"Descriptive statistics for file {os.path.basename(file_path)}:")
        print(df.describe())
        print("\n")
    
    # Clean up temp files
    for file in nc_files:
        os.remove(file)
    os.rmdir(temp_dir)

def main():
    base_directory = '/path/to/your/zip/files'  # Update with the path to your zip files
    
    for zip_file in os.listdir(base_directory):
        if zip_file.endswith('.zip'):
            zip_path = os.path.join(base_directory, zip_file)
            
            # Extract year and variable from the file name
            parts = zip_file.split('_')
            year = parts[3].split('.')[0]
            variable = '_'.join(parts[:-1])
            
            # Process the zip file
            process_zip(zip_path, year, variable)

if __name__ == "__main__":
    main()
