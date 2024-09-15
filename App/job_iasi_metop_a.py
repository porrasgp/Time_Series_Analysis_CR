import cdsapi

# Define the dataset
dataset = "satellite-carbon-dioxide"

# Define the request parameters
request = {
    'processing_level': 'level_2',
    'variable': 'carbon_dioxide_column',  # Adjust the variable if needed
    'sensor_and_algorithm': 'iasi_metop_a_nlis',  # Ensure this matches the CDS dataset
    'year': ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021'],
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
    'version': '10_1'  # Select only one version
}

# Initialize the CDS API client
client = cdsapi.Client()

# Make the request and download the data
client.retrieve(dataset, request).download('job_iasi_metop_a.zip')  # Specify a filename
