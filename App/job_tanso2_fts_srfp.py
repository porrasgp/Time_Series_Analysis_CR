import cdsapi

def download_data():
    dataset = "satellite-carbon-dioxide"
    client = cdsapi.Client()

    request = {
        'processing_level': 'level_2',
        'variable': 'mid_tropospheric_columns_of_atmospheric_carbon_dioxide',
        'sensor_and_algorithm': 'tanso2_fts_srfp',
        'year': ['2019', '2020', '2021', '2022'],
        'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
        'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
        'version': '2.0.0'
    }

    client.retrieve(dataset, request).download()
    print("Downloaded TANSO2-FTS data")
