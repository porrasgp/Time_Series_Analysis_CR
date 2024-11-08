import cdsapi

def download_data():
    dataset = "satellite-carbon-dioxide"
    client = cdsapi.Client()

    request = {
        'processing_level': 'level_2',
        'variable': 'mid_tropospheric_columns_of_atmospheric_carbon_dioxide',
        'sensor_and_algorithm': 'iasi_metop_b_nlis',
        'year': ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'],
        'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
        'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
        'version': '10.1'
    }

    client.retrieve(dataset, request).download()
    print("Downloaded IASI Metop-B data")