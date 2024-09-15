import cdsapi

# Definimos el dataset
dataset = "satellite-carbon-dioxide"

# Creamos el cliente de CDS
client = cdsapi.Client()

# Request para el sensor y algoritmo 'iasi_metop_a_nlis'
request_iasi_metop_a = {
    'processing_level': 'level_2',
    'variable': 'mid_tropospheric_columns_of_atmospheric_carbon_dioxide',
    'sensor_and_algorithm': 'iasi_metop_a_nlis',
    'year': ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021'],
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
    'version': '10.1'
}

# Descargamos los datos de IASI (Metop-A)
client.retrieve(dataset, request_iasi_metop_a).download()

# Request para el sensor y algoritmo 'iasi_metop_b_nlis'
request_iasi_metop_b = {
    'processing_level': 'level_2',
    'variable': 'mid_tropospheric_columns_of_atmospheric_carbon_dioxide',
    'sensor_and_algorithm': 'iasi_metop_b_nlis',
    'year': ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'],
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
    'version': '10.1'
}

# Descargamos los datos de IASI (Metop-B)
client.retrieve(dataset, request_iasi_metop_b).download()

# Request para el sensor y algoritmo 'iasi_metop_c_nlis'
request_iasi_metop_c = {
    'processing_level': 'level_2',
    'variable': 'mid_tropospheric_columns_of_atmospheric_carbon_dioxide',
    'sensor_and_algorithm': 'iasi_metop_c_nlis',
    'year': ['2019', '2020', '2021', '2022'],
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
    'version': '10.1'
}

# Descargamos los datos de IASI (Metop-C)
client.retrieve(dataset, request_iasi_metop_c).download()

# Request para el sensor y algoritmo 'airs_nlis'
request_airs_nlis = {
    'processing_level': 'level_2',
    'variable': 'mid_tropospheric_columns_of_atmospheric_carbon_dioxide',
    'sensor_and_algorithm': 'airs_nlis',
    'year': ['2003', '2004', '2005', '2006', '2007'],
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
    'version': '3.0'
}

# Descargamos los datos de AIRS
client.retrieve(dataset, request_airs_nlis).download()

# Request para el sensor y algoritmo 'tanso2_fts_srfp'
request_tanso2_fts_srfp = {
    'processing_level': 'level_2',
    'variable': 'mid_tropospheric_columns_of_atmospheric_carbon_dioxide',
    'sensor_and_algorithm': 'tanso2_fts_srfp',
    'year': ['2019', '2020', '2021', '2022'],
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
    'version': '2.0.0'
}

# Descargamos los datos de TANSO-FTS
client.retrieve(dataset, request_tanso2_fts_srfp).download()
