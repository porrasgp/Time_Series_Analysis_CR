import requests
from bs4 import BeautifulSoup
import pandas as pd

# Función para extraer una tabla de una página
def extract_table(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table_element = soup.find('table', attrs={'width': '84%', 'class': 'table mb-0'})
    if table_element:
        df = pd.read_html(str(table_element))[0]
        return df
    else:
        return None

# Función para extraer tablas de múltiples páginas y guardar cada DataFrame con el nombre de las subastas
def extract_tables_multiple_pages(start_page, end_page, subastas):
    for i in range(start_page, end_page + 1):
        url = f"https://aurora-applnx.com/aurora_clientes/subastas/preciosViewHistorico.php?c={i}"
        df = extract_table(url)
        if df is not None:
            if i <= len(subastas):
                df_filename = f"C:/Users/Admin/Desktop/PrecioGanadoBot/data/{subastas[i-1]}.csv"
                df.to_csv(df_filename, index=False)
                print(f"Tabla {i} ({subastas[i-1]}) guardada en {df_filename}")
            else:
                print(f"No se encontró el nombre de la subasta correspondiente a la tabla {i}.")
        else:
            print(f"No se encontró la tabla en la página {i}.")

# Rango de páginas a extraer (ejemplo: de la página 1 a la página 5)
start_page = 1
end_page = 7

# Lista de nombres de las subastas
subastas = ['Sancarleña', 'Barranca', 'Nicoya', 'Maleco', 'Upala', 'Limonal', 'Parrita']

# Llamar a la función para extraer las tablas de múltiples páginas y guardar cada DataFrame con el nombre de las subastas
extract_tables_multiple_pages(start_page, end_page, subastas)
