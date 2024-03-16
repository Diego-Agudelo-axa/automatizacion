import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy  import create_engine, text
import logging
import datetime



# Creación del objeto de registro
logger = logging.getLogger()

# Configuración del nivel de registro
logger.setLevel(logging.INFO)

# Creación del manejador de archivo
handler = logging.FileHandler('registro_etl_stock_mes_corriente.log')

# Configuración del formato del registro
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Adición del manejador al objeto de registro
logger.addHandler(handler)

# Generación de un mensaje de registro
logger.info('Este es un mensaje de registro')
print('Inicio del proceso')
logger.info('Inicio del proceso')

# Función para parsear la tabla HTML
def parse_html_table(html):
   print('Inicia Proceso para parsear el HTML')
   logger.info('Inicia Proceso para Validar el dataframe')
   soup = BeautifulSoup(html, 'html.parser')
   table = soup.find('table')

   datos = pd.read_html(str(table))[0]
   df = pd.DataFrame(datos)
   
   df.columns = ['id_bodega', 'bodega', 'disponible', 'inv_valor', 'linea', 'fabricante', 'id_interno', 'cod_bar', 'forma_farmaceutica', 'item']
   df['created_at'] = datetime.datetime.now()
   df['updated_at'] = datetime.datetime.now()
   df['fecha_informe'] = datetime.datetime.now()
   df['costo_und'] = 0
   df = df.iloc[1:]
   #print(df['disponible'])
   return df

# Función para validar los datos
def validate_data(df):
   print('Inicia Proceso para Validar el dataframe')
   logger.info('Inicia Proceso para cargar df a la base de datos')
   nombre_columna = 'id_bodega'
   df = df.dropna(subset=[nombre_columna])  
   # Validación de los tipos de datos
   #print(df['fecha'])
   # Conversión de la columna 'fecha' a fecha
   df['fecha_informe'] = pd.to_datetime(df['fecha_informe'], format='%d/%m/%Y')
   
   
   # Formateo de la fecha a formato MySQL
   df['fecha_informe'] = df['fecha_informe'].apply(lambda x: x.strftime('%Y-%m-%d'))
   
   # Eliminación de los espacios en blanco al principio y al final de la cadena
   df['inv_valor'] = df['inv_valor'].str.strip()
   # Reemplazo de los caracteres o símbolos no válidos con espacios
   df['inv_valor'] = df['inv_valor'].str.replace('=', '')
   df['disponible'] = df['disponible'].str.replace('=', '')
   #df['valor'] = df['valor'].str.replace('.', ',')
   # Conversión de la columna 'valor' a float
   df['inv_valor'] = df['inv_valor'].astype(float)
   df['disponible'] = df['disponible'].astype(int)
   #print(df['valor'])
   
   condition = (df['inv_valor'] != 0) & (df['disponible'] != 0)
   df.loc[condition, 'costo_und'] = df['inv_valor'] / df['disponible']
   
   df['costo_und'] = df['costo_und'].round(2)
   
   
   # Validación de valores faltantes
   #df = df.dropna()
   print(df)
   return df

# Función para cargar los datos en la base de datos MySQL
def load_data_to_mysql(df):
   print('Inicia Proceso para cargar df a la base de datos')
   logger.info('Inicia Proceso para cargar df a la base de datos')
   host = '127.0.0.1'
   user = 'desarrollo'
   password = '4x4D3s4rr0ll0'
   database = 'bigdatadb'
   engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
   nombre_tabla = 'stock_mes_corriente'
   print('truncando la tabla...')
   logger.info('truncando la tabla...')
   # Ejecuta la sentencia SQL para truncar la tabla
   with engine.connect() as connection:
      query = text(f'TRUNCATE TABLE {nombre_tabla}')
      connection.execute(query)
   column_order = ['fecha_informe', 'id_bodega', 'bodega', 'fabricante', 'linea', 'cod_bar', 'forma_farmaceutica', 'item', 'disponible', 'inv_valor', 'costo_und', 'id_interno', 'updated_at']
   df = df[column_order]
   df.to_sql(nombre_tabla, engine, if_exists='append', index=False) 
   print('Proceso finalizado') 
   logger.info('Proceso finalizado')
   # Cierre de la conexión a la base de datos
   #engine.commit()


# URL del web query
url = "https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=91521&email=diego.jimenez@axa.com.co&role=3&cr=1661&hash=AAEJ7tMQlFkfycVyW3OZPREyNVxRicD3Z7cigr-B3UzkBaK77Og"
#url ="https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=91521&email=diego.jimenez@axa.com.co&role=3&cr=1687&hash=AAEJ7tMQBT1Jy0eooD06J6BIASbwFCxzFug0auFFL-j1kuAfnRU"
# Obtención de los datos del web query
response = requests.get(url)
print(response.status_code)

# Manejo de errores
try:
   # Parseo de la respuesta HTML
   soup = BeautifulSoup(response.text, 'html.parser')

   # Extracción de la tabla
   table = soup.find('table')

   # Creación del DataFrame
   df = parse_html_table(str(table))
    
   #print(df.columns)

   # Validación de los datos
   df = validate_data(df)
   #print(df)

   # Carga de los datos en la base de datos MySQL
   load_data_to_mysql(df)

except Exception as e:
   print("Se produjo un error: %s", e)
   logging.error("Se produjo un error: %s", e)