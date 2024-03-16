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
handler = logging.FileHandler('registro_etl_ventas_miles.log')

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
   df.columns = ['Fecha', 'Número de documento', 'Valor', 'Agrupador', 'Ubicación', 'Clase', 'Clase: Nombre', 'A QUIEN CAE LA VENTA?', 'Documento Cliente', 'Nombre Cliente', 'Departamento', 'Nombre establecimiento', 'Lista de precios', 'Tipo de transacción', 'direccion envio', 'Método de Pago']
   #df.columns = ['agrupador', 'ubicacion', 'clase', 'nombre_clase', 'quien_cae_venta', 'documento_cliente', 'nombre_cliente', 'departamento', 'nombre_establecimiento', 'lista_precios', 'tipo_transaccion', 'fecha', 'numero_documento','direccion_envio', 'metodo_pago', 'valor']
   #df['created_at'] = datetime.datetime.now()
   df['Updated at'] = datetime.datetime.now()
   df = df.iloc[1:]
   return df

# Función para validar los datos
def validate_data(df):
   print('Inicia Proceso para Validar el dataframe')
   logger.info('Inicia Proceso para cargar df a la base de datos')
   # Validación de los tipos de datos
   #print(df['fecha'])
   # Conversión de la columna 'fecha' a fecha
   df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
   # Formateo de la fecha a formato MySQL
   df['Fecha'] = df['Fecha'].apply(lambda x: x.strftime('%Y-%m-%d'))
   # Eliminación de los espacios en blanco al principio y al final de la cadena
   df['Valor'] = df['Valor'].str.strip()
   # Reemplazo de los caracteres o símbolos no válidos con espacios
   df['Valor'] = df['Valor'].str.replace('=', '')

   # Conversión de la columna 'valor' a float
   df['Valor'] = df['Valor'].astype(float)
   #print(df['valor'])
        

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
   nombre_tabla = "ventas_miles_mes_corriente"
   print('truncando la tabla...')
   logger.info('truncando la tabla...')
   # Ejecuta la sentencia SQL para truncar la tabla
   with engine.connect() as connection:
      query = text(f'TRUNCATE TABLE {nombre_tabla}')
      connection.execute(query)
   column_order = ['Agrupador', 'Ubicación', 'Clase', 'Clase: Nombre', 'A QUIEN CAE LA VENTA?', 'Documento Cliente', 'Nombre Cliente', 'Departamento', 'Nombre establecimiento', 'Lista de precios', 'Tipo de transacción', 'Fecha', 'Número de documento','direccion envio', 'Método de Pago', 'Valor', 'Updated at']
   df = df[column_order]
   df.to_sql(nombre_tabla, engine, if_exists='append', index=False) 
   print('Proceso finalizado') 
   logger.info('Proceso finalizado')
   # Cierre de la conexión a la base de datos
   #engine.commit()


# URL del web query
url = "https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=91521&email=diego.jimenez@axa.com.co&role=3&cr=1885&hash=AAEJ7tMQnpiYwxbMgSBV8bXRBxcQcSV2rBVwHBUt-owpCFbVcXQ"

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

   # Validación de los datos
   df = validate_data(df)

   # Carga de los datos en la base de datos MySQL
   load_data_to_mysql(df)

except Exception as e:
   print("Se produjo un error: %s", e)
   logging.error("Se produjo un error: %s", e)