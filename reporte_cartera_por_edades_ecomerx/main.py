import requests
import logging
import sys

from extractTable import extractTable
from validateData import validateData
from transformData import transformData

from db import uploadDataToMysql

try:
   print("Inicio proceso")
   print("Inicia descarga de reporte.")

   url = "https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=118483&email=carlos.leon@distriaxa.co&role=3&cr=1917&hash=AAEJ7tMQAE7QJrg7QKPUBIXlr6ATONXNDPxJAVbB8Z2XCnmuzBA"

   response = requests.get(url)  # Obtención de los datos del web query.
   df = extractTable(str(response.text))  # obtencion de tabla dataframe.

   df = validateData(df)  # Validación de los datos.

   # print(df)
   # sys.exit()
   df = transformData(df)  # Se operan datos.

   uploadDataToMysql(df)  # Carga de los datos en la base de datos MySQL.

   print("Fin proceso.")
except Exception as e:
    print("Se produjo un error: %s", e)
    logging.error("Se produjo un error: %s", e)
