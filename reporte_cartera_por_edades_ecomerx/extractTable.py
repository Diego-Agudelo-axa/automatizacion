import pandas as pd
from bs4 import BeautifulSoup


# Funcion para dar formato tabla html.
def extractTable(html):
    print("Inicia Proceso para extraer tabla del HTML")
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    datos = pd.read_html(str(table))[0]
    df = pd.DataFrame(datos)

    df.columns = [
        "fecha_creacion",
        "fecha_vencimiento",
        "dias_vencimiento",
        "documento",
        "saldo_total",
        "categoria",
        "clase_nombre",
        "location_name",
        "origen_pedido",
        "cuenta",
        "nombre_cliente",
        "referencia_pago",
        "metodo_pago",
        "tipo_transaccion",
        "cliente_trabajo",
        "b"
    ]

    df = df.iloc[1:]
    return df
