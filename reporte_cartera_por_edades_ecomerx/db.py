from sqlalchemy import create_engine, text

host = "127.0.0.1"
user = "desarrollo"
password = "4x4D3s4rr0ll0"
database = "bigdatadb"
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")


# Función para cargar los datos en la base de datos MySQL
def uploadDataToMysql(df):
    print("Inicia proceso para cargar df a la base de datos")
    truncateTable()
    column_order = [
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
        
    ]
    df = df[column_order]
    df.to_sql(
        "tb_ecomerx_reporte_de_cartera_por_edades_netsuit_fin",
        engine,
        if_exists="append",
        index=False,
    )
    print("Carga finalizada.")


def truncateTable():
    print("Truncando tabla")
    table = "tb_ecomerx_reporte_de_cartera_por_edades_netsuit_fin"
    with engine.connect() as connection:
        query = text(f"TRUNCATE TABLE {table}")
        connection.execute(query)
    print("Tabla truncada")