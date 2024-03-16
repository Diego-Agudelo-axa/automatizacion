import pandas as pd

# Función para validar los datos, (typos y demas)
def validateData(df):

    print("Inicia proceso de validacion del dataframe.")

    df["saldo_total"] = df["saldo_total"].str.replace("=", "")
    df["dias_vencimiento"] = df["dias_vencimiento"].str.replace("=", "")
    df["origen_pedido"]=df["origen_pedido"].fillna("")

    df['fecha_creacion'] = pd.to_datetime(df['fecha_creacion'], format='%d/%m/%Y')
    df['fecha_vencimiento'] = pd.to_datetime(df['fecha_vencimiento'], format='%d/%m/%Y')

    return df
