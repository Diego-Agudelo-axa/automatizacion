# Función transformar datos (calculos)
def transformData(df):

    print("Inicia proceso de validacion del dataframe.")

    if  not len(df["origen_pedido"])==0:
        df["origen_pedido"]=df["clase_nombre"]
   
    return df
