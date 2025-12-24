import pandas as pd
from datetime import datetime

def leer_csv(data_dir):
    """
    Lee y retorna la información contenida en el CSV que se encuentra 
    en data_dir.

    Parámetros:
    data_dir (str): Dirección con la ubicación del archivo CSV del cual
        se quiere obtener la información.

    Retorna:
    data (pd.DataFrame): Datos con la información contenida en el
        archivo CSV.
    """
    data = pd.read_csv(data_dir, header=0, parse_dates=False, index_col=0,
                    sep=',')
    return data


def leer_txt(location):
    """
    Retorna una lista en la que sus elementos son las líneas del 
    archivo de texto que se indica
    """
    read_datos = open(location, 'r')
    datos = read_datos.readlines()
    read_datos.close()
    return datos

def transformar_fecha(text):
    """ Transforma el string con la fecha en un datetime"""
    ano = int(text[:4])
    mes = int(text[5:7])
    dia = int(text[8:])
    return datetime(ano, mes, dia)