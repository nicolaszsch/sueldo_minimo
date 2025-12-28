import os
from datetime import datetime
import pandas as pd


def leer_csv(file_name):
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
    data = pd.read_csv(file_name, header=0, parse_dates=False, index_col=0,
                    sep=',')
    return data


def transformar_fecha_csv(date_text):
    """ Transforma el string con la fecha en un datetime"""
    ano = int(date_text[:4])
    mes = int(date_text[5:7])
    dia = int(date_text[8:])
    return datetime(ano, mes, dia)


def leer_txt(file_name):
    """
    Retorna una lista en la que sus elementos son las líneas del 
    archivo de texto que se indica
    """
    read_datos = open(file_name, 'r')
    datos = read_datos.readlines()
    read_datos.close()
    return datos


def obenter_datos_txt(file_name):
    filas = leer_txt(file_name)
    datos = []
    fila_headers = True
    for fila in filas:
        if fila_headers:
            fila_headers = False
        else:
            datos.append(_separar_fila_txt(fila))
    return datos


def _separar_fila_txt(row):
    ano = int(row[:4])
    mes = int(row[5:7])
    dia = int(row[8:10])
    fecha = datetime(ano, mes, dia)
    valor_texto = row[11:]
    if valor_texto.find(".") == -1:
        valor = int(valor_texto) 
    else:
        valor = float(valor_texto)
    return [fecha, valor]


def existe_txt(file_name):
    existe = False
    if os.path.exists(file_name):
        existe = True
    return existe


def registro_de_fecha(date, information):
    registro_identificado = None
    for registro in information:    
        if date == registro[0]:
            registro_identificado = registro
    return registro_identificado


def expandir_segun_otra_serie(original_information, comparison_information):
    fechas_originales = []
    for registro in original_information:
        fechas_originales.append(registro[0])

    nueva_informacion = []
    for i in range(len(comparison_information)):
        if comparison_information[i][0] in fechas_originales:
            nueva_informacion.append(registro_de_fecha(comparison_information[i][0], original_information))
        else:
            if i == 0:
                nueva_informacion.append([comparison_information[i][0], original_information[0][1]])
            else:
                nueva_informacion.append([comparison_information[i][0], nueva_informacion[i-1][1]])
    return nueva_informacion
        

def comprimir_segun_otra_serie(original_information, comparison_information):
    fechas_comparacion = []
    for registro in comparison_information:
        fechas_comparacion.append(registro[0])

    nueva_informacion = []
    for registro in original_information:
        if registro[0] in fechas_comparacion:
            nueva_informacion.append(registro)
    return nueva_informacion