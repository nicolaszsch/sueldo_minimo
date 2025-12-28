from sm_configuracion import tablas_directas, tablas_compuestas
from sm_funciones import registro_de_fecha

def transformar(table, data):
    if table == tablas_directas[0]:                      #Sueldo Mínimo
        datos = _generar_lista(table, data)
    elif table == tablas_directas[1]:                    #IPC
        datos = _generar_lista(table, data[0])
    elif table == tablas_directas[2]:                    #Tasa Desempleo 
        desocupados = _generar_lista(table, data[0])
        fuerza_trabajo = _generar_lista(table, data[1])
        datos = _generar_tabla_division(desocupados, fuerza_trabajo)
    elif table in tablas_compuestas:                    #Sueldo Mínimo Real
        sueldo = _lista_fecha_ajustada(data[0])
        ipc = _lista_fecha_ajustada(data[1])
        datos = _generar_tabla_division(sueldo, ipc)
    return datos


def _generar_lista(table, query):
    """
    Transforma la información obtenida de la API del Banco Central,
    que es un Data Frame, en una lista.
    """
    lista = []
    for indice in query.index:
        if table == "sueldo_minimo":
            lista.append([str(indice)[:10], int(query.loc[indice, table])])
        else:
            lista.append([str(indice)[:10], float(query.loc[indice, table])])
    return lista

def _lista_fecha_ajustada(information):
    """
    Transforma la información obtenida de la API del Banco Central,
    que es un Data Frame, en una lista.
    """
    lista = []
    for registro in information:
        lista.append([str(registro[0])[:10], registro[1]])
    return lista

def _generar_tabla_division(numerators, divisors):
    "Validar que las fechas calcen"
    tasas = []
    for i in range(len(divisors)):
        tasas.append([divisors[i][0], registro_de_fecha(divisors[i][0],numerators)[1]/divisors[i][1]])
    return tasas