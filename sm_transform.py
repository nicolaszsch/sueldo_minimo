from sm_configuracion import tablas, tabla_manual

def transformar(table, data):
    if table == tablas[0]:                      #Sueldo Mínimo
        datos = generar_lista(table, data)
    if table == tablas[1]:                      #IPC
        datos = generar_lista(table, data[0])
    elif table == tablas[2]:                    #Tasa Desempleo 
        desocupados = generar_lista(table, data[0])
        fuerza_trabajo = generar_lista(table, data[1])
        datos = generar_tasa_desempleo_beta(desocupados, fuerza_trabajo)
    return datos


def generar_lista(table, query):
    """
    Transforma la información obtenida de la API del Banco Central,
    que es un Data Frame, en una lista.
    """
    lista = []
    for indice in query.index:
        lista.append([str(indice)[:10], float(query.loc[indice, table])])
    return lista

def generar_tasa_desempleo_beta(unemployeds, work_force):
    "Validar que las fechas calcen"
    tasas = []
    for i in range(len(unemployeds)):
        tasas.append([unemployeds[i][0], unemployeds[i][1]/work_force[i][1]])
    return tasas