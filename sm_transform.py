
def generar_lista(query, table):
    """
    Transforma la información obtenida de la API del Banco Central,
    que es un Data Frame, en una lista.
    """
    lista = []
    for ind in query.index:
        lista.append([str(ind)[:10], float(query.loc[ind, table])])
    return lista