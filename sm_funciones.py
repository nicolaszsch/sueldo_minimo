def leer_txt(location):
    """
    Retorna una lista en la que sus elementos son las líneas del 
    archivo de texto que se indica
    """
    read_datos = open(location, 'r')
    datos = read_datos.readlines()
    read_datos.close()
    return datos