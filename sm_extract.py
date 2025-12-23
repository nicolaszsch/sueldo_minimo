from sm_conexion_banco_central import ConexionBCentral

class Extraccion:
    
    def __init__(self):
        self.__conexion_origen_datos = ConexionBCentral()

    def obtener_consulta(self, table, from_date, until_date):
        self.__conexion_origen_datos.consultar(table, from_date, until_date)
        consulta_bruta = self.__conexion_origen_datos.consulta
        return consulta_bruta