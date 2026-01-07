import numpy as np
import bcchapi

from sm_configuracion import direccion_credenciales_banco_central, series_banco_central

class ConexionBCentral():

    def __init__(self):
        """Inicializa una instancia de la clase ConexionBCentral."""
        self._bc_siete = bcchapi.Siete(file = direccion_credenciales_banco_central)
        self.__consulta = None

    def consultar(self, tabla, start_date, end_date): 
        """
        Genera la/s consulta/s a la API del Banco Central, como 
        DataFrame, para obtener los datos para la tabla indicada, y
        para el periodo comprendido según las fechas indicadas. Luego
        de obtener la información, la asocia al atributo consulta.
        """
        consultas = []
        for serie in series_banco_central[tabla]:
            head = tabla
            df_consulta = self._bc_siete.cuadro(
            series = serie,
            nombres = [head],
            desde = start_date,
            hasta = end_date,
            observado = {head:np.mean}
            )
            consultas.append(df_consulta)
        self.__consulta = consultas

    @property
    def consulta(self):
        """Método que retorna el atributo consulta"""
        return self.__consulta