import bcchapi
import numpy as np
import pandas as pd

from sm_configuracion import direccion_credenciales_banco_central, series_banco_central

class ConexionBCentral():

    def __init__(self):
        self._bc_siete = bcchapi.Siete(file = direccion_credenciales_banco_central)
        self.__consulta = None

    def consultar(self, tabla, start_date, end_date): 
        """
        Genera la conslta para obtener los datos para la tabla indicada,
        y para el periodo comprendido según las fechas indicadas.
        Luego de obtener la información, TRANSFORMA los datos
        """
        head = tabla
        df_consulta = self._bc_siete.cuadro(
        series = series_banco_central[tabla],
        nombres = [head],
        desde = start_date,
        hasta = end_date,
        observado = {head:np.mean} 
        )
        self.__consulta = df_consulta.dropna()

    @property
    def consulta(self):
        """ Método que retorna el atributo consulta"""
        return self.__consulta
