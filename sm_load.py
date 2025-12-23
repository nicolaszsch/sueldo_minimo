from datetime import datetime
from dateutil.relativedelta import relativedelta

from sm_conexion_postgre import ConexionPostgre
from sm_extract import Extraccion
from sm_transform import generar_lista

class Carga:

    def __init__(self):
        self.__conexion_base_datos = ConexionPostgre()
        self.__extraccion = Extraccion()
        self.__fecha_ultima_actualizacion = None

    def seleccionar_tabla(self, table):  
        self.__conexion_base_datos.activar_tabla(table)
        self.__fecha_ultima_actualizacion = None

    def revisar_si_actualizar(self): 
        hay_que_actualizar = True
        existe_tabla = self.__revisar_existencia_tabla()
        if existe_tabla:
            ultima_actualizacion = self.__conexion_base_datos.obtener_ultima_fecha_actualizacion()
            if ultima_actualizacion is None:
                self.__fecha_ultima_actualizacion = datetime(2009,11,1)
            else:
                fecha_comparacion = ultima_actualizacion + relativedelta(months = +1)
                if self.__esta_actualizado(fecha_comparacion):
                    hay_que_actualizar = False
                else:
                    self.__fecha_ultima_actualizacion = ultima_actualizacion
        else:
            self.__conexion_base_datos.crear_tabla()
            self.__fecha_ultima_actualizacion = datetime(2009,11,1)
        return hay_que_actualizar   

    def actualizar(self, until_date):
        fecha_desde = self.__fecha_ultima_actualizacion + relativedelta(months = +1)
        tabla = self.__conexion_base_datos.nombre_tabla
        consulta_bruta = self.__extraccion.obtener_consulta(tabla, fecha_desde, until_date)        
        consulta = generar_lista(consulta_bruta, tabla)
        self.__actualizar_base_datos(consulta)

    def __actualizar_base_datos(self, query):
        self.__conexion_base_datos.insertar_datos(query)

    def cerrar_conexion(self):
        self.__conexion_base_datos.cierre_final()

    def __revisar_existencia_tabla(self): 
        existe_tabla = self.__conexion_base_datos.existe_tabla()
        return existe_tabla

    def __esta_actualizado(self, camparation_date):
            actualizado = True  
            
            fecha_actual = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

            if fecha_actual > camparation_date:
                actualizado = False
            return actualizado    