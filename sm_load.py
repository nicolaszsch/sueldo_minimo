import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from sm_conexion_postgre import ConexionPostgre
from sm_extract import Extraccion
from sm_transform import transformar
from sm_configuracion import fecha_inicial, tabla_manual, registro_txt, meses_desfase_informacion
from sm_funciones import existe_txt, obenter_datos_txt

class Carga:

    def __init__(self):
        self.__conexion_postgre = None
        self.__extraccion = None
        self.__extraccion_activa = False
        self.__nombre_tabla = None
        self.__fecha_ultima_actualizacion = None

        self.__inicializar_conexion()        

    def seleccionar_tabla(self, table):  
        """
        Activa la tabla indicada (definiendo el nombre y las columnas
        en la conexion_base_datos) y reinicia la 
        fecha_ultima_actualizacion.
        Se utiliza para cambiar la tabla a trabajar.
        """
        if not self.__extraccion_activa:
            self.__inicializar_extraccion()
        self.__nombre_tabla = table
        self.__conexion_postgre.activar_tabla(table)
        self.__fecha_ultima_actualizacion = None
        self.__datos_txt = None

    def revisar_si_actualizar(self):
        """
        Indica si la tabla activa de la conexion__postgre se debe
        actualizar o no y retorna el resultado.
        Para eso revisa si la tabla existe, si no, se crea la tabla y 
        se indica que debe agregar todos los datos (determinando la 
        "fecha desde" de la cosulta a realizar como la primera fecha
        con registro). Si la tabla existe, se busca la fecha del último
        registro, y si éste es nulo, también se deben agregar todos los
        datos, pero si en cambio, existe una última fecha, éste se
        compara con la fecha actual para determinar si se debe 
        actualizar la tabla o ya estpa actualizada.
        """
        tabla = self.__nombre_tabla
        hay_que_actualizar = True
        existe_tabla = self.__revisar_existencia_tabla()
        if existe_tabla:
            ultima_actualizacion = self.__obtener_fecha_ultima_actualizacion()
            if ultima_actualizacion is None:
                self.__fecha_ultima_actualizacion = fecha_inicial[tabla] + relativedelta(months = -1)
            else:
                if self.__esta_actualizado(ultima_actualizacion + relativedelta(months = meses_desfase_informacion[tabla])):
                    hay_que_actualizar = False
                else:
                    self.__fecha_ultima_actualizacion = ultima_actualizacion
        else:
            if not registro_txt:
                self.__conexion_postgre.crear_tabla(tabla)
            self.__fecha_ultima_actualizacion = fecha_inicial[tabla] + relativedelta(months = -1)
        return hay_que_actualizar   
    
    def actualizar(self, until_date):
        """
        Actualiza la tabla activa de la conexion_postgre.
        Para esto se define la fecha_desde y se utiliza la fecha hasta
        indicada para generar la consulta y extraer los nuevos datos, 
        los cuales se convierten en una lista para ser usados en la 
        actualización, al insertarlos en la tabla.
        """
        if not self.__extraccion_activa:
            self.__inicializar_extraccion()
        fecha_desde = self.__fecha_ultima_actualizacion + relativedelta(months = +1)
        consulta = self.__extraccion.obtener_datos(self.__nombre_tabla, fecha_desde, until_date)
        datos = transformar(self.__nombre_tabla, consulta)
        self.__actualizar_base_datos(datos)


    def cerrar_conexion(self):
        """Finaliza/cierra la conexion_base_datos"""
        self.__conexion_postgre.cierre_final()

    """
    def exportar_excel(self, table, from_date, until_date):
        nombre_archivo = table + ".xlsx"
        consulta_bruta = self.__extraccion.obtener_consulta(table, from_date, until_date)
        consulta_bruta.index.name = "fecha"
        print(f"type: {type(consulta_bruta)}")
        print(consulta_bruta)
        writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
        consulta_bruta.to_excel(writer, sheet_name='Hoja1', index=True)
        writer.close()
        #consulta_bruta.to_excel(nombre_archivo, index = True)
    """

    def __inicializar_conexion(self):
        if not registro_txt:
            self.__conexion_postgre = ConexionPostgre()

    def __revisar_existencia_tabla(self):  
        """
        El método determina la existencia de la tabla activa de la 
        conexion_postgre y retorna el resultado de la existencia de 
        ésta.
        """
        if registro_txt:
            existe_tabla = existe_txt(self.__nombre_tabla+'.txt')
        else:
            existe_tabla = self.__conexion_postgre.existe_tabla(self.__nombre_tabla)
        return existe_tabla

    def __obtener_fecha_ultima_actualizacion(self):
        if registro_txt:
            datos_registro = obenter_datos_txt(self.__nombre_tabla+".txt")
            ultima_fecha = datos_registro[len(datos_registro)-1][0]
        else:
            ultima_fecha = self.__conexion_postgre.obtener_ultima_fecha_actualizacion(self.__nombre_tabla)
        return ultima_fecha

    def __actualizar_base_datos(self, data):
        if registro_txt:
            self.__exportar_txt(self.__nombre_tabla, data)
        else:
            self.__conexion_postgre.insertar_datos(self.__nombre_tabla, data)

    def __inicializar_extraccion(self):
        self.__extraccion = Extraccion()
        self.__extraccion_activa = True

    @staticmethod
    def __esta_actualizado(camparison_date):
        """
        En base a la fecha de comparación indicada, que hace 
        referencia al siguiente periodo que se debe registrar, se
        revisa respecto a la fecha actual, si se debe actualizar 
        la información y retorna el resultado que indica si los
        datos están actualizados.
        """
        actualizado = True  
        
        fecha_actual = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if fecha_actual > camparison_date:
            actualizado = False
        return actualizado    

    @staticmethod 
    def __exportar_txt(table, data):
        nombre_archivo = table + ".txt"
        largo = len(data)
        print(f"largo: {largo}")
        archivo_text = open(nombre_archivo, 'w')
        archivo_text.write("fecha")
        archivo_text.write(',' + table + "_mensual")
        archivo_text.write('\n')
        for i in range(largo):
            archivo_text.write(str(data[i][0])+","+str(data[i][1]))
            archivo_text.write('\n')
        archivo_text.close()
    