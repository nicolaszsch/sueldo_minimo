from datetime import datetime
from dateutil.relativedelta import relativedelta

from sm_conexion_postgre import ConexionPostgre
from sm_extract import Extraccion
from sm_transform import transformar
from sm_configuracion import fecha_inicial, registro_txt, meses_desfase_informacion, tablas_fechas_analogas, tablas_compuestas, tablas_directas
from sm_funciones import existe_txt, obenter_datos_txt, expandir_segun_otra_serie, comprimir_segun_otra_serie

class Carga:

    def __init__(self):
        self._conexion_postgre = None
        self._extraccion = None
        self._extraccion_activa = False

        self._nombre_tabla = None
        self._fecha_ultima_actualizacion = None
        self._datos_sueldo = None

        self._inicializar_conexion()        

    def seleccionar_tabla(self, table):  
        """
        Activa la tabla indicada (definiendo el nombre y las columnas
        en la conexion_base_datos) y reinicia la 
        fecha_ultima_actualizacion.
        Se utiliza para cambiar la tabla a trabajar.
        """
        self._nombre_tabla = table
        if not registro_txt:
            self._conexion_postgre.activar_tabla(table)
        self._fecha_ultima_actualizacion = None


    def revisar_si_actualizar(self):
        """
        Indica si la tabla activa de la conexion_postgre se debe
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
        tabla = self._nombre_tabla
        hay_que_actualizar = True
        existe_tabla = self._revisar_existencia_tabla()
        if existe_tabla:
            ultima_actualizacion = self._obtener_fecha_ultima_actualizacion()  
            if ultima_actualizacion is None:
                self._fecha_ultima_actualizacion = fecha_inicial[tabla] + relativedelta(months = -1)
            else:
                if self._esta_actualizado(ultima_actualizacion + relativedelta(months = meses_desfase_informacion[tabla])):
                    hay_que_actualizar = False
                else:
                    self._fecha_ultima_actualizacion = ultima_actualizacion
        else:
            if not registro_txt:
                self._conexion_postgre.crear_tabla(tabla)
            self._fecha_ultima_actualizacion = fecha_inicial[tabla] + relativedelta(months = -1)
        return hay_que_actualizar   
    
    def actualizar(self, until_date):
        """
        Actualiza la tabla activa de la conexion_postgre.
        Para esto se define la fecha_desde y se utiliza la fecha hasta
        indicada para generar la consulta y extraer los nuevos datos, 
        los cuales se convierten en una lista para ser usados en la 
        actualización, al insertarlos en la tabla.
        """
        if not self._extraccion_activa:
            self._inicializar_extraccion()
        fecha_desde = self._fecha_ultima_actualizacion + relativedelta(months = +1)
        consulta = self._extraccion.obtener_datos(self._nombre_tabla, fecha_desde, until_date)
        datos = transformar(self._nombre_tabla, consulta)
        self._actualizar_base_datos(datos)
        self._gestionar_tablas_fechas_analogas(datos)

    def actualizar_tablas_compuestas(self):
        """
        Actualiza la tabla activa de la conexion_postgre.
        Para esto se define la fecha_desde y se utiliza la fecha hasta
        indicada para generar la consulta y extraer los nuevos datos, 
        los cuales se convierten en una lista para ser usados en la 
        actualización, al insertarlos en la tabla.
        """
        for comprimidas in [True, False]:
            tablas = self._definir_tablas_para_compuestas(comprimidas)
            if registro_txt:
                datos = self._generar_informacion_compuestas_txt(tablas)
                self._exportar_txt(tablas[0], datos, True, False)
            else:
                self._conexion_postgre.generar_tabla_compuesta(tablas)


    def cerrar_conexion(self):
        """Finaliza/cierra la conexion_base_datos"""
        if not registro_txt:
            self._conexion_postgre.cierre_final()


    def _inicializar_conexion(self):
        if not registro_txt:
            self._conexion_postgre = ConexionPostgre()

    def _inicializar_extraccion(self):
        self._extraccion = Extraccion()
        self._extraccion_activa = True

    def _revisar_existencia_tabla(self):  
        """
        El método determina la existencia de la tabla activa de la 
        conexion_postgre y retorna el resultado de la existencia de 
        ésta.
        """
        if registro_txt:
            existe_tabla = existe_txt(self._nombre_tabla+'.txt')
        else:
            existe_tabla = self._conexion_postgre.existe_tabla(self._nombre_tabla)
        return existe_tabla

    def _obtener_fecha_ultima_actualizacion(self):
        if registro_txt:
            datos_registro = obenter_datos_txt(self._nombre_tabla+'.txt')
            if(len(datos_registro)) == 0:
                ultima_fecha = None
            else:
                ultima_fecha = datos_registro[len(datos_registro)-1][0]
        else:
            ultima_fecha = self._conexion_postgre.obtener_ultima_fecha_actualizacion(self._nombre_tabla)
        return ultima_fecha

    def _actualizar_base_datos(self, data):
        if registro_txt:
            if self._fecha_ultima_actualizacion == fecha_inicial[self._nombre_tabla] + relativedelta(months = -1):
                con_encabezado = True
                anadir = False
            else:
                con_encabezado = False
                anadir = True
            self._exportar_txt(self._nombre_tabla, data, con_encabezado, anadir)
        else:
            self._conexion_postgre.insertar_datos(self._nombre_tabla, data)


    def _gestionar_tablas_fechas_analogas(self, data):
        self._actualizar_datos_sueldo(data)
        nuevas_tablas = self._generar_data_fechas_analogas(data)
        if nuevas_tablas is not None:
            for i in range(len(nuevas_tablas)):
                self.seleccionar_tabla(tablas_fechas_analogas[i])
                actualizar = self.revisar_si_actualizar()
                if actualizar:
                    self._actualizar_base_datos(nuevas_tablas[i])
    
    def _actualizar_datos_sueldo(self, data):
        if self._nombre_tabla == 'sueldo_minimo':
            self._datos_sueldo = data

    def _generar_data_fechas_analogas(self, data):
        if self._nombre_tabla == 'ipc':
            sueldo_minimo_ampliado = expandir_segun_otra_serie(self._datos_sueldo, data)
            ipc_comprimido = comprimir_segun_otra_serie(data, self._datos_sueldo)
            nuevas_tablas = [sueldo_minimo_ampliado, ipc_comprimido]
        else:
            nuevas_tablas = None
        return nuevas_tablas


    def _generar_informacion_compuestas_txt(self, tables):
        origin_data = self._obtener_tablas_origen_txt(tables)
        data = transformar(tables[0], origin_data)
        return data

    def _obtener_tablas_origen_txt(self, tables_name): 
        data = []
        for i in [1, 2]:
            data.append(obenter_datos_txt(tables_name[i]+'.txt'))
        return data


    @staticmethod
    def _esta_actualizado(camparison_date):
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
    def _exportar_txt(table, data, header, add):
        nombre_archivo = table + ".txt"
        largo = len(data)
        if add:
            archivo_text = open(nombre_archivo, 'a')
        else:
            archivo_text = open(nombre_archivo, 'w')
        if header:
            archivo_text.write("fecha")
            archivo_text.write(',' + table + "_mensual")
            archivo_text.write('\n')
        for i in range(largo):
            archivo_text.write(str(data[i][0])+","+str(data[i][1]))
            archivo_text.write('\n')
        archivo_text.close()
        
    @staticmethod
    def _definir_tablas_para_compuestas(compressed):
        if compressed:
            tabla = tablas_compuestas[0]
            tabla_1 = tablas_directas[0]
            tabla_2 = tablas_fechas_analogas[1]
        else:
            tabla = tablas_compuestas[1]
            tabla_1 = tablas_fechas_analogas[0]
            tabla_2 = tablas_directas[1]
        nombre_tablas = [tabla, tabla_1, tabla_2]
        return nombre_tablas