import psycopg2
from psycopg2.extras import execute_values

from sm_configuracion import direccion_credenciales_postgre, tipo_columnas
from sm_funciones import leer_txt

class ConexionPostgre:

    def __init__(self):
        self._conexion_base_datos = None
        self._columnas = None
        self._tipos = None
        
        self._inicializa_conexion()

    def activar_tabla(self, table_name):
        """
        Activa la tabla indicada, que con la que se trabajará, es decir,
        define sus atributos. Le asigna el nombre indicado al atributo
        nombre_tabla, y genera el nombre las columnas que se utilizarán.
        Como la estructura de las tablas es la mimsa, los tipos de 
        datos de cada columna quedan definidos al ejecutarse el 
        constructor. 
        """
        self._columnas = ['fecha', table_name + '_mensual']
        self._tipos = tipo_columnas[table_name]

    def crear_tabla(self, table):
        """
        Genera y ejecuta la sentencia para crear la tabla activa en 
        PostgreSQL.
        """
        cursor = self._conexion_base_datos.cursor()
        sentencia = self._generar_sentencia_crear(table)
        cursor.execute(sentencia)
        self._conexion_base_datos.commit()
        cursor.close()

    def insertar_datos(self, table, data): 
        """
        Genera y ejecuta la sentencia para insertar los datos indicados
        en la tabla activa en la base de PostgreSQL.
        """
        cursor = self._conexion_base_datos.cursor()
        sentencia = self._generar_sentencia_insertar(table)
        execute_values(cursor, sentencia, data)
        self._conexion_base_datos.commit()
        cursor.close()

    def generar_tabla_compuesta(self, tables):
        """
        Genera y ejecuta la sentencia para insertar los datos indicados
        en la tabla activa en la base de PostgreSQL.
        """
        cursor = self._conexion_base_datos.cursor()
        if self.existe_tabla(tables[0]):
            self._eliminar_tabla(tables[0])
        sentencia = self._generar_sentencia_tablas_compuestas(tables)
        cursor.execute(sentencia)
        self._conexion_base_datos.commit()
        cursor.close()

    def existe_tabla(self, table):  
        """
        Indica si la tabla activa existe en la base de PostgreSQL. Para
        esto, primero genera y ejecuta la sentencia que indica la
        existencia de la tabla, y luego retorna respuesta.
        """
        cursor = self._conexion_base_datos.cursor()
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = '{table}')")
        existe = cursor.fetchone()[0]
        self._conexion_base_datos.commit()
        cursor.close()
        return existe

    def obtener_ultima_fecha_actualizacion(self, table):
        """
        Determina cual es la última fecha de la cual se tiene registro
        en la tabla activa.
        """
        cursor = self._conexion_base_datos.cursor()
        cursor.execute(f"SELECT MAX(\"fecha\") FROM {table}")
        try:
            ultima_fecha = cursor.fetchone()[0]
        except:
            ultima_fecha = None
        self._conexion_base_datos.commit()
        cursor.close()
        return ultima_fecha

    def cierre_final(self):
        """Cierra la conexión establecida con PostgreSQL."""
        self._conexion_base_datos.close()
      
    def _inicializa_conexion(self):
        """
        Genera la conexión con la base de datos indicada en PostgreSQL.
        Para esto, primero se obtiene los datos para la conexion,
        credenciales y la base a conectar, contenidos en archivo de
        texto, los que se utilizan cuando, luego, se establece la
        conexión
        
        """
        credenciales = leer_txt(direccion_credenciales_postgre)
        conexion = psycopg2.connect(
            host = credenciales[0][:-1], 
            database = credenciales[1][:-1],
            user = credenciales[2][:-1],
            password = credenciales[3],
            )   
        self._conexion_base_datos = conexion
 
    def _generar_sentencia_crear(self, table): 
        """
        Genera la sentencia SQL que permite crear la tabla según la 
        información de la tabla activa. 
        """  
        columnas = self._columnas
        tipos = self._tipos
        columnas_sql = ", ".join(f"{columnas[i]} {(tipos[i])}" for i in range(len(columnas)))
        sentencia_crear = f"CREATE TABLE IF NOT EXISTS {table} ({columnas_sql});"
        return sentencia_crear

    def _generar_sentencia_insertar(self, table):
        """
        Genera la sentencia SQL que permite insertar datos a la tabla
        activa, según la infoprmación de ésta.
        """  
        columnas = self._columnas
        sentencia_insertar = f"INSERT INTO {table} ({','.join(f"{columna}" for columna in columnas)}) VALUES %s"
        return sentencia_insertar

    def _generar_sentencia_tablas_compuestas(self, tables):
        """
        Genera la sentencia SQL que permite insertar datos a la tabla
        activa, según la infoprmación de ésta.
        """  
        "Hola hola"
        columnas = self._columnas
        sentencia_insertar = (f'SELECT ip.fecha AS "fecha", sm.{tables[1]}_mensual/ip.{tables[2]}_mensual AS {tables[0]}_mensual '
                                f'INTO {tables[0]} '
                                f'FROM {tables[1]} AS sm, {tables[2]} AS ip '
                                f'WHERE ip.fecha = sm.fecha')
        return sentencia_insertar

    def _eliminar_tabla(self, table):
        cursor = self._conexion_base_datos.cursor()
        sentencia = f"DROP TABLE {table}"
        cursor.execute(sentencia)
        self._conexion_base_datos.commit()
        cursor.close()