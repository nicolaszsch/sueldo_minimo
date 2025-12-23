from datetime import datetime
import pandas as pd
import psycopg2 # Ver si puedo dejrla en una línea cpn la de abajo
from psycopg2.extras import execute_values

from sm_configuracion import direccion_credenciales_postgre, tipos_columna
from sm_funciones import leer_txt

class ConexionPostgre:

    def __init__(self):
        self.__conexion_base_datos = None
        self.__nombre_tabla = None
        self.__columnas = None
        self.__tipos = tipos_columna

        self.__inicializa_conexion()


    def activar_tabla(self, table_name):
        """
        Activa la tabla indicada, que con la que se trabajará, es decir,
        define sus atributos. Le asigna el nombre indicado al atributo
        nombre_tabla, y genera el nombre las columnas que se utilizarán.
        Como la estructura de las tablas es la mimsa, los tipos de 
        datos de cada columna quedan definidos al ejecutarse el 
        constructor. 
        """
        self.__nombre_tabla = table_name
        self.__columnas = ['fecha', table_name + '_mensual']


    def crear_tabla(self):
        """
        Genera y ejecuta la sentencia para crear la tabla activa en PostgreSQL.
        """
        cursor = self.__conexion_base_datos.cursor()
        sentencia = self.__generar_sentencia_crear(self.__nombre_tabla, self.__columnas, self.__tipos)
        cursor.execute(sentencia)
        self.__conexion_base_datos.commit()
        cursor.close()

    def insertar_datos(self, data): 
        """
        Genera y ejecuta la sentencia para insertar los datos indicados
        en la tabla activa en la base de PostgreSQL.
        """
        cursor = self.__conexion_base_datos.cursor()
        sentencia = self.__generar_sentencia_insertar(self.__nombre_tabla, self.__columnas)
        execute_values(cursor, sentencia, data)
        self.__conexion_base_datos.commit()
        cursor.close()

    def existe_tabla(self):  
        """
        Indica si la tabla activa existe en la base de PostgreSQL. Para
        esto, primero genera y ejecuta la sentencia que indica la
        existencia de la tabla, y luego retorna respuesta.
        """
        cursor = self.__conexion_base_datos.cursor()
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = '{self.__nombre_tabla}')")
        existe = cursor.fetchone()[0]
        return existe

    def obtener_ultima_fecha_actualizacion(self):
        """
        Determina cual es la última fecha de la cual se tiene registro
        en la tabla activa.
        """
        cursor = self.__conexion_base_datos.cursor()
        cursor.execute(f"SELECT MAX(\"fecha\") FROM {self.__nombre_tabla}")
        try:
            ultima_fecha = cursor.fetchone()[0]
        except:
            ultima_fecha = None
        self.__conexion_base_datos.commit()
        cursor.close()
        return ultima_fecha

    def cierre_final(self):
        """Cierra la conexión establecida con PostgreSQL."""
        self.__conexion_base_datos.close()

    def __inicializa_conexion(self):
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
        self.__conexion_base_datos = conexion

    def __generar_sentencia_crear(table_name, columns, types): 
        """
        Genera la sentencia SQL que permite crear la tabla según la 
        información de la tabla activa. 
        """  
        columnas_sql = ", ".join(f"{columns[i]} {(types[i])}" for i in range(len(columns)))
        sentencia_crear = f"CREATE TABLE IF NOT EXISTS {table_name} ({columnas_sql});"
        return sentencia_crear

    @staticmethod 
    def __generar_sentencia_insertar(table_name, columns):
        """
        Genera la sentencia SQL que permite insertar datos a la tabla
        activa, según la infoprmación de ésta.
        """  
        sentencia_insertar = f"INSERT INTO {table_name} ({','.join(f"{column}" for column in columns)}) VALUES %s"
        return sentencia_insertar

    @property
    def nombre_tabla(self):
        return self.__nombre_tabla