from sm_conexion_banco_central import ConexionBCentral
from sm_configuracion import direccion_base_salario, tabla_manual
from sm_funciones import leer_csv, transformar_fecha_csv


class Extraccion:
    
    def __init__(self):
        """Inicializa una instancia de la clase Extraccion."""
        self._conexion_banco_central = None
        self._conexion_activa = False
    
    def obtener_datos(self, table, from_date, until_date):
        """
        Dependiendo de la tabla que se busca actualizar, se obtiene, la
        información en basea una consulta 'manual' o 'conectada' (a la
        base del Banco Central).
        """
        if table == tabla_manual:
            consulta = self._obtener_consulta_manual(from_date, until_date)
        else:
            consulta = self._obtener_consulta_conectada(table, from_date, until_date)  
        return consulta
        
    def _obtener_consulta_conectada(self, table, from_date, until_date):  
        """
        En base a la tabla que se busca actualizar, y las fechas que
        determinan el periodo a buscar, se genera una consulta, a la 
        conexion_banco_central, para obtener los datos para luego
        retornarlos.
        """
        if not self._conexion_activa:
            self._inicializar_conexion()
        self._conexion_banco_central.consultar(table, from_date, until_date)
        consulta_bruta = self._conexion_banco_central.consulta
        return consulta_bruta

    def _inicializar_conexion(self):
        """
        Inicializa el atributo conexion_banco_central, asignándole una
        instancia de ConexionBCentral. Luego indica que el atributo
        conexion_activa es True.
        """
        self._conexion_banco_central = ConexionBCentral()
        self._conexion_activa = True
    
    def _obtener_consulta_manual(self, from_date, until_date):
        """
        Obtiene los datos de la 'consulta manual', que es para la tabla
        de Sueldos Mínimos, leyendo el archivo CSV en el que se 
        encuentra la información, para luego retornarla.
        """
        consulta_bruta = leer_csv(direccion_base_salario)
        consulta = self._acotar_consulta(consulta_bruta, from_date, until_date)
        return consulta
    
    @staticmethod
    def _acotar_consulta(query, from_date, until_date):
        """
        Selecciona solo la información necesaria, en este caso respecto
        del Sueldo Mínimo, considerando solo las fechas (que componen 
        los índices del DataFrame) que corresponden (entre los límites
        indicados).
        """
        consulta_acotada = query
        for indice in query.index:
            fecha_indice = transformar_fecha_csv(indice)
            if fecha_indice < from_date or fecha_indice > until_date:
                consulta_acotada = consulta_acotada.drop(indice)
        return consulta_acotada