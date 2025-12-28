from datetime import datetime

from sm_load import Carga 
from sm_configuracion import tablas_directas

tiempo_inicial = datetime.now()

carga = Carga()

for tabla in tablas_directas:
    carga.seleccionar_tabla(tabla)
    hay_que_actualizar = carga.revisar_si_actualizar()
    if hay_que_actualizar:
        carga.actualizar(datetime.now())

carga.actualizar_tablas_compuestas()

carga.cerrar_conexion()  

tiempo_final = datetime.now() 

tiempo_ejecucion = tiempo_final - tiempo_inicial
segundos = tiempo_ejecucion.seconds

print(f"Se realizó la ejecución en {segundos} segundos.")