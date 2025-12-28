from datetime import datetime

registro_txt = False

direccion_credenciales_banco_central = 'credenciales_banco_central.txt'
direccion_credenciales_postgre = 'credenciales_postgre.txt'

direccion_base_salario = 'sueldo_minimo.csv'

tablas_directas = ['sueldo_minimo', 'ipc', 'tasa_desempleo']
tablas_fechas_analogas = ['sueldo_minimo_ampliado', 'ipc_comprimido']
tablas_compuestas = ['sueldo_minimo_real_comprimido', 'sueldo_minimo_real']
tabla_manual = 'sueldo_minimo'

series_banco_central = {tablas_directas[1]: ['F074.IPC.IND.Z.EP23.C.M'], tablas_directas[2]: ['F049.DES.PMT.INE9.01.M', 'F049.FTR.PMT.INE9.01.M']}

tipo_columnas = {tablas_directas[0]: ['TIMESTAMP', 'INTEGER'], tablas_directas[1]: ['TIMESTAMP', 'NUMERIC'], tablas_directas[2]: ['TIMESTAMP', 'NUMERIC'], tablas_fechas_analogas[0]: ['TIMESTAMP', 'INTEGER'], tablas_fechas_analogas[1]: ['TIMESTAMP', 'NUMERIC']}
fecha_inicial = {tablas_directas[0]:  datetime(2009,7,1), tablas_directas[1]: datetime(2009,12,1), tablas_directas[2]:  datetime(2010,3,1), tablas_fechas_analogas[0]: datetime(2009,12,1), tablas_fechas_analogas[1]: datetime(2009,12,1)}
meses_desfase_informacion = {tablas_directas[0]: 0, tablas_directas[1]: 1, tablas_directas[2]:  2, tablas_fechas_analogas[0]: 1, tablas_fechas_analogas[1]: 1}