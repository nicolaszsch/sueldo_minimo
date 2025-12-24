from datetime import datetime

registro_txt = False

direccion_credenciales_banco_central = 'credenciales_banco_central.txt'
direccion_credenciales_postgre = 'credenciales_postgre.txt'

direccion_base_salario = "sueldo_minimo.csv"

tablas = ['sueldo_minimo', 'ipc', 'tasa_desempleo']
tabla_manual = "sueldo_minimo"

series_banco_central = {tablas[1]: ['F074.IPC.IND.Z.EP23.C.M'], tablas[2]: ['F049.DES.PMT.INE9.01.M', 'F049.FTR.PMT.INE9.01.M']}
tipos_columna = {tablas[0]: ['TIMESTAMP', 'INTEGER'], tablas[1]: ['TIMESTAMP', 'NUMERIC'], tablas[2]: ['TIMESTAMP', 'NUMERIC']}
fecha_inicial = {tablas[0]:  datetime(2009,7,1), tablas[1]: datetime(2009,12,1), tablas[2]:  datetime(2010,3,1)}
meses_desfase_informacion = {tablas[0]: 0, tablas[1]: 1, tablas[2]:  2}