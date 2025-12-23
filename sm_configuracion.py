direccion_credenciales_banco_central = 'credenciales_banco_central.txt'
direccion_credenciales_postgre = 'credenciales_postgre.txt'

tablas = ['ipc', 'fuerza_trabajo', 'desocupados']

series_banco_central = {tablas[0]: 'F074.IPC.IND.Z.EP23.C.M', tablas[1]: 'F049.FTR.PMT.INE9.01.M', tablas[2]: 'F049.CES.PMT.INE9.01.M'}

tipos_columna = ['TIMESTAMP', 'NUMERIC']
