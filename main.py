from fastapi import FastAPI
import pyodbc
from datetime import date
import datetime
import time

ahora = datetime.datetime.now()
formatnow = ahora.strftime('%Y-%m-%d')



app = FastAPI()

def create_server_connection(server, bd, user, pswd):
    conexion = None
    #stringcon = "'DRIVER={ODBC Driver 17 for SQL server};SERVER="+server+";DATABASE="+bd+";UID="+user+";PWD="+pswd
    #print(stringcon)
    try:
        conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server};SERVER='+server+';DATABASE='+bd+';UID='+user+';PWD='+pswd)
        #print('Conexion exitosa a: ' + bd)
    except:
        print('Error al conectar')
    #return mssgeCon
    return conexion



@app.get("/CveGralxCol")
async def hsiicart():
    connection = create_server_connection()
    cursorsiic = connection.cursor()
    queryAvisos="SELECT DISTINCT(CVE_GRAL) AS Clave_General, NOMCOL as Colonia FROM sde.HORARIOS_SERVICIO_ADP"
    cursorsiic.execute(queryAvisos)
    columns = [column[0] for column in cursorsiic.description]
    results = []
    for row in cursorsiic.fetchall():
        results.append(dict(zip(columns, row)))
    cursorsiic.close()

    return results

@app.get("/horarioServicioxClave/{clavegral}")
async def hsiicartClave(clavegral: str):
    connection = create_server_connection()
    cursorsiic = connection.cursor()
    queryAvisos="SELECT FUENTES,SERVICIO,ZONA_NUM,REGION,SUPERVIS_1,LUNES,MARTES,MIERCOLES,JUEVES,VIERNES,SABADO,DOMINGO,CVE_GRAL,DIASXSEM,HRS_X_SEM,SERV_1,SERV1_HR_I,SERV1_HR_F,SERV_2,SERV2_HR_I,SERV2_HR_F,SERV_3,SERV3_HR_I,SERV3_HR_F,SERV_4,SERV4_HR_I,SERV4_HR_F,SERV_5, SERV5_HR_I, SERV5_HR_F, CONTINUO, COLONIAS_996 as COLONIA FROM sde.HORARIOS_SERVICIO_ADP WHERE CVE_GRAL = '"+clavegral+"'"
    cursorsiic.execute(queryAvisos)
    columns = [column[0] for column in cursorsiic.description]
    results = []
    for row in cursorsiic.fetchall():
        results.append(dict(zip(columns, row)))
    cursorsiic.close()

    return results

@app.get("/horarioServicioxColonia/{colonia}")
async def hsiicartClaveCol(colonia: str):
    connection = create_server_connection()
    cursorsiic = connection.cursor()
    queryAvisos="SELECT FUENTES,SERVICIO,ZONA_NUM,REGION,SUPERVIS_1,LUNES,MARTES,MIERCOLES,JUEVES,VIERNES,SABADO,DOMINGO,CVE_GRAL,DIASXSEM,HRS_X_SEM,SERV_1,SERV1_HR_I,SERV1_HR_F,SERV_2,SERV2_HR_I,SERV2_HR_F,SERV_3,SERV3_HR_I,SERV3_HR_F,SERV_4,SERV4_HR_I,SERV4_HR_F,SERV_5, SERV5_HR_I, SERV5_HR_F, CONTINUO, COLONIAS_996 as COLONIA FROM sde.HORARIOS_SERVICIO_ADP WHERE COLONIAS_996 = '"+colonia+"'"
    cursorsiic.execute(queryAvisos)
    columns = [column[0] for column in cursorsiic.description]
    results = []
    for row in cursorsiic.fetchall():
        results.append(dict(zip(columns, row)))
    cursorsiic.close()

    return results


@app.get("/logger/{loggerName}")
async def loggers(loggerName: str):
    inicio = 0
    inicio = time.time()    
    connection = create_server_connection()
    cursorlogger = connection.cursor()
    querylogger="SELECT TOP(3)D.Nombre as NombreLogger, D.Serial, C.Valor, T.Nombre as Tipo_Dato, A.maximo, A.minimo, R.Fecha, D.Latitud, D.Longitud FROM SCADA.Dispositivos as D INNER JOIN SCADA.DispositivosCanales as DC ON D.Id=DC.IdDispositivo INNER JOIN SCADA.Canales as C ON DC.idCanal = C.Id INNER JOIN SCADA.Tipos as T ON C.IdTipo = T.Id INNER JOIN SCADA.Alarmas as A ON C.IdAlarma = A.Id INNER JOIN SCADA.Registros as R ON C.Id = R.IdCanal WHERE (D.Habilitado = 1 AND D.Nombre like '%"+loggerName+"%') AND R.Fecha < '"+formatnow+"' ORDER BY R.Fecha DESC;"
    # #Ejecutamos la consulta
    #print(querylogger)
    cursorlogger.execute(querylogger)
    # #imprimimos resultados 
    columns = [column[0] for column in cursorlogger.description]
    results = []
    for row in cursorlogger.fetchall():
        results.append(dict(zip(columns, row)))
    fin = time.time()
    #print("Tiempo de ejecución en minutos: ")
    print((fin-inicio))
    return results

@app.get("/avisos/{colonia}")
async def aviso(colonia: str):    
    connection = create_server_connection()
    cursor = connection.cursor()
    queryAvisos="SELECT WR.CREATE_DATE AS CREACION_AVISO, US.NAME AS CLIENTE, WRT.NAME_TYPE AS TIPO_AVISO, TY.NAME_TYPE AS TIPO_AVERIA, WRE.NAME_TYPE ESTADO_AVISO, ST.NAME_TYPE AS ESTADO_AVERIA, GE.ENTITY_NAME AS COLONIA, SU.COD_NIS AS SUMINISTRO, (SELECT TOP 1 CP11.DESCRIPTION FROM OUC_COMMON_ADMIN.GCGT_WO_CONNECTION_POINT CP11 WHERE CP11.ID_CONNECTION = SS.ID_CONNECTION) AS POLIGONO_DE_SERVICIO_SUMINISTRO, (SELECT TOP 1 CP22.DESCRIPTION FROM OUC_COMMON_ADMIN.GCGT_WO_CONNECTION_POINT CP22 LEFT JOIN OUC_ADMIN.GCCC_OUTAGE_AREA_DETAIL OAD ON OAD.ID_OUTAGE = OU.ID_OUTAGE WHERE CP22.ID_CONNECTION = OAD.ID_CONNECTION) AS POLIGONO_DE_SERVICIO_AVERIA, OFI.OFFICE_NAME AS ZONA, REG.DESCRIPTION AS REGION, EM1.NAME AS RESPONSABLE, FA.ID_FIELD_ACTIVITY AS NUM_OT, TI.DESCRIPTION AS TIPO_OT, AST.DESCRIPTION AS ESTADO_OT, PE.NAME AS SUPERVISOR_BRIGADA, FA.RESOLVE_COMMENTARY AS COMENTARIO_RESOLUCION FROM OUC_ADMIN.GCCC_OUTAGE OU FULL OUTER JOIN OUC_ADMIN.GCCC_OUTAGE_OT GOT ON GOT.ID_OUTAGE = OU.ID_OUTAGE INNER JOIN OUC_ADMIN.GCCC_OUTAGE_TYPE TY ON TY.COD_DEVELOP = OU.OUTAGE_TYPE INNER JOIN OUC_ADMIN.GCCC_OUTAGE_STATUS ST ON ST.COD_DEVELOP = OU.OUTAGE_STATUS INNER JOIN OUC_ADMIN.GCCC_WARNING WR ON WR.ID_OUTAGE = OU.ID_OUTAGE INNER JOIN OUC_ADMIN.GCCC_WARNING_TYPE WRT ON WRT.COD_DEVELOP =WR.WARNING_TYPE INNER JOIN OUC_ADMIN.GCCC_WARNING_STATUS WRE ON WRE.COD_DEVELOP= WR.WARNING_STATUS INNER JOIN OUC_ARQ_ADMIN.GCXS_USER US ON US.USER_ID = WR.UPDATE_USER INNER JOIN OUC_COMMON_ADMIN.GCCOM_ADDRESS AD ON AD.ID_ADDRESS = OU.ID_ADDRESS INNER JOIN OUC_COMMON_ADMIN.GCCOM_STREET STRE ON STRE.ID_STREET = AD.ID_STREET INNER JOIN OUC_COMMON_ADMIN.GCCOM_GEOGRAPHIC_ENTITY GE ON GE.ID_GEO_ENTITY = AD.ID_GEO_ENTITY INNER JOIN OUC_ADMIN.GCGT_WF_ENTITY_RESPONSIBLE ER ON ER.ID_ENTITY_RESPONSIBLE = OU.ID_RESPONSIBLE LEFT JOIN OUC_COMMON_ADMIN.GCCB_EMPLOYEE EM1 ON EM1.ID_EMPLOYEE_CB = ER.ID_EMPLOYEE LEFT JOIN OUCW_ADMIN.GCGT_WO_FIELD_ACTIVITY FA ON FA.ID_FIELD_ACTIVITY = GOT.ID_FIELD_ACTIVITY INNER JOIN OUCW_ADMIN.GCGT_WO_TECHNICAL_CENTER TC ON TC.ID_CENTER = FA.ID_CENTER LEFT JOIN OUCW_ADMIN.GCGT_WO_TIPOLOGY TI ON TI.COD_TIPOLOGY = FA.COD_TIPOLOGY LEFT JOIN OUCW_ADMIN.GCGT_WO_ACTIVITY_STATUS AST ON AST.COD_DEVELOP = FA.COD_STATUS_DEVELOP INNER JOIN OUC_ADMIN.GCCC_OUTAGE_ATTRIBUTES OUA ON OUA.ID_OUTAGE = OU.ID_OUTAGE INNER JOIN OUC_COMMON_ADMIN.GCGT_MASTER_ATTRIBUTES MT ON MT.COD_DEVELOP = OUA.COD_MASTER_ATTRIBUTE INNER JOIN OUC_COMMON_ADMIN.GCGT_ATTRIBUTE_VALUES AV ON AV.COD_DEVELOP = OUA.ATTRIBUTE_VALUE LEFT JOIN OUCW_ADMIN.GCGT_WO_PERSONNEL PE ON PE.ID_PERSONNEL = FA.ID_PERSONNEL LEFT JOIN OUC_COMMON_ADMIN.GCCB_OFFICE OFI ON OFI.ID_OFFICE = OU.ID_BUSINESS_UNIT LEFT JOIN OUC_COMMON_ADMIN.GCCOM_ORGANIZATIONAL_AREA OA ON OA.ID_ORGANIZATIONAL_AREA = OFI.ID_ORGANIZATIONAL_AREA LEFT JOIN OUC_COMMON_ADMIN.GCCOM_ORGANIZATIONAL_AREA REG ON REG.ID_ORGANIZATIONAL_AREA = OA.ID_PARENT_AREA LEFT JOIN OUC_COMMON_ADMIN.GCCOM_SUPPLY SU ON SU.ID_ADDRESS = AD.ID_ADDRESS LEFT JOIN OUC_COMMON_ADMIN.GCCOM_SECTOR_SUPPLY SS ON SS.ID_SUPPLY = SU.ID_SUPPLY LEFT JOIN OUC_COMMON_ADMIN.GCGT_WO_CONNECTION_POINT CP ON CP.ID_CONNECTION = SS.ID_CONNECTION WHERE GE.ENTITY_TYPE = '30' AND TY.NAME_TYPE = 'Falta de Agua' AND WR.CREATE_DATE > '2023-11-01' AND GE.ENTITY_NAME = '"+colonia+"' ORDER BY WR.CREATE_DATE DESC"
    #Ejecutamos la consulta
    cursor.execute(queryAvisos)
    #imprimimos resultados
    #row = cursor.fetchone()
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    cursor.close()

    return results

