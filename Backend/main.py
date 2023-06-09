import cx_Oracle
from fastapi import FastAPI, Body, Path, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List
import subprocess

app = FastAPI()
app.title = "Mi aplicación con  FastAPI"
app.version = "0.0.1"


def dbConnection():
    connection = cx_Oracle.connect("user","password","myservice")
    connection.autocommit = True
    return connection


@app.get('/cargarTemporal', tags=['carga_temporal'])
def getCargaMasiva():
    try:
        cursor = dbConnection().cursor()
        sql = "CREATE TABLE DB_EXCEL_OG( \
                NOMBRE_VICTIMA varchar(100), \
                APELLIDO_VICTIMA varchar(100), \
                DIRECCION_VICTIMA varchar(200), \
                FECHA_PRIMERA_SOSPECHA varchar(100), \
                FECHA_CONFIRMACION varchar(100), \
                FECHA_MUERTE varchar(100) , \
                ESTADO_VICTIMA varchar(100), \
                NOMBRE_ASOCIADO varchar(100), \
                APELLIDO_ASOCIADO varchar(100), \
                FECHA_CONOCIO varchar(100), \
                CONTACTO_FISICO varchar(100), \
                FECHA_INICIO_CONTACTO varchar(100), \
                FECHA_FIN_CONTACTO varchar(100), \
                NOMBRE_HOSPITAL varchar(100), \
                DIRECCION_HOSPITAL varchar(200), \
                UBICACION_VICTIMA varchar(200), \
                FECHA_LLEGADA varchar(100), \
                FECHA_RETIRO varchar(100), \
                TRATAMIENTO varchar(100), \
                EFECTIVIDAD int , \
                FECHA_INICIO_TRATAMIENTO varchar(100), \
                FECHA_FIN_TRATAMIENTO varchar(100), \
                EFECTIVIDAD_EN_VICTIMA int  \
                )"
        cursor.execute(sql)

    except:
        return HTMLResponse('<h1>Error al crear tabla temporal</h1>')
    try:
        comando_sqlldr = f"sqlldr sebas/sebas@myservice control=carga_masiva.ctl data=DB_Excel_OG.csv"
        resultado_comando = subprocess.run(comando_sqlldr, shell=True, capture_output=True)
        # Verificar si la carga de datos fue exitosa o no
        #if resultado_comando.returncode == 0:
        #    return HTMLResponse('<h1>Carga masiva completada</h1>')
        return HTMLResponse('<h1>Carga masiva completada</h1>')
    except:
        return HTMLResponse('<h1>Error al cargar la informacion</h1>')

@app.get('/cargarModelo', tags=['carga_modelo'])
def getCargaModelo():

    try:
        #CREACION DE TODAS LAS TABLAS

        cursor = dbConnection().cursor()
        sql = "CREATE TABLE victima( \
                id_victima int generated by default as identity not null, \
                nombre varchar(100), \
                apellido varchar(100), \
                direccion varchar(200), \
                fecha_primera_sospecha timestamp(6), \
                fecha_confirmacion timestamp(6), \
                fecha_muerte timestamp(6), \
                estado_victima varchar(100), \
                primary key(id_victima) \
                )"
        cursor.execute(sql)

        sql = "CREATE TABLE asociados( \
                id_asociado int generated by default as identity not null, \
                nombre varchar(100), \
                apellido varchar(100), \
                primary key(id_asociado) \
                )"
        cursor.execute(sql)

        sql = "CREATE TABLE contacto_con_asociado( \
                id_contacto_asociado int generated by default as identity not null, \
                id_victima int, \
                id_asociado int, \
                dt_inicia timestamp(6), \
                dt_termina timestamp(6), \
                dt_conocer timestamp(6), \
                razon varchar(100), \
                primary key(id_contacto_asociado), \
                constraint fk_victima foreign key (id_victima) references victima(id_victima), \
                constraint fk_asociado foreign key (id_asociado) references asociados(id_asociado) \
                )"
        cursor.execute(sql)

        sql = "CREATE TABLE hospital( \
                id_hospital int generated by default as identity not null, \
                nombre varchar(100), \
                direccion varchar(200), \
                primary key(id_hospital) \
                )"
        cursor.execute(sql)

        sql = "CREATE TABLE hospital_utilizado( \
                id_hosp_u int generated by default as identity not null, \
                id_hospital int, \
                id_victima int, \
                primary key(id_hosp_u), \
                constraint fk_hospital foreign key (id_hospital) references hospital(id_hospital), \
                constraint fk_victima_hosp foreign key (id_victima) references victima(id_victima) \
                )"
        cursor.execute(sql)

        sql = "CREATE TABLE ubicacion( \
                id_ubicacion int generated by default as identity not null, \
                direccion varchar(200), \
                primary key(id_ubicacion) \
                )"
        cursor.execute(sql)

        sql = "CREATE TABLE ubicaciones_victima( \
                id_ubicacion_v int generated by default as identity not null,  \
                id_ubicacion int,  \
                id_victima int,  \
                dt_llegada timestamp(6),  \
                dt_salida timestamp(6),  \
                primary key(id_ubicacion_v),  \
                constraint fk_ubicacion foreign key (id_ubicacion) references ubicacion(id_ubicacion),  \
                constraint fk_victima_ubi foreign key (id_victima) references victima(id_victima)  \
                )"
        cursor.execute(sql)

        sql = "CREATE TABLE tratamiento( \
                id_tratamiento int generated by default as identity not null, \
                nombre_tratamiento varchar(100), \
                efectividad_gen int, \
                primary key(id_tratamiento) \
                )"
        cursor.execute(sql)

        sql = "CREATE TABLE tratamiento_aplicado( \
                id_trtmnto_aplicado int generated by default as identity not null, \
                id_tratamiento int, \
                id_victima int, \
                dt_inicio timestamp(6), \
                dt_final timestamp(6), \
                efectividad int, \
                primary key(id_trtmnto_aplicado), \
                constraint fk_tratamiento foreign key (id_tratamiento) references tratamiento(id_tratamiento), \
                constraint fk_victima_trt foreign key (id_victima) references victima(id_victima) \
                )"
        cursor.execute(sql)

        #LLENAR TABLAS CON LA INFO DE LA TABLA CARGADA DE MANERA MASIVA
       
        sql = "INSERT INTO victima(nombre, apellido, direccion, fecha_primera_sospecha, fecha_confirmacion, fecha_muerte , estado_victima) \
                SELECT DISTINCT NOMBRE_VICTIMA,  \
                APELLIDO_VICTIMA,  \
                DIRECCION_VICTIMA,  \
                TO_DATE(FECHA_PRIMERA_SOSPECHA, 'DD/MM/YYYY HH24:MI'),  \
                TO_DATE(FECHA_CONFIRMACION, 'DD/MM/YYYY HH24:MI'),  \
                TO_DATE(FECHA_MUERTE, 'DD/MM/YYYY HH24:MI'), \
                ESTADO_VICTIMA    \
                FROM DB_EXCEL_OG"
        cursor.execute(sql)

        sql = "INSERT INTO asociados(nombre, apellido) \
                SELECT DISTINCT NOMBRE_ASOCIADO,  \
                APELLIDO_ASOCIADO   \
                FROM DB_EXCEL_OG"
        cursor.execute(sql)

        sql = "INSERT INTO hospital(nombre, direccion) \
                SELECT DISTINCT NOMBRE_HOSPITAL,   \
                DIRECCION_HOSPITAL  \
                FROM DB_EXCEL_OG"
        cursor.execute(sql)

        sql = "INSERT INTO ubicacion(direccion) \
                SELECT DISTINCT UBICACION_VICTIMA  \
                FROM DB_EXCEL_OG"
        cursor.execute(sql)

        sql = "INSERT INTO tratamiento(nombre_tratamiento, efectividad_gen) \
                SELECT DISTINCT TRATAMIENTO, EFECTIVIDAD \
                FROM DB_EXCEL_OG"
        cursor.execute(sql)

        sql = "INSERT INTO CONTACTO_CON_ASOCIADO(id_victima, id_asociado, dt_inicia, dt_termina, dt_conocer, razon) \
                SELECT DISTINCT VIC.ID_VICTIMA, ASO.ID_ASOCIADO,  \
                TO_DATE(TT.FECHA_INICIO_CONTACTO,'DD/MM/YYYY HH24:MI'),  \
                TO_DATE(TT.FECHA_FIN_CONTACTO,'DD/MM/YYYY HH24:MI'),  \
                TO_DATE(TT.FECHA_CONOCIO,'DD/MM/YYYY HH24:MI'),  \
                TT.CONTACTO_FISICO \
                FROM DB_EXCEL_OG TT \
                INNER JOIN VICTIMA vic \
                    ON TT.NOMBRE_VICTIMA = VIC.NOMBRE AND TT.APELLIDO_VICTIMA = VIC.APELLIDO \
                INNER JOIN ASOCIADOS aso \
                    ON TT.NOMBRE_ASOCIADO = ASO.NOMBRE AND TT.APELLIDO_ASOCIADO = ASO.APELLIDO "
        cursor.execute(sql)

        sql = "INSERT INTO HOSPITAL_UTILIZADO (id_hospital, id_victima) \
                SELECT DISTINCT hosp.ID_HOSPITAL,  \
                vic.ID_VICTIMA \
                FROM DB_EXCEL_OG TT \
                INNER JOIN HOSPITAL hosp \
                    ON TT.NOMBRE_HOSPITAL = hosp.NOMBRE \
                INNER JOIN VICTIMA vic \
                    ON TT.NOMBRE_VICTIMA = vic.NOMBRE AND TT.APELLIDO_VICTIMA = VIC.APELLIDO"
        cursor.execute(sql)

        sql = "INSERT INTO UBICACIONES_VICTIMA(id_ubicacion, id_victima, dt_llegada, dt_salida) \
                SELECT DISTINCT ubi.ID_UBICACION, v.ID_VICTIMA,  \
                TO_DATE(TT.FECHA_LLEGADA ,'DD/MM/YYYY HH24:MI'),  \
                TO_DATE(TT.FECHA_RETIRO ,'DD/MM/YYYY HH24:MI')  \
                FROM DB_EXCEL_OG TT \
                INNER JOIN UBICACION ubi \
                    ON TT.UBICACION_VICTIMA = UBI.DIRECCION \
                INNER JOIN VICTIMA v \
                    ON TT.NOMBRE_VICTIMA = v.NOMBRE AND TT.APELLIDO_VICTIMA = V.APELLIDO"
        cursor.execute(sql)

        sql = "INSERT INTO TRATAMIENTO_APLICADO(id_tratamiento, id_victima, dt_inicio, dt_final, efectividad) \
                SELECT DISTINCT t.ID_TRATAMIENTO, v.ID_VICTIMA,  \
                TO_DATE(tt.FECHA_INICIO_TRATAMIENTO,'DD/MM/YYYY HH24:MI'),  \
                TO_DATE(tt.FECHA_FIN_TRATAMIENTO,'DD/MM/YYYY HH24:MI'),  \
                tt.EFECTIVIDAD_EN_VICTIMA \
                FROM DB_EXCEL_OG TT \
                INNER JOIN TRATAMIENTO t \
                    ON TT.TRATAMIENTO = t.NOMBRE_TRATAMIENTO \
                INNER JOIN VICTIMA v  \
                    ON TT.NOMBRE_VICTIMA = v.NOMBRE AND TT.APELLIDO_VICTIMA = v.APELLIDO "
        cursor.execute(sql)

        return HTMLResponse('<h1>Modelo cargado exitosamente</h1>')

    except cx_Oracle.DatabaseError as e:
        print("------ load model error ------")
        print(e)
        return HTMLResponse('<h1>Error al cargar el modelo</h1>')


@app.get('/eliminarTemporal', tags=['eliminar_temporal'])
def getEliminarTemp():
    try:
        cursor = dbConnection().cursor()
        sql = "DROP TABLE DB_EXCEL_OG"
        cursor.execute(sql)
        return HTMLResponse('<h1>Tabla Temporal eliminada</h1>')
    except:
        return HTMLResponse('<h1>Error al eliminar tabla temporal</h1>')

@app.get('/eliminarModelo', tags=['eliminar_modelo'])
def getEliminarModelo():
    try:
        cursor = dbConnection().cursor()
        sql = "DROP TABLE contacto_con_asociado"
        cursor.execute(sql)
        sql = "DROP TABLE hospital_utilizado"
        cursor.execute(sql)
        sql = "DROP TABLE ubicaciones_victima"
        cursor.execute(sql)
        sql = "DROP TABLE tratamiento_aplicado"
        cursor.execute(sql)
        sql = "DROP TABLE asociados"
        cursor.execute(sql)
        sql = "DROP TABLE hospital"
        cursor.execute(sql)
        sql = "DROP TABLE ubicacion"
        cursor.execute(sql)
        sql = "DROP TABLE tratamiento"
        cursor.execute(sql)
        sql = "DROP TABLE victima"
        cursor.execute(sql)


        return HTMLResponse('<h1>Modelo eliminado con exito</h1>')
    except cx_Oracle.DatabaseError as e:
        print("------ delete error ------")
        print(e)
        return HTMLResponse('<h1>Error al eliminar el modelo</h1>')


@app.get('/consulta1', tags=['consulta1'])
def getConsulta1():
    cursor = dbConnection().cursor()
    sql = "SELECT DISTINCT h.NOMBRE ,  h.DIRECCION  , COUNT(v.ESTADO_VICTIMA) CANTIDAD_MUERTES \
            FROM HOSPITAL_UTILIZADO hu \
            INNER JOIN HOSPITAL h \
                ON hu.ID_HOSPITAL = h.ID_HOSPITAL \
            INNER JOIN VICTIMA v  \
                ON hu.ID_VICTIMA = v.ID_VICTIMA \
            WHERE v.ESTADO_VICTIMA = 'Muerte' OR v.FECHA_MUERTE IS NOT NULL \
            GROUP BY h.NOMBRE, h.DIRECCION \
            ORDER BY 3 DESC"
    cursor .execute(sql)
    data = cursor.fetchall()
    return data

@app.get('/consulta2', tags=['consulta2'])
def getConsulta2():
    cursor = dbConnection().cursor()
    sql = "SELECT v.NOMBRE, v.APELLIDO, v.ESTADO_VICTIMA, t.NOMBRE_TRATAMIENTO, ta.EFECTIVIDAD \
            FROM VICTIMA v \
            INNER JOIN TRATAMIENTO_APLICADO ta \
                ON V.ID_VICTIMA = ta.ID_VICTIMA \
            INNER JOIN TRATAMIENTO t \
                ON ta.ID_TRATAMIENTO = t.ID_TRATAMIENTO \
            WHERE t.NOMBRE_TRATAMIENTO = 'Transfusiones de sangre' AND ta.EFECTIVIDAD > 5 AND v.ESTADO_VICTIMA = 'En cuarentena' \
            ORDER BY 5 DESC"
    cursor.execute(sql)
    data = cursor.fetchall()
    return data

@app.get('/consulta3', tags=['consulta3'])
def getConsulta3():
    cursor = dbConnection().cursor()
    sql = "SELECT v.NOMBRE, v.APELLIDO, v.DIRECCION, COUNT(DISTINCT CCA.ID_ASOCIADO) as PERS_ASOCIADAS \
            FROM VICTIMA v \
            INNER JOIN CONTACTO_CON_ASOCIADO cca \
                ON v.ID_VICTIMA = CCA.ID_VICTIMA \
            WHERE v.ESTADO_VICTIMA = 'Muerte' OR FECHA_MUERTE IS NOT NULL \
            GROUP BY v.NOMBRE, v.APELLIDO, v.DIRECCION \
            HAVING COUNT(DISTINCT CCA.ID_ASOCIADO) > 3 \
            ORDER BY 4 DESC"
    cursor.execute(sql)
    data = cursor.fetchall()
    return data

@app.get('/consulta4', tags=['consulta4'])
def getConsulta4():
    cursor = dbConnection().cursor()
    sql = "SELECT v.NOMBRE, v.APELLIDO, COUNT(DISTINCT CCA.ID_ASOCIADO) as PERS_ASOCIADAS \
            FROM VICTIMA v \
            INNER JOIN CONTACTO_CON_ASOCIADO cca \
                ON v.ID_VICTIMA = CCA.ID_VICTIMA \
            WHERE v.ESTADO_VICTIMA = 'Suspendido' AND CCA.RAZON = 'Beso' \
            GROUP BY v.NOMBRE, v.APELLIDO, v.DIRECCION \
            HAVING COUNT(DISTINCT CCA.ID_ASOCIADO) > 2 \
            ORDER BY 3 DESC"
    cursor.execute(sql)
    data = cursor.fetchall()
    return data

@app.get('/consulta5', tags=['consulta5'])
def getConsulta5():
    cursor = dbConnection().cursor()
    sql = "SELECT v.NOMBRE, v.APELLIDO, COUNT(TA.ID_TRATAMIENTO) \
            FROM VICTIMA v  \
            INNER JOIN TRATAMIENTO_APLICADO ta \
                ON V.ID_VICTIMA = TA.ID_VICTIMA \
            INNER JOIN TRATAMIENTO t \
                ON t.ID_TRATAMIENTO = TA.ID_TRATAMIENTO  \
            WHERE T.NOMBRE_TRATAMIENTO = 'Oxigeno' \
            GROUP BY v.NOMBRE, v.APELLIDO  \
            ORDER BY 3 DESC \
            FETCH NEXT 5 ROWS ONLY"
    cursor.execute(sql)
    data = cursor.fetchall()
    return data

@app.get('/consulta6', tags=['consulta6'])
def getConsulta6():
    return HTMLResponse('<h1>Consulta6</h1>')

@app.get('/consulta7', tags=['consulta7'])
def getConsulta7():
    cursor = dbConnection().cursor()
    sql = "SELECT v.nombre, v.apellido,v.DIRECCION \
            FROM VICTIMA v \
            WHERE ( \
            SELECT COUNT(*) FROM TRATAMIENTO_APLICADO ta \
            INNER JOIN TRATAMIENTO t  \
            ON t.ID_TRATAMIENTO  = ta.ID_TRATAMIENTO  \
            WHERE ta.ID_VICTIMA  = v.ID_VICTIMA) = 2  \
            ORDER BY v.nombre"
    cursor.execute(sql)
    data = cursor.fetchall()
    return data

@app.get('/consulta8', tags=['consulta8'])
def getConsulta8():
    cursor = dbConnection().cursor()
    sql = "SELECT v.NOMBRE AS nombre, v.APELLIDO AS apellido, \
            EXTRACT(MONTH FROM v.FECHA_PRIMERA_SOSPECHA) AS mes, \
            COUNT(t.ID_TRATAMIENTO) AS cant_tratamientos \
            FROM VICTIMA v \
            INNER JOIN TRATAMIENTO_APLICADO ta ON v.ID_VICTIMA = ta.ID_VICTIMA \
            INNER JOIN TRATAMIENTO t ON t.ID_TRATAMIENTO = ta.ID_TRATAMIENTO \
            GROUP BY v.NOMBRE, v.APELLIDO, v.FECHA_PRIMERA_SOSPECHA \
            HAVING COUNT(t.ID_TRATAMIENTO) = ( \
            SELECT MAX(cant_tratamientos) \
            FROM ( \
            SELECT COUNT(*) AS cant_tratamientos \
            FROM TRATAMIENTO_APLICADO \
            GROUP BY ID_VICTIMA \
            ) tratamientos_por_victima \
            ) OR COUNT(t.ID_TRATAMIENTO) = ( \
            SELECT MIN(cant_tratamientos) \
            FROM ( \
            SELECT COUNT(*) AS cant_tratamientos \
            FROM TRATAMIENTO_APLICADO \
            GROUP BY ID_VICTIMA \
            ) tratamientos_por_victima \
            ) \
            ORDER BY cant_tratamientos DESC"
    cursor.execute(sql)
    data = cursor.fetchall()
    return data


@app.get('/consulta9', tags=['consulta9'])
def getConsulta9():
    cursor = dbConnection().cursor()
    sql = "SELECT h.NOMBRE, \
            COUNT(hu.ID_VICTIMA) AS VIC_PER_HOSP, \
            (COUNT(hu.ID_VICTIMA)/366)*100 PORCT_VICTMS \
            FROM VICTIMA v  \
            INNER JOIN HOSPITAL_UTILIZADO hu \
                ON V.ID_VICTIMA = HU.ID_VICTIMA \
            INNER JOIN HOSPITAL h  \
                ON h.ID_HOSPITAL = hu.ID_HOSPITAL \
            GROUP BY h.nombre \
            ORDER BY 1"
    cursor.execute(sql)
    data = cursor.fetchall()
    return data


@app.get('/consulta10', tags=['consulta10'])
def getConsulta10():
    cursor = dbConnection().cursor()
    sql = "SELECT nombre, razon, CANT_CASOS \
            FROM ( \
              SELECT h.NOMBRE AS nombre, cca.RAZON AS razon, COUNT(cca.RAZON) AS CANT_CASOS, \
                     ROW_NUMBER() OVER (PARTITION BY h.NOMBRE ORDER BY COUNT(cca.RAZON) DESC) AS RN \
              FROM VICTIMA v  \
              INNER JOIN CONTACTO_CON_ASOCIADO cca  \
                ON v.ID_VICTIMA = cca.ID_VICTIMA  \
              INNER JOIN HOSPITAL_UTILIZADO hu  \
                ON hu.ID_VICTIMA = v.ID_VICTIMA \
              INNER JOIN HOSPITAL h  \
                ON h.ID_HOSPITAL = hu.ID_HOSPITAL \
              WHERE cca.RAZON IS NOT NULL \
              GROUP BY h.NOMBRE, cca.RAZON \
            ) \
            WHERE RN = 1"
    cursor.execute(sql)
    data = cursor.fetchall()
    return data
