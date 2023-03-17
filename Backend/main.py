import cx_Oracle
from fastapi import FastAPI, Body, Path, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List

app = FastAPI()
app.title = "Mi aplicación con  FastAPI"
app.version = "0.0.1"


def dbConnection():
    connection = cx_Oracle.connect("sebas","sebas","myservice")
    return connection

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
    sql = "SELECT v.NOMBRE, v.APELLIDO, v.DIRECCION, COUNT(DISTINCT CCA.ID_ASOCIADO) as PERS_ASOCIADAS, \
            COUNT (DISTINCT ta.ID_TRATAMIENTO) as CANT_TRTM \
            FROM VICTIMA v  \
            INNER JOIN CONTACTO_CON_ASOCIADO cca  \
                ON v.ID_VICTIMA = CCA.ID_VICTIMA \
            INNER JOIN HOSPITAL_UTILIZADO hu  \
                ON v.ID_VICTIMA = hu.ID_VICTIMA \
            INNER JOIN TRATAMIENTO_APLICADO ta \
                ON V.ID_VICTIMA = TA.ID_VICTIMA \
            GROUP BY v.NOMBRE, v.APELLIDO, v.DIRECCION \
            HAVING COUNT(DISTINCT CCA.ID_ASOCIADO) < 2 AND COUNT (DISTINCT ta.ID_TRATAMIENTO) = 2 \
            ORDER BY 5 DESC"
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