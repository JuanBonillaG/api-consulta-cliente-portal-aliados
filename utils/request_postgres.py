import json
import sqlalchemy
import pandas as pd
import datetime as dt

from utils.connect_sql import getEngine

def request_postgres(input_query, params):

  # Crear el motor de SQLAlchemy
  engine = getEngine()
  
  with engine.connect() as connection:

    results = connection.execute(sqlalchemy.text(input_query), params)

    data = pd.DataFrame(results.fetchall())
    
    return data

def consulta_clientes_aliados(CLAVE_AGENTE = None,TIPO_DOCUMENTO_ASEGURADO = None,NUMERO_DOCUMENTO_ASEGURADO = None,NOMBRE = None,ESTADO_POLIZA = None,CODIGO_PRODUCTO = None,):
    try:
        filtros = {
            "CLAVE_AGENTE": CLAVE_AGENTE,
            "TIPO_DOCUMENTO_ASEGURADO": TIPO_DOCUMENTO_ASEGURADO,
            "NUMERO_DOCUMENTO_ASEGURADO": NUMERO_DOCUMENTO_ASEGURADO,
            "NOMBRE": NOMBRE,
            "ESTADO_POLIZA": ESTADO_POLIZA,
            "CODIGO_PRODUCTO": CODIGO_PRODUCTO
        }

        # Inicializar las cláusulas WHERE y los parámetros
        where_clauses = []
        params = {}

        # Construir las cláusulas WHERE dinámicamente (solo si el filtro es enviado dentro del body)
        for key, value in filtros.items():
            if value is not None:
                where_clauses.append(f'"{key}" = {value}')
                params[key] = value

        # Verificar si se proporcionaron filtros
        if not where_clauses:
            return {
                "error": "Debe proporcionar al menos un filtro"
            }
        
        # Unir las cláusulas WHERE con AND
        where_sql = " AND ".join(where_clauses)

        # Construir la consulta SQL
        #TODO: Crar clientes_portal_aliados en api_backend
        query = f"""
            SELECT 
              "CLAVE_AGENTE",
              "TIPO_DOCUMENTO_ASEGURADO",
              "NUMERO_DOCUMENTO_ASEGURADO",
              "NOMBRE",
              "ESTADO_POLIZA",
              "CODIGO_PRODUCTO",
              "CIUDAD",
              "DIRECCION",
              "CELULAR",
              "CORREO",
              "PRODUCTOS_VIGENTES",
              "FECHA_NACIMIENTO",
              "EDAD",
              "ESTADO_CIVIL",
              "FECHA_SARLAFT_SIMPLIFICADO",
              "FECHA_SARLAFT_ORDINARIO",
              "NUMERO_POLIZA",
              "PRODUCTO_POLIZA",
              "ESTADO_POLIZA",
              "ROL",
              "VALOR_ASEGURADO",
              "VALOR_PRIMA",
              "VALOR_ASISTENCIA",
              "VIGENCIA_INICIO_POLIZA",
              "VIGENCIA_FIN_POLIZA",
              "FECHA_EMISION",
              "CODIGO_RAMO_EMISION",
              "NOMBRE_RAMO_EMISION",
              "CODIGO_SUBPRODUCTO",
              "NOMBRE_SUBPRODUCTO",
              "NUMERO_POLIZA_MADRE",
              "CANTIDAD_RIESGOS",
              "TIPO_DOCUMENTO_TOMADOR",
              "NUMERO_DOCUMENTO_TOMADOR",
              "NOMBRE_TOMADOR",
              "VIGENCIA_INICIO_POLIZA_MADRE",
              "VIGENCIA_FIN_POLIZA_MADRE",
              "PERIODICIDAD_PAGO",
              "FORMA_PAGO",
              "CANAL_DE_DESCUENTO",
              "VALOR_ASEGURADO"
            FROM "api_backend"."paliados_clientes"
            WHERE {where_sql};
        """

        response = request_postgres(query, params)
        response = json.loads(response.to_json(orient='records', date_format='iso'))

        return {"clientes": response}

    except Exception as e:
        return {
            "error": str(e),
            "message": "Excepción en la consulta de clientes aliados"
        }