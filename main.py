import sys 
sys.path.append('.')

#* Librerías para el API
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import os

#* Librerías propias para el funcionamiento del API
from utils.request_postgres import consulta_clientes_aliados

# Parametros básicos y clases
app = FastAPI() 
puerto = os.environ.get("PORT", 8080)

# Configuración de CORS
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#* Definición de un endpoint
descripcion_path = 'API que retorna información de clientes aliados desde BigQuery, consultando por filtros como código agente, documento, producto, etc.'
summary_path = 'Consulta clientes desde BigQuery con múltiples filtros opcionales (al menos uno requerido)'
endpoint_end = '/api_consulta_portal_aliados'

# Mock de ejemplo de entrada en Body de request
"""
{
  "codigo_agente": "55903",
  "tipo_documento": "CC",
  "id_documento": "1020948732",
  "nombre": "Luz Elena Arevalo Arenas",
  "estado_poliza": "Vigente",
  "producto": "Educadores Plus"
}
"""

@app.post(endpoint_end, summary=summary_path, description=descripcion_path)
async def api_consulta_afiliacion_empresa(body: dict):
    codigo_agente = body.get("codigo_agente")
    tipo_documento = body.get("tipo_documento")
    id_documento = body.get("id_documento")
    nombre = body.get("nombre")
    estado_poliza = body.get("estado_poliza")
    producto = body.get("producto")

    if not any([codigo_agente, tipo_documento, id_documento, nombre, estado_poliza, producto]):
        return {"error": "Debe proporcionar al menos un parámetro"}

    response = consulta_clientes_aliados(
        CLAVE_AGENTE=codigo_agente,
        TIPO_DOCUMENTO_ASEGURADO=tipo_documento,
        NUMERO_DOCUMENTO_ASEGURADO=id_documento,
        NOMBRE=nombre,
        ESTADO_POLIZA=estado_poliza,
        NOMBRE_PRODUCTO=producto
    )
    return response

@app.get("/", response_class=RedirectResponse)
async def redirect_to_docs():
    return "/docs"

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(puerto))
