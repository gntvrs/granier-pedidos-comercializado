from carga_params import cargar_datos_reales
from funciones_stg import (
    forecast_stock_centros,
    generar_pedidos_centros_desde_forecastV2,
    ajustar_pedidos_a_minimos_logisticos,
    ajustar_pedidos_por_restricciones_logisticas
)

from google.cloud import bigquery
import pandas as pd
import math

PROJECT_ID = "business-intelligence-444511"
DATASET = "granier_logistica"

def ejecutar_pipeline(proveedor_id: int, consumo_extra_pct: float):

    client = bigquery.Client()

    # 1) Carga dinámica de datos
    datos = cargar_datos_reales(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct
    )

    stock_centros   = datos["stock_inicial_centros"]
    consumo_diario  = datos["consumo_diario"]
    dias_obj        = datos["dias_stock_objetivo"]
    dias_seg        = datos["dias_stock_seguridad"]

    # ... tu pipeline actual aquí, sin tocar lógica ...
    # (Puedes copiar literalmente el contenido de pipeline_EPTY.py
    #  salvo la carga inicial y la persistencia)

    # EJEMPLO – al final del pipeline, en tu lógica:
    return {
        "stock_centros_rows": len(stock_centros),
        "materiales": len({m for _, m in stock_centros.itertuples(index=False)}),
        "centros": len({c for c, _ in stock_centros.itertuples(index=False)})
    }
