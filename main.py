from fastapi import FastAPI
from pipeline import ejecutar_pipeline

app = FastAPI(
    title="Planificación Proveedores Granier",
    version="1.0.0"
)

@app.get("/planificar")
def planificar(
    proveedor_id: int,
    consumo_extra_pct: float = 0.0
):
    """
    Ejecuta la planificación completa:
    - Generación dinámica de filtro Centro-Material
    - Ajuste del consumo medio
    - Forecast 60 días
    - Generación de pedidos
    - Ajustes logísticos
    - Persistencia en BigQuery
    """
    result = ejecutar_pipeline(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct
    )

    return {
        "status": "OK",
        "proveedor_id": proveedor_id,
        "consumo_extra_pct": consumo_extra_pct,
        "detalles": result
    }
