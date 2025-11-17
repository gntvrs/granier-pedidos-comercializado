from fastapi import FastAPI
from pipeline import ejecutar_pipeline

app = FastAPI()

@app.get("/planificar")
def planificar(
    proveedor_id: int,
    consumo_extra_pct: float = 0.0
):
    resultado = ejecutar_pipeline(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct
    )

    return {
        "status": "OK",
        "proveedor_id": proveedor_id,
        "consumo_extra_pct": consumo_extra_pct,
        "resultado": resultado
    }
