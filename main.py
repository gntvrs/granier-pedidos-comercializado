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
from fastapi import FastAPI
from google.cloud import bigquery

app = FastAPI()
bq = bigquery.Client()

@app.get("/materiales_revisar")
def materiales_revisar():
    query = """
        SELECT
            Centro,
            Material,
            dias_rotura_21d,
            CMD_SAP,
            CMD_Ajustado_Rotura,
            ratio_ly,
            CMD_Ajustado_Final
        FROM `business-intelligence-444511.granier_logistica.v_ZLO12_curado`
        WHERE Flag_Rotura_Total = 1
    """

    results = bq.query(query).result()

    materiales = []
    for row in results:
        materiales.append({
            "centro": row.Centro,
            "material": row.Material,
            "dias_rotura_21d": row.dias_rotura_21d,
            "cmd_sap": row.CMD_SAP,
            "cmd_ajustado_rotura": row.CMD_Ajustado_Rotura,
            "ratio_ly": row.ratio_ly,
            "cmd_ajustado_final": row.CMD_Ajustado_Final,
        })

    return {"materiales_revisar": materiales}

