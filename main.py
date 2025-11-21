from fastapi import FastAPI
from google.cloud import bigquery
from pipeline import ejecutar_pipeline
from carga_params import generar_filtro_cm


app = FastAPI()
bq = bigquery.Client()


# -------------------------------------------------------------
# 1) ENDPOINT PRINCIPAL DE PLANIFICACIÓN
# -------------------------------------------------------------
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


# -------------------------------------------------------------
# 2) ENDPOINT DE ROTURAS TOTALES PARA REVISIÓN MANUAL
# -------------------------------------------------------------
@app.get("/materiales_revisar")
def materiales_revisar(proveedor_id: int):

    client = bigquery.Client()

    # 1) Reutilizamos exactamente el mismo filtro CM
    df_cm = generar_filtro_cm(client, proveedor_id)

    if df_cm.empty:
        return {
            "proveedor_id": proveedor_id,
            "materiales_revisar": []
        }

    # Convertimos a tu lista de pares CM
    pares = [(row["Centro"], row["Material"]) for _, row in df_cm.iterrows()]

    # 2) Construir lista con formato BIGQUERY
    cm_structs = ",\n        ".join(
        [f"STRUCT('{c}' AS Centro, {m} AS Material)" for c, m in pares]
    )

    # 3) Consulta limpia contra tu vista ya curada
    query = f"""
        WITH cm AS (
          SELECT * FROM UNNEST([
            {cm_structs}
          ])
        )
        SELECT 
            z.Centro,
            z.Material,
            z.dias_rotura_21d,
            z.CMD_SAP,
            z.CMD_Ajustado_Rotura,
            z.ratio_ly,
            z.CMD_Ajustado_Final
        FROM `business-intelligence-444511.granier_logistica.v_ZLO12_curado` z
        JOIN cm USING (Centro, Material)
        WHERE z.Flag_Rotura_Total = 1
    """

    results = client.query(query).result()

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

    return {
        "proveedor_id": proveedor_id,
        "materiales_revisar": materiales
    }


