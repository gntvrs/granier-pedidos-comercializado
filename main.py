from fastapi import FastAPI
from google.cloud import bigquery

from pipeline import ejecutar_pipeline
from pipeline_v2 import ejecutar_pipeline_v2        # ⬅️ añadimos esto

from carga_params import generar_filtro_cm


app = FastAPI()
bq = bigquery.Client()


# -------------------------------------------------------------
# 1) ENDPOINT PRINCIPAL DE PLANIFICACIÓN (V1)
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

    # ============================================================
    # DEBUG DEFINITIVO – Localizar qué valor rompe JSON
    # ============================================================
    
    import json
    
    print("\n🔍 DEBUG JSON – Analizando pedidos uno a uno...\n")
    
    for idx, ped in enumerate(resultado["pedidos"]):
        try:
            json.dumps(ped)
        except Exception as e:
            print("❌ ERROR EN JSON EN FILA:", idx)
            print("Contenido completo del pedido problemático:")
            print(ped)
            print("\nAnalizando campos uno por uno:")
            for k, v in ped.items():
                try:
                    json.dumps(v)
                except Exception as e_field:
                    print(f"   🔥 Campo problemático: {k}")
                    print(f"      Valor: {repr(v)}")
                    print(f"      Error: {e_field}")
            raise   # ← Reventar el endpoint expresamente para ver el error en logs
    
    print("\n✅ DEBUG JSON – Ningún problema detectado en pedidos individuales")


    return {
        "status": "OK",
        "proveedor_id": proveedor_id,
        "consumo_extra_pct": consumo_extra_pct,
        "resultado": resultado
    }


# -------------------------------------------------------------
# 1.1) NUEVO ENDPOINT DE PLANIFICACIÓN V2 (experimental)
# -------------------------------------------------------------
@app.get("/planificar_v2")
def planificar_v2(
    proveedor_id: int,
    consumo_extra_pct: float = 0.0
):
    """
    Versión experimental del pipeline (V2).
    Lleva: CMD ajustado por rotura + estacionalidad + restricción logística V2 + CAP/PAL.
    """

    resultado = ejecutar_pipeline_v2(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct
    )

    return {
        "status": "OK_V2",
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

    # 1) Reutilizamos el mismo filtro CM del pipeline
    df_cm = generar_filtro_cm(client, proveedor_id)

    if df_cm.empty:
        return {
            "proveedor_id": proveedor_id,
            "materiales_revisar": []
        }

    # Convertir a lista de pares CM
    pares = [(row["Centro"], row["Material"]) for _, row in df_cm.iterrows()]

    # 2) Construcción dinámica para BigQuery
    cm_structs = ",\n        ".join(
        [f"STRUCT('{c}' AS Centro, {m} AS Material)" for c, m in pares]
    )

    # 3) Query contra vista curada
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



