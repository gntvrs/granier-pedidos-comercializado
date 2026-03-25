from typing import Optional

from fastapi import FastAPI
from google.cloud import bigquery

from pipeline import ejecutar_pipeline
from pipeline_v2 import ejecutar_pipeline_v2
from carga_params import generar_filtro_cm


app = FastAPI()
bq = bigquery.Client()


@app.get("/planificar")
def planificar(
    proveedor_id: int,
    consumo_extra_pct: float = 0.0
):
    resultado = ejecutar_pipeline(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct
    )

    import json

    print("\n🔍 DEBUG JSON – Analizando pedidos uno a uno...\n")

    for idx, ped in enumerate(resultado["pedidos"]):
        try:
            json.dumps(ped)
        except Exception:
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
            raise

    print("\n✅ DEBUG JSON – Ningún problema detectado en pedidos individuales")

    return {
        "status": "OK",
        "proveedor_id": proveedor_id,
        "consumo_extra_pct": consumo_extra_pct,
        "resultado": resultado
    }


@app.get("/planificar_v2")
def planificar_v2(
    proveedor_id: Optional[int] = None,
    consumo_extra_pct: float = 0.0,
    centro: str | None = None,
    fecha_corte: str | None = None
):
    """
    V2:
    - si proveedor_id viene informado, planifica ese proveedor
    - si proveedor_id viene vacío, construye el universo completo y resuelve
      el proveedor por Material usando el último Fecha_Pedido de stg_ME2L
    """

    resultado = ejecutar_pipeline_v2(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct,
        centro=centro,
        fecha_corte=fecha_corte
    )

    return {
        "status": "OK_V2",
        "proveedor_id": proveedor_id,
        "consumo_extra_pct": consumo_extra_pct,
        "centro": centro,
        "fecha_corte": fecha_corte,
        "resultado": resultado
    }


@app.get("/materiales_revisar")
def materiales_revisar(proveedor_id: int):

    client = bigquery.Client()
    df_cm = generar_filtro_cm(client, proveedor_id)

    if df_cm.empty:
        return {
            "proveedor_id": proveedor_id,
            "materiales_revisar": []
        }

    pares = [(row["Centro"], row["Material"]) for _, row in df_cm.iterrows()]

    cm_structs = ",\n        ".join(
        [f"STRUCT('{c}' AS Centro, {m} AS Material)" for c, m in pares]
    )

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
