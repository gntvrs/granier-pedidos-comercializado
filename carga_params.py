from google.cloud import bigquery
import pandas as pd

def generar_filtro_cm(client, PROJECT_ID, proveedor_id):
    sql = f"""
    WITH
    hist_me2l AS (
      SELECT DISTINCT CAST(Material AS INT64) AS Material,
                      CAST(Centro AS STRING)  AS Centro
      FROM `{PROJECT_ID}.granier_staging.stg_ME2L`
      WHERE Proveedor = {proveedor_id}
        AND Centro IN ('0801','2801','2901','4601','1009')
        AND Material IS NOT NULL
    ),
    pendientes AS (
      SELECT DISTINCT CAST(Material AS INT64) AS Material,
                      CAST(Centro AS STRING)  AS Centro
      FROM `{PROJECT_ID}.granier_logistica.Tbl_Pedidos_Pendientes`
      WHERE Proveedor = {proveedor_id}
        AND Centro IN ('0801','2801','2901','4601','1009')
        AND Material IS NOT NULL
    ),
    union_all AS (
      SELECT * FROM hist_me2l
      UNION DISTINCT
      SELECT * FROM pendientes
    ),
    zlo AS (
      SELECT CAST(Centro AS STRING) AS Centro,
             CAST(Material AS INT64) AS Material
      FROM `{PROJECT_ID}.granier_staging.stg_ZLO12`
      WHERE Fecha = CURRENT_DATE()
        AND Centro IN ('0801','2801','2901','4601','1009')
    )
    SELECT DISTINCT z.Centro, z.Material
    FROM zlo z
    JOIN union_all u USING (Material)
    """

    return client.query(sql).to_dataframe()


def cargar_datos_reales(
    proveedor_id: int,
    consumo_extra_pct: float = 0.0
):

    client = bigquery.Client()
    PROJECT_ID = "business-intelligence-444511"

    # ======================
    # 1) Filtro dinámico CM
    # ======================
    df_cm = generar_filtro_cm(client, PROJECT_ID, proveedor_id)
    pares = {(row["Centro"], row["Material"]) for _, row in df_cm.iterrows()}

    # ======================
    # 2) Stock + consumo
    # ======================
    unnest_structs = ", ".join([
        f"STRUCT('{c}' AS Centro, {m} AS Material)"
        for c, m in pares
    ])

    sql_sc = f"""
    WITH cm AS (
      SELECT * FROM UNNEST([
        {unnest_structs}
      ])
    )
    SELECT 
      z.Centro,
      z.Material,
      z.Consumo_medio_diario,
      z.Libre_util_centro + z.Cantidad_pdte_entrada - z.Cantidad_pdte_salida AS Stock,
      z.Piezas_por_amasada AS cantidad_min_fabricacion
    FROM `{PROJECT_ID}.granier_staging.stg_ZLO12` z
    JOIN cm USING (Centro, Material)
    WHERE z.Fecha = CURRENT_DATE()
    """

    df_sc = client.query(sql_sc).to_dataframe()

    # ======================
    # 3) Ajuste consumo
    # ======================
    consumo_diario = {
        (row["Centro"], row["Material"]):
            row["Consumo_medio_diario"] * (1 + consumo_extra_pct)
        for _, row in df_sc.iterrows()
    }

    # ======================
    # 4) Stock fábrica + logística
    # ======================
    # ... igual que en tu script ...

    return {
        "stock_inicial_centros": df_sc[["Centro","Material","Stock"]],
        "consumo_diario": consumo_diario,
        # ...
    }
