# ============================================================
# carga_params.py – Carga dinámica desde BigQuery
# ============================================================

from google.cloud import bigquery
import pandas as pd


PROJECT_ID = "business-intelligence-444511"


# ============================================================
# 1) GENERAR PARES CENTRO–MATERIAL POR PROVEEDOR
# ============================================================

def generar_filtro_cm(client, proveedor_id: int):
    """
    Devuelve un DataFrame con todas las parejas Centro–Material
    activas hoy según:
        – Históricos ME2L
        – Pedidos Pendientes
        – ZLO12 (centros válidos hoy)
    """

    sql = f"""
    WITH
    hist_me2l AS (
      SELECT DISTINCT
        CAST(Material AS INT64) AS Material,
        CAST(Centro AS STRING)  AS Centro
      FROM `{PROJECT_ID}.granier_staging.stg_ME2L`
      WHERE Proveedor = {proveedor_id}
        AND Centro IN ('0801','2801','2901','4601','1009')
        AND Material IS NOT NULL
    ),
    pendientes AS (
      SELECT DISTINCT
        CAST(Material AS INT64) AS Material,
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
      SELECT
        CAST(Centro AS STRING)  AS Centro,
        CAST(Material AS INT64) AS Material
      FROM `{PROJECT_ID}.granier_staging.stg_ZLO12`
      WHERE Fecha = CURRENT_DATE()
        AND Centro IN ('0801','2801','2901','4601','1009')
    )
    SELECT DISTINCT
      z.Centro,
      z.Material
    FROM zlo z
    JOIN union_all u USING (Material)
    """

    return client.query(sql).to_dataframe()



# ============================================================
# 2) CARGA PRINCIPAL PARA EL PIPELINE
# ============================================================

def cargar_datos_reales(
    proveedor_id: int,
    consumo_extra_pct: float = 0.0
):
    print("📥 get datos BQ...")

    client = bigquery.Client()

    # --------------------------------------------------------
    # 2.1) Obtener pares CM dinámicos
    # --------------------------------------------------------
    print(f"   → Generando filtro CM dinámico para proveedor {proveedor_id}...")
    df_cm = generar_filtro_cm(client, proveedor_id)

    if df_cm.empty:
        raise ValueError(f"No se encontraron materiales para proveedor {proveedor_id}")

    pares = [(row["Centro"], row["Material"]) for _, row in df_cm.iterrows()]


    # --------------------------------------------------------
    # 2.2) Stock + Consumo solo para esos pares
    # --------------------------------------------------------
    print("   → Cargando stock/consumo...")

    cm_structs = ",\n        ".join(
        [f"STRUCT('{c}' AS Centro, {m} AS Material)" for c, m in pares]
    )

    sql_sc = f"""
    WITH cm AS (
      SELECT * FROM UNNEST([
        {cm_structs}
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

    if df_sc.empty:
        raise ValueError("No hay datos ZLO12 hoy para los materiales detectados.")


    # --------------------------------------------------------
    # 2.3) Ajuste del consumo
    # --------------------------------------------------------
    consumo_diario = {
        (row["Centro"], row["Material"]):
            row["Consumo_medio_diario"] * (1 + consumo_extra_pct)
        for _, row in df_sc.iterrows()
    }


    # --------------------------------------------------------
    # 2.4) Stock fábrica (centro 1004)
    # --------------------------------------------------------
    print("   → Cargando stock de fábrica...")

    sql_fabr = f"""
    SELECT 
      Material,
      Libre_util_centro + Cantidad_pdte_entrada - Cantidad_pdte_salida AS Stock
    FROM `{PROJECT_ID}.granier_staging.stg_ZLO12`
    WHERE Fecha = CURRENT_DATE()
      AND Centro = "1004"
    """

    df_fabr = client.query(sql_fabr).to_dataframe()
    stock_fabrica = {row["Material"]: row["Stock"] for _, row in df_fabr.iterrows()}


    # --------------------------------------------------------
    # 2.5) Parámetros de fabricación (Tbl_Produccion_Parametros)
    # --------------------------------------------------------
    print("   → Cargando parámetros de producción...")

    sql_param = f"""
    SELECT
      Material,
      Puesto_de_trabajo,
      Un_Hora,
      ` StockObj_Dias`,
      ` Grupo_de_Fabr`
    FROM `{PROJECT_ID}.granier_logistica.Tbl_Produccion_Parmetros`
    """

    df_param = client.query(sql_param).to_dataframe()
    df_param.columns = [c.strip() for c in df_param.columns]  # Fix espacios

    puesto_trabajo = {
        row["Material"]: row["Puesto_de_trabajo"]
        for _, row in df_param.iterrows()
    }
    grupo_de_fabr = {
        row["Material"]: row["Grupo_de_Fabr"]
        for _, row in df_param.iterrows()
    }


    # --------------------------------------------------------
    # 2.6) Objetivo/seguridad por centro (Master_Logistica)
    # --------------------------------------------------------
    print("   → Cargando stock objetivo/seguridad por centro...")

    sql_obj = f"""
    SELECT 
      centro AS Centro,
      stock_objetivo AS Dias_Stock_Objetivo,
      stock_seguridad AS Dias_Stock_Seguridad
    FROM `{PROJECT_ID}.granier_logistica.Master_Logistica`
    WHERE centro_suministrador = "1004"
    """

    df_obj = client.query(sql_obj).to_dataframe()

    dias_obj_por_centro = {row["Centro"]: int(row["Dias_Stock_Objetivo"] or 0) for _, row in df_obj.iterrows()}
    dias_seg_por_centro = {row["Centro"]: int(row["Dias_Stock_Seguridad"] or 0) for _, row in df_obj.iterrows()}

    dias_stock_objetivo = {
        (row["Centro"], row["Material"]): dias_obj_por_centro.get(row["Centro"], 0)
        for _, row in df_sc.iterrows()
    }

    dias_stock_seguridad = {
        (row["Centro"], row["Material"]): dias_seg_por_centro.get(row["Centro"], 0)
        for _, row in df_sc.iterrows()
    }


    # --------------------------------------------------------
    # DEVOLVER TODO LO NECESARIO PARA EL PIPELINE
    # --------------------------------------------------------
    print("✅ Datos cargados correctamente.")

    return {
        "stock_inicial_centros": df_sc[["Centro","Material","Stock"]],
        "consumo_diario": consumo_diario,
        "cantidad_min_fabricacion": {
            row["Material"]: row["cantidad_min_fabricacion"]
            for _, row in df_sc.iterrows()
        },
        "stock_fabrica": stock_fabrica,
        "dias_stock_objetivo": dias_stock_objetivo,
        "dias_stock_seguridad": dias_stock_seguridad,
        "puesto_trabajo": puesto_trabajo,
        "grupo_de_fabr": grupo_de_fabr,
    }
