from google.cloud import bigquery

PROJECT_ID = "business-intelligence-444511"


def _sql_proveedor_filter(alias: str, proveedor_id: int | None) -> str:
    if proveedor_id is None:
        return ""
    return f"AND {alias}.Proveedor = {int(proveedor_id)}"



def generar_filtro_cm(client, proveedor_id: int | None = None, centro: str | None = None):
    """
    Devuelve un DataFrame con las parejas Centro–Material activas hoy.

    - Si `proveedor_id` viene informado, filtra el universo por proveedor.
    - Si `proveedor_id` viene vacío o None, construye el universo completo
      y resuelve un proveedor por Material usando el proveedor más reciente
      de stg_ME2L según Fecha_Pedido.
    - Si `centro` viene informado, filtra solo ese centro.
    """

    centros_default = ["0801", "2801", "2901", "4601", "1009"]

    if centro is not None and str(centro).strip() != "":
        centro = str(centro).strip()
        filtro_centros_sql = f"= '{centro}'"
    else:
        centros_sql = ",".join([f"'{c}'" for c in centros_default])
        filtro_centros_sql = f"IN ({centros_sql})"

    filtro_proveedor_me2l = _sql_proveedor_filter("m", proveedor_id)
    filtro_proveedor_pend = _sql_proveedor_filter("p", proveedor_id)

    proveedor_select = str(int(proveedor_id)) if proveedor_id is not None else "lp.Proveedor"
    proveedor_null_filter = "" if proveedor_id is not None else "AND lp.Proveedor IS NOT NULL"

    sql = f"""
    WITH
    excluidos AS (
      SELECT 
        CAST(Centro AS STRING)  AS Centro,
        CAST(Material AS INT64) AS Material
      FROM `{PROJECT_ID}.granier_logistica.Tbl_excluidos_flujo_comercializado`
    ),
    hist_me2l AS (
      SELECT DISTINCT
        CAST(m.Material AS INT64) AS Material,
        CAST(m.Centro AS STRING)  AS Centro
      FROM `{PROJECT_ID}.granier_staging.stg_ME2L` m
      WHERE 1=1
        {filtro_proveedor_me2l}
        AND m.Centro {filtro_centros_sql}
        AND m.Material IS NOT NULL
        AND m.Material != 30226
    ),
    pendientes AS (
      SELECT DISTINCT
        CAST(p.Material AS INT64) AS Material,
        CAST(p.Centro AS STRING)  AS Centro
      FROM `{PROJECT_ID}.granier_logistica.Tbl_Pedidos_Pendientes` p
      WHERE 1=1
        {filtro_proveedor_pend}
        AND p.Centro {filtro_centros_sql}
        AND p.Material IS NOT NULL
        AND p.Material != 30226
    ),
    union_all AS (
      SELECT * FROM hist_me2l
      UNION DISTINCT
      SELECT * FROM pendientes
    ),
    zlo AS (
      SELECT DISTINCT
        CAST(Centro AS STRING)  AS Centro,
        CAST(Material AS INT64) AS Material
      FROM `{PROJECT_ID}.granier_staging.stg_ZLO12`
      WHERE Fecha = CURRENT_DATE()
        AND Centro {filtro_centros_sql}
        AND Material != 30226
    ),
    latest_provider AS (
      SELECT
        CAST(Material AS INT64) AS Material,
        CAST(Proveedor AS INT64) AS Proveedor
      FROM (
        SELECT
          Material,
          Proveedor,
          Fecha_Pedido,
          Pedido,
          Posicion,
          ROW_NUMBER() OVER (
            PARTITION BY Material
            ORDER BY Fecha_Pedido DESC, Pedido DESC, Posicion DESC
          ) AS rn
        FROM `{PROJECT_ID}.granier_staging.stg_ME2L`
        WHERE Material IS NOT NULL
          AND Proveedor IS NOT NULL
          AND Material != 30226
      )
      WHERE rn = 1
    )
    SELECT DISTINCT
      u.Centro,
      u.Material,
      {proveedor_select} AS Proveedor
    FROM union_all u
    JOIN zlo z USING (Centro, Material)
    LEFT JOIN excluidos e USING (Centro, Material)
    LEFT JOIN latest_provider lp USING (Material)
    WHERE e.Material IS NULL
      {proveedor_null_filter}
    ORDER BY u.Centro, u.Material
    """

    return client.query(sql).to_dataframe()



def cargar_datos_reales(
    proveedor_id: int | None = None,
    consumo_extra_pct: float = 0.0,
    centro: str | None = None,
    fecha_corte: str | None = None
):
    print("📥 get datos BQ (V2, ZLO12 curado)...")

    client = bigquery.Client()

    print(f"   → Generando filtro CM dinámico para proveedor {proveedor_id}...")
    df_cm = generar_filtro_cm(client, proveedor_id, centro=centro)

    if df_cm.empty:
        proveedor_txt = "TODOS" if proveedor_id is None else str(proveedor_id)
        raise ValueError(f"No se encontraron materiales para proveedor {proveedor_txt}")

    pares = [(row["Centro"], row["Material"], row["Proveedor"]) for _, row in df_cm.iterrows()]

    cm_structs = ",\n        ".join(
        [f"STRUCT('{c}' AS Centro, {m} AS Material, {int(p)} AS Proveedor)" for c, m, p in pares]
    )

    print("   → Cargando stock (ZLO12 + pendientes - roturas)...")

    fecha_entrega_filter = f"AND p.Fecha_de_entrega <= DATE('{fecha_corte}')" if fecha_corte else ""
    fecha_rotura_filter = f"AND r.Fecha_Rotura <= DATE('{fecha_corte}')" if fecha_corte else ""

    sql_stock = f"""
    WITH cm AS (
      SELECT * FROM UNNEST([
        {cm_structs}
      ])
    ),
    pendientes AS (
      SELECT
        cm.Centro,
        cm.Material,
        SUM(IFNULL(SAFE_CAST(p.Cantidad AS FLOAT64), 0)) AS Cantidad_Pendiente_Entrada
      FROM cm
      LEFT JOIN `{PROJECT_ID}.granier_logistica.Tbl_Pedidos_Pendientes` p
        ON CAST(p.Centro AS STRING) = cm.Centro
       AND CAST(p.Material AS INT64) = cm.Material
       AND CAST(p.Proveedor AS INT64) = cm.Proveedor
      WHERE 1=1
        {fecha_entrega_filter}
      GROUP BY 1, 2
    ),
    roturas AS (
      SELECT
        cm.Centro,
        cm.Material,
        SUM(IFNULL(r.Cantidad_Rotura, 0)) AS Cantidad_Rotura
      FROM cm
      LEFT JOIN `{PROJECT_ID}.granier_logistica.Tbl_Roturas_Proveedor` r
        ON CAST(r.Centro AS STRING) = cm.Centro
       AND CAST(r.Material AS INT64) = cm.Material
       AND CAST(r.Proveedor AS INT64) = cm.Proveedor
       AND r.Estado = 'ABIERTA'
      WHERE 1=1
        {fecha_rotura_filter}
      GROUP BY 1, 2
    )
    SELECT
      z.Centro,
      z.Material,
      IFNULL(SAFE_CAST(z.Libre_util_centro AS FLOAT64), 0) AS Stock_Actual,
      IFNULL(SAFE_CAST(z.Libre_util_centro AS FLOAT64), 0)
        - IFNULL(SAFE_CAST(z.Cantidad_pdte_salida AS FLOAT64), 0)
        + IFNULL(p.Cantidad_Pendiente_Entrada, 0)
        - IFNULL(r.Cantidad_Rotura, 0) AS Stock
    FROM `{PROJECT_ID}.granier_logistica.ZLO12_STREAMING_CURRENT` z
    JOIN cm
      ON CAST(z.Centro AS STRING) = cm.Centro
     AND CAST(z.Material AS INT64) = cm.Material
    LEFT JOIN pendientes p
      ON CAST(z.Centro AS STRING) = p.Centro
     AND CAST(z.Material AS INT64) = p.Material
    LEFT JOIN roturas r
      ON CAST(z.Centro AS STRING) = r.Centro
     AND CAST(z.Material AS INT64) = r.Material
    """

    df_stock = client.query(sql_stock).to_dataframe()

    if df_stock.empty:
        raise ValueError("No hay datos en ZLO12_STREAMING_CURRENT para los materiales detectados.")

    print("   → Cargando CMD + parámetros desde v_ZLO12_curado...")

    sql_cmd = f"""
    WITH cm AS (
      SELECT DISTINCT Centro, Material FROM UNNEST([
        {cm_structs}
      ])
    )
    SELECT
      z.Centro,
      z.Material,
      z.CMD_SAP,
      z.CMD_Ajustado_Final,
      z.cantidad_min_fabricacion
    FROM `{PROJECT_ID}.granier_logistica.v_ZLO12_curado` z
    JOIN cm USING (Centro, Material)
    """

    df_cmd = client.query(sql_cmd).to_dataframe()

    if df_cmd.empty:
        raise ValueError("No hay datos de CMD en v_ZLO12_curado para los materiales detectados.")

    df_sc = df_stock.merge(df_cmd, on=["Centro", "Material"], how="left")
    df_sc = df_sc.merge(df_cm[["Centro", "Material", "Proveedor"]], on=["Centro", "Material"], how="left")

    df_sc["CMD_Ajustado_Final"] = df_sc["CMD_Ajustado_Final"].fillna(df_sc["CMD_SAP"])
    df_sc["CMD_SAP"] = df_sc["CMD_SAP"].fillna(0)
    df_sc["CMD_Ajustado_Final"] = df_sc["CMD_Ajustado_Final"].fillna(0)
    df_sc["cantidad_min_fabricacion"] = df_sc["cantidad_min_fabricacion"].fillna(0)

    consumo_diario = {
        (row["Centro"], row["Material"]): float(row["CMD_Ajustado_Final"]) * (1.0 + float(consumo_extra_pct))
        for _, row in df_sc.iterrows()
    }

    cmd_sap = {
        (row["Centro"], row["Material"]): float(row["CMD_SAP"])
        for _, row in df_sc.iterrows()
    }

    cantidad_min_fabricacion = {
        row["Material"]: float(row["cantidad_min_fabricacion"])
        for _, row in df_sc.iterrows()
    }

    print("   → Cargando stock de fábrica (Centro 1004)...")

    sql_fabr = f"""
    SELECT
      Material,
      Stock
    FROM `{PROJECT_ID}.granier_logistica.v_ZLO12_curado`
    WHERE Centro = "1004"
    """

    df_fabr = client.query(sql_fabr).to_dataframe()
    stock_fabrica = {row["Material"]: float(row["Stock"]) for _, row in df_fabr.iterrows()}

    print("   → Cargando parámetros de producción (Tbl_Produccion_Parmetros)...")

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
    df_param.columns = [c.strip() for c in df_param.columns]

    puesto_trabajo = {row["Material"]: row["Puesto_de_trabajo"] for _, row in df_param.iterrows()}
    grupo_de_fabr = {row["Material"]: row["Grupo_de_Fabr"] for _, row in df_param.iterrows()}

    print("   → Cargando stock objetivo/seguridad por centro (Master_Logistica)...")

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

    print("   → Cargando mínimos logísticos (Master_Pedidos_Min)...")

    sql_minimos = f"""
    SELECT
      CAST(Material AS INT64) AS Material,
      Cajas_capa,
      Cajas_palet
    FROM `{PROJECT_ID}.granier_logistica.Master_Pedidos_Min`
    """

    df_minimos = client.query(sql_minimos).to_dataframe()

    print("   → Cargando rotación CAP/PAL (Stock_Dias_CAP_PAL)...")

    sql_rotacion = f"""
    WITH cm AS (
      SELECT DISTINCT Centro, Material FROM UNNEST([
        {cm_structs}
      ])
    )
    SELECT
      r.Centro,
      r.Material,
      r.cajas_cap,
      r.cajas_pal,
      r.dias_stock_cap,
      r.dias_stock_pal
    FROM `{PROJECT_ID}.granier_logistica.Stock_Dias_CAP_PAL` r
    JOIN cm USING (Centro, Material)
    """

    df_rotacion = client.query(sql_rotacion).to_dataframe()

    print("   → Cargando Precio_estandar_PMV desde Master_Articulos_Centro...")

    sql_precio = f"""
    SELECT 
        CAST(Material AS INT64) AS Material,
        CAST(Centro AS STRING) AS Centro,
        Precio_estandar_PMV
    FROM `{PROJECT_ID}.granier_maestros.Master_Articulos_Centro`
    """

    df_precio = client.query(sql_precio).to_dataframe()

    precio_pmv = {
        (row["Centro"], row["Material"]): float(row["Precio_estandar_PMV"])
        for _, row in df_precio.iterrows()
        if row["Precio_estandar_PMV"] is not None
    }

    print("✅ Datos cargados correctamente (V2).")

    return {
        "stock_inicial_centros": df_sc[["Centro", "Material", "Stock", "Stock_Actual", "Proveedor"]],
        "consumo_diario": consumo_diario,
        "cmd_sap": cmd_sap,
        "stock_fabrica": stock_fabrica,
        "cantidad_min_fabricacion": cantidad_min_fabricacion,
        "dias_stock_objetivo": dias_stock_objetivo,
        "dias_stock_seguridad": dias_stock_seguridad,
        "dias_seg_por_centro": dias_seg_por_centro,
        "puesto_trabajo": puesto_trabajo,
        "grupo_de_fabr": grupo_de_fabr,
        "minimos_logisticos": df_minimos,
        "rotacion": df_rotacion,
        "precio_pmv": precio_pmv,
        "cm_proveedor": df_cm[["Centro", "Material", "Proveedor"]],
    }
