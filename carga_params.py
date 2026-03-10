from google.cloud import bigquery

PROJECT_ID = "business-intelligence-444511"

def generar_filtro_cm(client, proveedor_id: int, centro: str | None = None):
    """
    Devuelve un DataFrame con todas las parejas Centro–Material
    activas hoy para un proveedor.

    Si se informa `centro`, filtra solo ese centro.
    Si `centro` viene vacío o None, usa el conjunto completo
    de centros del flujo.
    """

    centros_default = ["0801", "2801", "2901", "4601", "1009"]

    # Construimos la condición SQL de centro dinámicamente
    if centro is not None and str(centro).strip() != "":
        centro = str(centro).strip()
        filtro_centros_sql = f"= '{centro}'"
    else:
        centros_sql = ",".join([f"'{c}'" for c in centros_default])
        filtro_centros_sql = f"IN ({centros_sql})"

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
        CAST(Material AS INT64) AS Material,
        CAST(Centro AS STRING)  AS Centro
      FROM `{PROJECT_ID}.granier_staging.stg_ME2L`
      WHERE Proveedor = {proveedor_id}
        AND Centro {filtro_centros_sql}
        AND Material IS NOT NULL
        AND Material != 30226
    ),
    pendientes AS (
      SELECT DISTINCT
        CAST(Material AS INT64) AS Material,
        CAST(Centro AS STRING)  AS Centro
      FROM `{PROJECT_ID}.granier_logistica.Tbl_Pedidos_Pendientes`
      WHERE Proveedor = {proveedor_id}
        AND Centro {filtro_centros_sql}
        AND Material IS NOT NULL
        AND Material != 30226
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
        AND Centro {filtro_centros_sql}
        AND Material != 30226
    )
    SELECT DISTINCT
      u.Centro,
      u.Material
    FROM union_all u
    JOIN zlo z USING (Centro, Material)
    LEFT JOIN excluidos e USING (Centro, Material)
    WHERE e.Material IS NULL
    """

    return client.query(sql).to_dataframe()

def cargar_datos_reales(
    proveedor_id: int,
    consumo_extra_pct: float = 0.0,
    centro: str | None = None
):
    print("📥 get datos BQ (V2, ZLO12 curado)...")

    client = bigquery.Client()

    # --------------------------------------------------------
    # 1) Obtener pares Centro-Material dinámicos (filtro por proveedor)
    # --------------------------------------------------------
    from carga_params import generar_filtro_cm  # si ya está importado arriba, puedes quitar esta línea

    print(f"   → Generando filtro CM dinámico para proveedor {proveedor_id}...")
    df_cm = generar_filtro_cm(client, proveedor_id, centro=centro)

    if df_cm.empty:
        raise ValueError(f"No se encontraron materiales para proveedor {proveedor_id}")

    pares = [(row["Centro"], row["Material"]) for _, row in df_cm.iterrows()]

    # Construimos los STRUCT para filtrar solo esos pares en las vistas
    cm_structs = ",\n        ".join(
        [f"STRUCT('{c}' AS Centro, {m} AS Material)" for c, m in pares]
    )

    # --------------------------------------------------------
    # 2) Stock desde ZLO12_STREAMING_CURRENT
    # --------------------------------------------------------
    print("   → Cargando stock actual desde ZLO12_STREAMING_CURRENT...")

    sql_stock = f"""
    WITH cm AS (
      SELECT * FROM UNNEST([
        {cm_structs}
      ])
    )
    SELECT
      z.Centro,
      z.Material,
      SAFE_CAST(z.Libre_util_centro AS FLOAT64) AS Stock_Actual,
      SAFE_CAST(z.Libre_utilizacion AS FLOAT64) AS Stock
    FROM `{PROJECT_ID}.granier_logistica.ZLO12_STREAMING_CURRENT` z
    JOIN cm USING (Centro, Material)
    """

    df_stock = client.query(sql_stock).to_dataframe()

    if df_stock.empty:
        raise ValueError("No hay datos en ZLO12_STREAMING_CURRENT para los materiales detectados.")

    # --------------------------------------------------------
    # 3) CMD + cantidad mínima fabricación desde v_ZLO12_curado
    # --------------------------------------------------------
    print("   → Cargando CMD + parámetros desde v_ZLO12_curado...")

    sql_cmd = f"""
    WITH cm AS (
      SELECT * FROM UNNEST([
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

    # --------------------------------------------------------
    # 4) Merge stock + CMD
    # --------------------------------------------------------
    df_sc = df_stock.merge(
        df_cmd,
        on=["Centro", "Material"],
        how="left"
    )

    # Si faltara CMD_Ajustado_Final, caemos a CMD_SAP
    df_sc["CMD_Ajustado_Final"] = df_sc["CMD_Ajustado_Final"].fillna(df_sc["CMD_SAP"])

    # Si faltara también CMD_SAP, lo dejamos en 0
    df_sc["CMD_SAP"] = df_sc["CMD_SAP"].fillna(0)
    df_sc["CMD_Ajustado_Final"] = df_sc["CMD_Ajustado_Final"].fillna(0)

    # cantidad mínima fabricación
    df_sc["cantidad_min_fabricacion"] = df_sc["cantidad_min_fabricacion"].fillna(0)

    # --------------------------------------------------------
    # 5) Ajuste del consumo (CMD_Ajustado_Final * (1 + % extra))
    # --------------------------------------------------------
    consumo_diario = {
        (row["Centro"], row["Material"]):
            float(row["CMD_Ajustado_Final"]) * (1.0 + float(consumo_extra_pct))
        for _, row in df_sc.iterrows()
    }

    # CMD “puro” SAP
    cmd_sap = {
        (row["Centro"], row["Material"]): float(row["CMD_SAP"])
        for _, row in df_sc.iterrows()
    }

    cantidad_min_fabricacion = {
        row["Material"]: float(row["cantidad_min_fabricacion"])
        for _, row in df_sc.iterrows()
    }


    # --------------------------------------------------------
    # 4) Stock fábrica (centro 1004) también desde v_ZLO12_curado
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # 5) Parámetros de producción (Tbl_Produccion_Parmetros)
    # --------------------------------------------------------
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
    # ⚠️ Importante: solo strip de nombre de columna, NO tocar los backticks del SQL
    df_param.columns = [c.strip() for c in df_param.columns]

    puesto_trabajo = {
        row["Material"]: row["Puesto_de_trabajo"]
        for _, row in df_param.iterrows()
    }
    grupo_de_fabr = {
        row["Material"]: row["Grupo_de_Fabr"]
        for _, row in df_param.iterrows()
    }

    # --------------------------------------------------------
    # 6) Objetivo / seguridad por centro (Master_Logistica)
    # --------------------------------------------------------
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

    dias_obj_por_centro = {
        row["Centro"]: int(row["Dias_Stock_Objetivo"] or 0)
        for _, row in df_obj.iterrows()
    }
    dias_seg_por_centro = {
        row["Centro"]: int(row["Dias_Stock_Seguridad"] or 0)
        for _, row in df_obj.iterrows()
    }

    # Mapear días objetivo/seguridad a cada par Centro-Material
    dias_stock_objetivo = {
        (row["Centro"], row["Material"]): dias_obj_por_centro.get(row["Centro"], 0)
        for _, row in df_sc.iterrows()
    }
    dias_stock_seguridad = {
        (row["Centro"], row["Material"]): dias_seg_por_centro.get(row["Centro"], 0)
        for _, row in df_sc.iterrows()
    }

    # --------------------------------------------------------
    # 7) Mínimos logísticos (Master_Pedidos_Min)
    # --------------------------------------------------------
    print("   → Cargando mínimos logísticos (Master_Pedidos_Min)...")

    sql_minimos = f"""
    SELECT
      CAST(Material AS INT64) AS Material,
      Cajas_capa,
      Cajas_palet
    FROM `{PROJECT_ID}.granier_logistica.Master_Pedidos_Min`
    """

    df_minimos = client.query(sql_minimos).to_dataframe()

    # --------------------------------------------------------
    # 8) Rotación CAP / PAL (Stock_Dias_CAP_PAL) – solo para pares CM del proveedor
    # --------------------------------------------------------
    print("   → Cargando rotación CAP/PAL (Stock_Dias_CAP_PAL)...")

    sql_rotacion = f"""
    WITH cm AS (
      SELECT * FROM UNNEST([
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

    # --------------------------------------------------------
    # 9) Precios estándar PMV por Centro–Material
    # --------------------------------------------------------
    print("   → Cargando Precio_estandar_PMV desde Master_Articulos_Centro...")
    
    sql_precio = f"""
    SELECT 
        CAST(Material AS INT64) AS Material,
        CAST(Centro AS STRING) AS Centro,
        Precio_estandar_PMV
    FROM `{PROJECT_ID}.granier_maestros.Master_Articulos_Centro`
    """
    
    df_precio = client.query(sql_precio).to_dataframe()
    
    # Diccionario: (Centro, Material) → precio
    precio_pmv = {
        (row["Centro"], row["Material"]): float(row["Precio_estandar_PMV"])
        for _, row in df_precio.iterrows()
        if row["Precio_estandar_PMV"] is not None
    }


    # --------------------------------------------------------
    # 10) Devolver todo lo necesario para el pipeline V2
    # --------------------------------------------------------
    print("✅ Datos cargados correctamente (V2).")

    return {
        # Base para forecast y simulación
        "stock_inicial_centros": df_sc[["Centro", "Material", "Stock", "Stock_Actual"]],
        "consumo_diario": consumo_diario,
        "cmd_sap": cmd_sap,
        "stock_fabrica": stock_fabrica,
        "cantidad_min_fabricacion": cantidad_min_fabricacion,
    
        # Días objetivo / seguridad
        "dias_stock_objetivo": dias_stock_objetivo,
        "dias_stock_seguridad": dias_stock_seguridad,
        "dias_seg_por_centro": dias_seg_por_centro,
    
        # Parámetros de producción
        "puesto_trabajo": puesto_trabajo,
        "grupo_de_fabr": grupo_de_fabr,
    
        # Novedades V2
        "minimos_logisticos": df_minimos,
        "rotacion": df_rotacion,
        "precio_pmv": precio_pmv,
    }
