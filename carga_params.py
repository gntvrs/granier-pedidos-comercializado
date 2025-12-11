from google.cloud import bigquery

PROJECT_ID = "business-intelligence-444511"

def generar_filtro_cm(client, proveedor_id: int):
    """
    Devuelve un DataFrame con todas las parejas Centro–Material
    activas hoy, excluyendo las referencias marcadas en
    granier_logistica.Tbl_excluidos_flujo_comercializado
    """

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
        AND Centro IN ('0801','2801','2901','4601','1009')
        AND Material IS NOT NULL
        AND Material != 30226
    ),
    pendientes AS (
      SELECT DISTINCT
        CAST(Material AS INT64) AS Material,
        CAST(Centro AS STRING)  AS Centro
      FROM `{PROJECT_ID}.granier_logistica.Tbl_Pedidos_Pendientes`
      WHERE Proveedor = {proveedor_id}
        AND Centro IN ('0801','2801','2901','4601','1009')
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
        AND Centro IN ('0801','2801','2901','4601','1009')
        AND Material != 30226
    )
    SELECT DISTINCT
      u.Centro,
      u.Material
    FROM union_all u
    JOIN zlo z USING (Centro, Material)
    LEFT JOIN excluidos e USING (Centro, Material)
    WHERE e.Material IS NULL   -- ⬅️ EXCLUIR REFERENCIAS
    """

    return client.query(sql).to_dataframe()

def cargar_datos_reales(
    proveedor_id: int,
    consumo_extra_pct: float = 0.0
):
    print("📥 get datos BQ (V2, ZLO12 curado)...")

    client = bigquery.Client()

    # --------------------------------------------------------
    # 1) Obtener pares Centro-Material dinámicos (filtro por proveedor)
    # --------------------------------------------------------
    from carga_params import generar_filtro_cm  # si ya está importado arriba, puedes quitar esta línea

    print(f"   → Generando filtro CM dinámico para proveedor {proveedor_id}...")
    df_cm = generar_filtro_cm(client, proveedor_id)

    if df_cm.empty:
        raise ValueError(f"No se encontraron materiales para proveedor {proveedor_id}")

    pares = [(row["Centro"], row["Material"]) for _, row in df_cm.iterrows()]

    # Construimos los STRUCT para filtrar solo esos pares en las vistas
    cm_structs = ",\n        ".join(
        [f"STRUCT('{c}' AS Centro, {m} AS Material)" for c, m in pares]
    )

    # --------------------------------------------------------
    # 2) Stock + CMD desde v_ZLO12_curado
    # --------------------------------------------------------
    print("   → Cargando stock + CMD desde v_ZLO12_curado...")
    
    sql_sc = f"""
    WITH cm AS (
      SELECT * FROM UNNEST([
        {cm_structs}
      ])
    )
    SELECT
      z.Centro,
      z.Material,
      z.Stock,
      z.CMD_SAP,
      z.CMD_Ajustado_Final,
      z.cantidad_min_fabricacion
    FROM `{PROJECT_ID}.granier_logistica.v_ZLO12_curado` z
    JOIN cm USING (Centro, Material)
    """
    
    df_sc = client.query(sql_sc).to_dataframe()
    
    if df_sc.empty:
        raise ValueError("No hay datos en v_ZLO12_curado para los materiales detectados.")
    
    # 3) Ajuste del consumo (CMD_Ajustado_Final * (1 + % extra))
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
        "stock_inicial_centros": df_sc[["Centro", "Material", "Stock"]],
        "consumo_diario": consumo_diario,      # = CMD_Ajustado (ya con % extra)
        "cmd_sap": cmd_sap,                    # nuevo
        "stock_fabrica": stock_fabrica,
        "cantidad_min_fabricacion": cantidad_min_fabricacion,
    
        # Días objetivo / seguridad
        "dias_stock_objetivo": dias_stock_objetivo,
        "dias_stock_seguridad": dias_stock_seguridad,
    
        # Parámetros de producción
        "puesto_trabajo": puesto_trabajo,
        "grupo_de_fabr": grupo_de_fabr,
    
        # Novedades V2
        "minimos_logisticos": df_minimos,
        "rotacion": df_rotacion,
        "precio_pmv": precio_pmv,
    }
