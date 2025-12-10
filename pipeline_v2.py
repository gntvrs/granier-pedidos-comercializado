# ============================================================
# pipeline_v2.py – Forecast + Pedidos con lógica avanzada
# ============================================================

from datetime import datetime, date, timedelta
from google.cloud import bigquery
import pandas as pd
import numpy as np

from carga_params import cargar_datos_reales
from funciones_stg import (
    forecast_stock_centros,
    generar_pedidos_centros_desde_forecastV2,
    ajustar_pedidos_por_restricciones_logisticas_v2,
    ajustar_pedidos_a_minimos_logisticos_v2,
)

PROJECT_ID = "business-intelligence-444511"
DATASET    = "granier_logistica"


def ejecutar_pipeline_v2(proveedor_id: int, consumo_extra_pct: float):

    print("🚀 Ejecutando PIPELINE V2...")
    client = bigquery.Client()

    # ========================================================
    # 1) CARGA COMPLETA DE PARÁMETROS DESDE carga_params
    # ========================================================
    print("📥 Cargando datos reales + parámetros...")

    datos = cargar_datos_reales(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct
    )

    stock_centros   = datos["stock_inicial_centros"]
    consumo_diario  = datos["consumo_diario"]
    dias_obj        = datos["dias_stock_objetivo"]
    dias_seg        = datos["dias_stock_seguridad"]
    cantidad_min_fabricacion = datos["cantidad_min_fabricacion"]
    df_minimos      = datos["minimos_logisticos"]
    df_rotacion     = datos["rotacion"]
    cmd_sap_dict    = datos["cmd_sap"]
    precio_pmv      = datos["precio_pmv"]

    print(f"✔ Centros-material: {len(stock_centros)}")
    print(f"✔ Registros rotación CAP/PAL: {len(df_rotacion)}")

    # ========================================================
    # 2) CARGA DE ARTÍCULOS (para enriquecer pedidos)
    # ========================================================
    sql_art = f"""
    SELECT 
      CAST(Material AS INT64) AS Material,
      CAST(Codigo_Base AS INT64) AS Codigo_Base,
      Texto_breve,
      N_antiguo_material
    FROM `{PROJECT_ID}.granier_maestros.Master_ArticulosSAP`
    """

    df_art = client.query(sql_art).to_dataframe()
    df_art["Material"] = pd.to_numeric(df_art["Material"], errors="coerce").astype("Int64")

    # ========================================================
    # 3) LOOP ITERATIVO – Forecast → Pedidos → Ajustes
    # ========================================================
    MAX_ITERS = 50

    entregas_totales = pd.DataFrame(columns=["Centro", "Material", "Fecha_Entrega", "Cantidad"])
    pedidos_total    = pd.DataFrame(columns=[
        "Centro", "Material", "Fecha_Carga", "Fecha_Entrega",
        "Cantidad", "Fecha_Rotura", "Comentarios"
    ])

    for i in range(MAX_ITERS):

        print(f"\n🔁 Iteración {i}")

        forecast = forecast_stock_centros(
            stock_inicial=stock_centros,
            consumo_diario=consumo_diario,
            entregas_planificadas=entregas_totales,
            dias_forecast=60,
            clamp_cero=True
        )

        roturas = forecast[forecast["Rotura"] == True]

        if roturas.empty:
            print("✅ SIN ROTURAS → Pipeline estable")
            break

        print(f"   → Roturas detectadas: {len(roturas)}")

        nuevos = generar_pedidos_centros_desde_forecastV2(
            forecast_df=forecast,
            consumo_diario=consumo_diario,
            dias_stock_seguridad=dias_seg,
            dias_stock_objetivo=dias_obj
        )

        if nuevos.empty:
            print("⚠ Roturas detectadas pero NO se generan pedidos. Rompo.")
            break

        print("   → Nuevos pedidos generados")

        # 3.2) Restricción logística (adelantos)
        nuevos = ajustar_pedidos_por_restricciones_logisticas_v2(
            pedidos_df=nuevos,
            dia_corte=2,
            consumo_diario=consumo_diario,
            dias_stock_objetivo=dias_obj
        )

        # 3.3) CAP / PALET (solo comentamos PALET)
        nuevos = ajustar_pedidos_a_minimos_logisticos_v2(
            nuevos,
            df_minimos=df_minimos,
            df_rotacion=df_rotacion
        )

        if "Cantidad_ajustada" in nuevos.columns:
            nuevos["Cantidad"] = nuevos["Cantidad_ajustada"]
            nuevos.drop(columns=["Cantidad_ajustada"], inplace=True)

        pedidos_total = pd.concat([pedidos_total, nuevos], ignore_index=True)
        entregas_totales = pd.concat(
            [entregas_totales, nuevos[["Centro", "Material", "Fecha_Entrega", "Cantidad"]]],
            ignore_index=True
        )

    # ========================================================
    # 4) FORECAST FINAL
    # ========================================================
    forecast_final = forecast_stock_centros(
        stock_inicial=stock_centros,
        consumo_diario=consumo_diario,
        entregas_planificadas=pedidos_total[["Centro", "Material", "Fecha_Entrega", "Cantidad"]],
        dias_forecast=60,
        clamp_cero=True
    )

    # ========================================================
    # 5) PERSISTENCIA EN BIGQUERY (FORECAST + PEDIDOS RAW)
    # ========================================================
    print("\n💾 Guardando resultados en BigQuery...")

    # FORECAST
    out_f = forecast_final.copy()
    out_f["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")
    out_f["Material"] = pd.to_numeric(out_f["Material"], errors="coerce").astype("Int64")
    out_f = out_f.merge(df_art, on="Material", how="left")

    client.load_table_from_dataframe(
        out_f,
        f"{PROJECT_ID}.{DATASET}.Forecast_StockCentros_Proveedor{proveedor_id}_V2",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()

    # PEDIDOS RAW → tabla BQ simple
    if not pedidos_total.empty:
        out_p_bq = pedidos_total.copy()
        out_p_bq["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")

        iso_bq = pd.to_datetime(out_p_bq["Fecha_Entrega"]).dt.isocalendar()
        out_p_bq["Ano"] = iso_bq["year"].astype(int)
        out_p_bq["Semana_Num"] = iso_bq["week"].astype(int)
        out_p_bq["Semana_ISO"] = (
            out_p_bq["Ano"].astype(str) + "-W" + out_p_bq["Semana_Num"].astype(str).str.zfill(2)
        )

        out_p_bq["Material"] = pd.to_numeric(out_p_bq["Material"], errors="coerce").astype("Int64")
        out_p_bq = out_p_bq.merge(df_art, on="Material", how="left")

        client.load_table_from_dataframe(
            out_p_bq,
            f"{PROJECT_ID}.{DATASET}.Tbl_Pedidos_Simples_Proveedor{proveedor_id}_V2",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        ).result()
    else:
        out_p_bq = pd.DataFrame(columns=pedidos_total.columns)

    # ========================================================
    # 6) FORMATO JSON PARA endpoint /planificar_v2
    # ========================================================
    print("📤 Preparando JSON de salida...")

    # 6.1 – Normalizar forecast para lookup de stock de llegada
    forecast_aux = forecast_final.copy()
    forecast_aux["Fecha"] = pd.to_datetime(forecast_aux["Fecha"]).dt.date
    col_stock_forecast = "Stock_estimado"

    forecast_aux["key"] = list(zip(
        forecast_aux["Centro"],
        forecast_aux["Material"],
        forecast_aux["Fecha"]
    ))

    stock_llegada_dict = {
        k: float(v)
        for k, v in zip(forecast_aux["key"], forecast_aux[col_stock_forecast])
    }

    # 6.2 – Construir out_p enriquecido SOLO para front / JSON
    if not pedidos_total.empty:
        out_p = pedidos_total.copy()
        out_p["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")

        iso = pd.to_datetime(out_p["Fecha_Entrega"]).dt.isocalendar()
        out_p["Ano"] = iso["year"].astype(int)
        out_p["Semana_Num"] = iso["week"].astype(int)
        out_p["Semana_ISO"] = (
            out_p["Ano"].astype(str) + "-W" + out_p["Semana_Num"].astype(str).str.zfill(2)
        )

        out_p["Fecha_Entrega"] = pd.to_datetime(out_p["Fecha_Entrega"]).dt.date
        out_p["Material"] = pd.to_numeric(out_p["Material"], errors="coerce").astype("Int64")

        out_p = out_p.merge(df_art, on="Material", how="left")

        # CMD_Sap y CMD_Ajustado
        def _cmd_sap(row):
            return cmd_sap_dict.get((row["Centro"], row["Material"]), None)

        def _cmd_ajustado(row):
            return consumo_diario.get((row["Centro"], row["Material"]), None)

        out_p["CMD_Sap"] = out_p.apply(_cmd_sap, axis=1)
        out_p["CMD_Ajustado"] = out_p.apply(_cmd_ajustado, axis=1)

        # Precio estándar PMV
        def _precio_pmv(row):
            return precio_pmv.get((row["Centro"], row["Material"]), None)

        out_p["Precio_estandar_PMV"] = out_p.apply(_precio_pmv, axis=1)

        # Valor total
        def _valor(row):
            precio = row["Precio_estandar_PMV"]
            cantidad = row["Cantidad"]
            if precio is None or cantidad is None:
                return None
            return float(precio) * float(cantidad)

        out_p["Valor_total"] = out_p.apply(_valor, axis=1)

        # Días de stock en la llegada
        def _dias_stock_llegada(row):
            key = (row["Centro"], row["Material"], row["Fecha_Entrega"])
            stock_llegada = stock_llegada_dict.get(key, None)
            cmd_adj = row["CMD_Ajustado"]

            if stock_llegada is None or cmd_adj in (None, 0):
                return None
            return float(stock_llegada) / float(cmd_adj)

        out_p["Dias_stock_llegada"] = out_p.apply(_dias_stock_llegada, axis=1)

    else:
        out_p = pd.DataFrame(columns=pedidos_total.columns)

    # 6.3 – Orden final Año / Semana / Centro / Codigo_Base
    if not out_p.empty:
        out_p["Ano"] = pd.to_numeric(out_p["Ano"], errors="coerce")
        out_p["Semana_Num"] = pd.to_numeric(out_p["Semana_Num"], errors="coerce")

        out_p = out_p.sort_values(
            by=["Ano", "Semana_Num", "Centro", "Codigo_Base"],
            ascending=[True, True, True, True]
        ).reset_index(drop=True)

    columnas_sheets = [
        "Ano",
        "Semana_Num",
        "Centro",
        "Codigo_Base",
        "Material",
        "Texto_breve",
        "N_antiguo_material",
        "Fecha_Rotura",
        "Fecha_Entrega",
        "Cantidad",
        "CMD_Sap",
        "CMD_Ajustado",
        "Dias_stock_llegada",
        "Precio_estandar_PMV",
        "Valor_total",
        "Comentarios",
    ]

    # 6.5 – Limpieza global antes del JSON
    # Quitar columnas auxiliares si quedaran
    cols_basura = [
        "material_x",
        "material_y",
        "cajas_capa",
        "cajas_pal",
        "dias_stock_pal",
    ]
    out_p = out_p.drop(columns=[c for c in cols_basura if c in out_p.columns])

    # Sustituir NaN / ±inf → None
    out_p = out_p.replace({np.nan: None, np.inf: None, -np.inf: None})

    for col in columnas_sheets:
        if col in out_p.columns:
            out_p[col] = out_p[col].apply(
                lambda x: None
                if isinstance(x, float) and (np.isnan(x) if x is not None else False)
                else x
            )
    # --------------------------------------------------------
    # 6.6 — RETURN FORMATO JSON PARA EL ENDPOINT
    # --------------------------------------------------------
    
    # Eliminar completamente cualquier DataFrame del return
    forecast_len = int(len(forecast_final))
    
    pedidos_json = out_p[columnas_sheets].to_dict(orient="records")
    
    return {
        "proveedor": proveedor_id,
        "pedidos_rows": len(out_p),
        "forecast_rows": forecast_len,
        "pedidos": pedidos_json
    }


    
