# =============================================
# pipeline.py – Pipeline completo para endpoint
# =============================================

from carga_params import cargar_datos_reales
from funciones_stg import (
    forecast_stock_centros,
    generar_pedidos_centros_desde_forecastV2,
    ajustar_pedidos_a_minimos_logisticos,
    ajustar_pedidos_por_restricciones_logisticas
)

from google.cloud import bigquery
import pandas as pd
import math
from datetime import datetime

PROJECT_ID = "business-intelligence-444511"
DATASET    = "granier_logistica"


# ==================================================================================
# ============================== PIPELINE PRINCIPAL ================================
# ==================================================================================

def ejecutar_pipeline(proveedor_id: int, consumo_extra_pct: float):

    print("🚀 Ejecutando pipeline...")
    client = bigquery.Client()

    # -------------------------------------------------------------------------
    # 1) CARGA DINÁMICA DE DATOS (proveedor + consumo_extra_pct)
    # -------------------------------------------------------------------------
    print("🔄 Cargando datos reales desde BQ...")
    datos = cargar_datos_reales(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct
    )

    stock_centros   = datos["stock_inicial_centros"]
    consumo_diario  = datos["consumo_diario"]
    dias_obj        = datos["dias_stock_objetivo"]
    dias_seg        = datos["dias_stock_seguridad"]
    cantidad_min_fabricacion = datos["cantidad_min_fabricacion"]

    print(f"   → Centros-material cargados: {len(stock_centros)}")
    print(f"   → Registros consumo: {len(consumo_diario)}")


    # -------------------------------------------------------------------------
    # 2) CARGA ARTÍCULOS (JOIN final)
    # -------------------------------------------------------------------------
    print("📦 Cargando información artículos SAP...")

    sql_art = f"""
    SELECT 
      CAST(Material AS INT64) AS Material,
      CAST(Codigo_Base AS INT64) AS Codigo_Base,
      N_antiguo_material,
      Texto_breve
    FROM `{PROJECT_ID}.granier_maestros.Master_ArticulosSAP`
    """

    df_art = client.query(sql_art).to_dataframe()
    df_art["Material"] = pd.to_numeric(df_art["Material"], errors="coerce").astype("Int64")


    # -------------------------------------------------------------------------
    # 3) MÍNIMOS LOGÍSTICOS
    # -------------------------------------------------------------------------
    print("📦 Cargando mínimos logísticos...")

    sql_minimos = f"""
    SELECT 
      CAST(Material AS INT64) AS Material,
      Cajas_capa,
      Cajas_palet
    FROM `{PROJECT_ID}.granier_logistica.Master_Pedidos_Min`
    """

    df_minimos = client.query(sql_minimos).to_dataframe()


    # -------------------------------------------------------------------------
    # 4) BUCLE PRINCIPAL DEL PIPELINE (FORECAST + PEDIDOS)
    # -------------------------------------------------------------------------
    print("🔁 Iniciando bucle iterativo...")

    entregas_totales = pd.DataFrame(columns=["Centro","Material","Fecha_Entrega","Cantidad"])
    pedidos_total    = pd.DataFrame(columns=[
        "Centro","Material","Fecha_Carga","Fecha_Entrega","Cantidad","Fecha_Rotura","Comentarios"
    ])

    MAX_ITERS = 50

    for i in range(MAX_ITERS):

        forecast = forecast_stock_centros(
            stock_inicial=stock_centros,
            consumo_diario=consumo_diario,
            entregas_planificadas=entregas_totales,
            dias_forecast=60,
            clamp_cero=True,
        )

        print(f"\n🔍 Iteración {i}")
        roturas = forecast[forecast["Rotura"] == True]

        if roturas.empty:
            print("✅ Sin roturas. Pipeline estable.")
            break

        print(f"   → Roturas detectadas: {len(roturas)}")

        nuevos = generar_pedidos_centros_desde_forecastV2(
            forecast_df=forecast,
            consumo_diario=consumo_diario,
            dias_stock_seguridad=dias_seg,
            dias_stock_objetivo=dias_obj,
        )
        
        # --- IMPORTANTE: este bloque estaba en el original ---
        if not nuevos.empty:
            print(f"🧾 Iter {i} – primeros pedidos generados:")
            print(nuevos[['Centro', 'Material', 'Fecha_Carga', 'Fecha_Entrega', 'Fecha_Rotura']].head(5))
        
        # --- IMPORTANTE: este break estaba DETRÁS del bloque anterior ---
        if nuevos.empty:
            print("⚠ Hay roturas pero no se han generado nuevos pedidos. Salgo por seguridad.")
            break
        
        # --- Ajuste sobre los nuevos ---
        nuevos = ajustar_pedidos_a_minimos_logisticos(nuevos, df_minimos)

        nuevos["Cantidad"] = nuevos["Cantidad_ajustada"]
        nuevos.drop(columns=["Cantidad_ajustada"], inplace=True)

        # Acumular pedidos
        pedidos_total = pd.concat([pedidos_total, nuevos], ignore_index=True)
        entregas_totales = pd.concat([entregas_totales, nuevos[["Centro","Material","Fecha_Entrega","Cantidad"]]], ignore_index=True)


    # -------------------------------------------------------------------------
    # 5) AJUSTE POR RESTRICCIONES LOGÍSTICAS
    # -------------------------------------------------------------------------
    print("\n📦 Ajustes logísticos finales...")

    if not pedidos_total.empty:
        pedidos_total = ajustar_pedidos_por_restricciones_logisticas(
            pedidos_total,
            dia_corte=2   # miércoles
        )


    # -------------------------------------------------------------------------
    # 6) FORECAST FINAL
    # -------------------------------------------------------------------------
    forecast_final = forecast_stock_centros(
        stock_inicial=stock_centros,
        consumo_diario=consumo_diario,
        entregas_planificadas=pedidos_total[["Centro","Material","Fecha_Entrega","Cantidad"]],
        dias_forecast=60,
        clamp_cero=True,
    )


    # -------------------------------------------------------------------------
    # 7) PERSISTENCIA EN BIGQUERY
    # -------------------------------------------------------------------------
    print("\n💾 Persistiendo resultados en BigQuery...")

    # ===== FORECAST =====
    forecast_out = forecast_final.copy()
    forecast_out["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")
    forecast_out["Material"] = pd.to_numeric(forecast_out["Material"], errors="coerce").astype("Int64")
    forecast_out = forecast_out.merge(df_art, on="Material", how="left")

    client.load_table_from_dataframe(
        forecast_out,
        f"{PROJECT_ID}.{DATASET}.Forecast_StockCentros_Proveedor{proveedor_id}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()

    # ===== PEDIDOS =====
    if not pedidos_total.empty:

        pedidos_out = pedidos_total.copy()
        pedidos_out["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")

        iso = pd.to_datetime(pedidos_out["Fecha_Entrega"]).dt.isocalendar()
        pedidos_out["Ano_ISO"]    = iso["year"].astype(int)
        pedidos_out["Semana_Num"] = iso["week"].astype(int)
        pedidos_out["Semana_ISO"] = pedidos_out["Ano_ISO"].astype(str) + "-W" + pedidos_out["Semana_Num"].astype(str).str.zfill(2)

        pedidos_out["Material"] = pd.to_numeric(pedidos_out["Material"], errors="coerce").astype("Int64")
        pedidos_out = pedidos_out.merge(df_art, on="Material", how="left")

        client.load_table_from_dataframe(
            pedidos_out,
            f"{PROJECT_ID}.{DATASET}.Tbl_Pedidos_Simples_Proveedor{proveedor_id}",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()

    else:
        pedidos_out = pd.DataFrame(columns=pedidos_total.columns)


    # -------------------------------------------------------------------------
    # 8) JSON DE SALIDA PARA EL ENDPOINT
    # -------------------------------------------------------------------------
    print("📤 Preparando JSON de salida...")

    # ===============================================
    # ENRIQUECER PEDIDOS PARA EL FRONT (Google Sheets)
    # ===============================================
    
    # Año
    pedidos_out["Ano"] = pd.to_datetime(pedidos_out["Fecha_Entrega"]).dt.year
    
    # Semana ISO ya está calculada → extraemos Semana_Num si no está
    if "Semana_Num" not in pedidos_out.columns:
        iso = pd.to_datetime(pedidos_out["Fecha_Entrega"]).dt.isocalendar()
        pedidos_out["Semana_Num"] = iso["week"].astype(int)
    
    # Filas finales para Google Sheets
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
    ]
    
    pedidos_json = pedidos_out[columnas_sheets].to_dict(orient="records")


    return {
        "proveedor": proveedor_id,
        "consumo_extra_pct": consumo_extra_pct,
        "forecast_rows": len(forecast_out),
        "pedidos_rows": len(pedidos_out),
        "pedidos": pedidos_json
    }

