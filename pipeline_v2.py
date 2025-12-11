# ============================================================
# pipeline_v2.py – Forecast + Pedidos con lógica avanzada
# ============================================================

from datetime import datetime, date, timedelta
from google.cloud import bigquery
import pandas as pd
import math

from carga_params import cargar_datos_reales
from funciones_stg import (
    forecast_stock_centros,
    generar_pedidos_centros_desde_forecastV2,
    ajustar_pedidos_por_restricciones_logisticas_v2,
    ajustar_pedidos_a_minimos_logisticos_v2
)

PROJECT_ID = "business-intelligence-444511"
DATASET    = "granier_logistica"


# ============================================================
#                      PIPELINE V2
# ============================================================

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
    df_rotacion     = datos["rotacion"]     # vista CAP/PAL
    cmd_sap_dict    = datos["cmd_sap"]             # nuevo


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

        # 3.1) Generar nuevos pedidos para estas roturas
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

        # ======================================================
        # 3.2) AJUSTE LOGÍSTICO V2 – RESTRICCIÓN SEMANAL
        # ======================================================
        nuevos = ajustar_pedidos_por_restricciones_logisticas_v2(
            pedidos_df=nuevos,
            dia_corte=2,
            consumo_diario=consumo_diario,
            dias_stock_objetivo=dias_obj
        )

        # ======================================================
        # 3.3) AJUSTE CAP / PALET SEGÚN ROTACIÓN
        # ======================================================
        nuevos = ajustar_pedidos_a_minimos_logisticos_v2(
            nuevos,
            df_minimos=df_minimos,
            df_rotacion=df_rotacion
        )

        # Aplicamos la cantidad ajustada final
        if "Cantidad_ajustada" in nuevos.columns:
            nuevos["Cantidad"] = nuevos["Cantidad_ajustada"]
            nuevos.drop(columns=["Cantidad_ajustada"], inplace=True)

        # Añadir al histórico de pedidos
        pedidos_total = pd.concat([pedidos_total, nuevos], ignore_index=True)

        # Añadir al calendario real de entregas
        entregas_totales = pd.concat(
            [entregas_totales, nuevos[["Centro","Material","Fecha_Entrega","Cantidad"]]],
            ignore_index=True
        )


    # ========================================================
    # 4) FORECAST FINAL (una vez todas las entregas están fijadas)
    # ========================================================
    forecast_final = forecast_stock_centros(
        stock_inicial=stock_centros,
        consumo_diario=consumo_diario,
        entregas_planificadas=pedidos_total[["Centro","Material","Fecha_Entrega","Cantidad"]],
        dias_forecast=60,
        clamp_cero=True
    )


    # ========================================================
    # 5.0) PRE-CÁLCULO NECESARIO PARA ENRIQUECIMIENTO
    # stock_llegada_dict debe existir ANTES de la persistencia
    # ========================================================

    forecast_aux = forecast_final.copy()
    forecast_aux["Fecha"] = pd.to_datetime(forecast_aux["Fecha"]).dt.date

    col_stock_forecast = "Stock_estimado"

    # Construir llave para stock llegado
    forecast_aux["key"] = list(zip(
        forecast_aux["Centro"],
        forecast_aux["Material"],
        forecast_aux["Fecha"]
    ))

    stock_llegada_dict = {
        k: float(stock_value)
        for k, stock_value in zip(
            forecast_aux["key"],
            forecast_aux[col_stock_forecast]
        )
    }

    
    # ========================================================
    # 5) PERSISTENCIA EN BIGQUERY
    # ========================================================
    print("\n💾 Guardando resultados en BigQuery...")

    # ---- FORECAST ----
    out_f = forecast_final.copy()
    out_f["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")
    out_f["Material"] = pd.to_numeric(out_f["Material"], errors="coerce").astype("Int64")
    out_f = out_f.merge(df_art, on="Material", how="left")

    client.load_table_from_dataframe(
        out_f,
        f"{PROJECT_ID}.{DATASET}.Forecast_StockCentros_Proveedor{proveedor_id}_V2",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()


    # ---- PEDIDOS ----
    if not pedidos_total.empty:

        # =============================
        # 5.1 Preparación inicial out_p
        # =============================
        out_p = pedidos_total.copy()
        out_p["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")

        # Semana ISO
        iso = pd.to_datetime(out_p["Fecha_Entrega"]).dt.isocalendar()
        out_p["Ano"] = iso["year"].astype(int)
        out_p["Semana_Num"] = iso["week"].astype(int)
        out_p["Semana_ISO"] = (
            out_p["Ano"].astype(str)
            + "-W"
            + out_p["Semana_Num"].astype(str).str.zfill(2)
        )

        # Limpieza tipos
        out_p["Fecha_Entrega"] = pd.to_datetime(out_p["Fecha_Entrega"]).dt.date
        out_p["Material"] = pd.to_numeric(out_p["Material"], errors="coerce").astype("Int64")

        # Info SAP
        out_p = out_p.merge(df_art, on="Material", how="left")

        # =============================
        # 5.2 Enriquecimiento FINAL para BigQuery
        #    (CMD_Sap, CMD_Ajustado, Dias_stock_llegada)
        # =============================

        # CMD_Sap
        def _cmd_sap(row):
            return cmd_sap_dict.get((row["Centro"], row["Material"]), None)

        # CMD_Ajustado
        def _cmd_ajustado(row):
            return consumo_diario.get((row["Centro"], row["Material"]), None)

        out_p["CMD_Sap"] = out_p.apply(_cmd_sap, axis=1)
        out_p["CMD_Ajustado"] = out_p.apply(_cmd_ajustado, axis=1)

        # Dias_stock_llegada
        def _dias_stock_llegada(row):
            key = (row["Centro"], row["Material"], row["Fecha_Entrega"])
            stock_llegada = stock_llegada_dict.get(key, None)
            cmd_adj = row["CMD_Ajustado"]

            if stock_llegada is None or cmd_adj in (None, 0):
                return None

            return stock_llegada / cmd_adj

        out_p["Dias_stock_llegada"] = out_p.apply(_dias_stock_llegada, axis=1)

        # =============================
        # 5.3 Subida a BigQuery (ya enriquecido)
        # =============================
        client.load_table_from_dataframe(
            out_p,
            f"{PROJECT_ID}.{DATASET}.Tbl_Pedidos_Simples_Proveedor{proveedor_id}_V2",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        ).result()

    else:
        out_p = pd.DataFrame(columns=pedidos_total.columns)

    print(">>> OUT_P SHAPE:", out_p.shape)
    print(">>> OUT_P COLUMNS:", out_p.columns.tolist())
    print(out_p.head(5))

    # ========================================================
    # 6) FORMATO JSON PARA endpoint /planificar_v2
    # ========================================================

    print("📤 Preparando JSON de salida...")

    # --------------------------------------------------------
    # 6.1 Formato final para JSON
    # --------------------------------------------------------
    out_p_json = out_p.copy()

    # Asegurar formato fecha JSON-friendly
    out_p_json["Fecha_Entrega"] = pd.to_datetime(out_p_json["Fecha_Entrega"]).dt.date
    out_p_json["Fecha_Rotura"] = pd.to_datetime(out_p_json["Fecha_Rotura"]).dt.date

    # Orden final
    out_p_json = out_p_json.sort_values(
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
        "Dias_stock_llegada"
    ]

    pedidos_json = out_p_json[columnas_sheets].to_dict(orient="records")

    # --------------------------------------------------------
    # 6.2 RETURN FINAL DEL ENDPOINT
    # --------------------------------------------------------
    return {
        "proveedor": proveedor_id,
        "pedidos_rows": len(out_p),
        "forecast_rows": len(forecast_aux),
        "pedidos": pedidos_json
    }
