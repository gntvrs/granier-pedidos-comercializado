# ============================================================
# pipeline_v2.py – Forecast + Pedidos con lógica avanzada
# ============================================================

from datetime import date, timedelta
from google.cloud import bigquery
import pandas as pd
from typing import Optional
from carga_params import cargar_datos_reales
from funciones_stg import (
    forecast_stock_centros,
    generar_pedidos_centros_desde_forecastV2,
    ajustar_pedidos_por_restricciones_logisticas_v2,
    ajustar_pedidos_a_minimos_logisticos_v2
)

PROJECT_ID = "business-intelligence-444511"
DATASET = "granier_logistica"


# ============================================================
#                      PIPELINE V2
# ============================================================
def ejecutar_pipeline_v2(
    proveedor_id: int | None,
    consumo_extra_pct: float,
    centro: str | None = None,
    fecha_corte: str | None = None
):

    print("🚀 Ejecutando PIPELINE V2...")
    client = bigquery.Client()

    print(f"📥 Cargando datos reales + parámetros... centro={centro}, fecha_corte={fecha_corte}")

    datos = cargar_datos_reales(
        proveedor_id=proveedor_id,
        consumo_extra_pct=consumo_extra_pct,
        centro=centro,
        fecha_corte=fecha_corte
    )

    stock_centros = datos["stock_inicial_centros"]
    consumo_diario = datos["consumo_diario"]
    dias_obj = datos["dias_stock_objetivo"]
    dias_seg = datos["dias_stock_seguridad"]
    dias_seg_por_centro = datos["dias_seg_por_centro"]
    cantidad_min_fabricacion = datos["cantidad_min_fabricacion"]
    df_minimos = datos["minimos_logisticos"]
    df_rotacion = datos["rotacion"]
    cmd_sap_dict = datos["cmd_sap"]
    df_cm_proveedor = datos["cm_proveedor"]

    print(f"✔ Centros-material: {len(stock_centros)}")
    print(f"✔ Registros rotación CAP/PAL: {len(df_rotacion)}")

    if fecha_corte:
        hoy = date.today()
        fecha_corte_dt = pd.to_datetime(fecha_corte).date()
        dias_hasta_corte = max((fecha_corte_dt - hoy).days, 0)

        if centro is not None and str(centro).strip() != "":
            stock_seguridad_centro = int(dias_seg_por_centro.get(str(centro).strip(), 0))
        else:
            stock_seguridad_centro = max(dias_seg_por_centro.values()) if dias_seg_por_centro else 0

        dias_forecast = dias_hasta_corte + stock_seguridad_centro
        fecha_limite_global = fecha_corte_dt + timedelta(days=stock_seguridad_centro)

        print(
            f"📅 fecha_corte={fecha_corte_dt} | hoy={hoy} | "
            f"dias_hasta_corte={dias_hasta_corte} | "
            f"stock_seguridad_centro={stock_seguridad_centro} | "
            f"dias_forecast={dias_forecast} | "
            f"fecha_limite_global={fecha_limite_global}"
        )
    else:
        fecha_corte_dt = None
        stock_seguridad_centro = None
        dias_forecast = 60
        fecha_limite_global = None
        print(f"📅 Sin fecha_corte informada. Usando dias_forecast={dias_forecast}")

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

    MAX_ITERS = 50

    entregas_totales = pd.DataFrame(columns=["Centro", "Material", "Fecha_Entrega", "Cantidad"])
    pedidos_total = pd.DataFrame(columns=[
        "Centro", "Material", "Fecha_Carga", "Fecha_Entrega",
        "Cantidad", "Fecha_Rotura", "Comentarios"
    ])

    # El resto del motor sigue trabajando a grano Centro-Material
    stock_centros_forecast = stock_centros[["Centro", "Material", "Stock", "Stock_Actual"]].copy()

    for i in range(MAX_ITERS):

        print(f"\n🔁 Iteración {i}")

        forecast = forecast_stock_centros(
            stock_inicial=stock_centros_forecast,
            consumo_diario=consumo_diario,
            entregas_planificadas=entregas_totales,
            dias_forecast=dias_forecast,
            clamp_cero=True
        )

        forecast["Fecha"] = pd.to_datetime(forecast["Fecha"]).dt.date

        if fecha_limite_global is not None:
            forecast_para_pedidos = forecast[forecast["Fecha"] <= fecha_limite_global].copy()
            roturas = forecast_para_pedidos[forecast_para_pedidos["Rotura"] == True].copy()
            print(f"   → Forecast filtrado hasta {fecha_limite_global}: {len(forecast_para_pedidos)} filas")
        else:
            forecast_para_pedidos = forecast.copy()
            roturas = forecast[forecast["Rotura"] == True].copy()

        if roturas.empty:
            if fecha_limite_global is not None:
                print(f"✅ SIN ROTURAS hasta fecha límite {fecha_limite_global} → Pipeline estable")
            else:
                print("✅ SIN ROTURAS → Pipeline estable")
            break

        print(f"   → Roturas detectadas: {len(roturas)}")

        nuevos = generar_pedidos_centros_desde_forecastV2(
            forecast_df=forecast_para_pedidos,
            consumo_diario=consumo_diario,
            dias_stock_seguridad=dias_seg,
            dias_stock_objetivo=dias_obj
        )

        if nuevos.empty:
            print("⚠ Roturas detectadas pero NO se generan pedidos. Rompo.")
            break

        print("   → Nuevos pedidos generados")

        nuevos = ajustar_pedidos_por_restricciones_logisticas_v2(
            pedidos_df=nuevos,
            dia_corte=2,
            consumo_diario=consumo_diario,
            dias_stock_objetivo=dias_obj
        )

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

    forecast_final = forecast_stock_centros(
        stock_inicial=stock_centros_forecast,
        consumo_diario=consumo_diario,
        entregas_planificadas=pedidos_total[["Centro", "Material", "Fecha_Entrega", "Cantidad"]],
        dias_forecast=dias_forecast,
        clamp_cero=True
    )

    forecast_aux = forecast_final.copy()
    forecast_aux["Fecha"] = pd.to_datetime(forecast_aux["Fecha"]).dt.date
    col_stock_forecast = "Stock_estimado"
    forecast_aux["key"] = list(zip(forecast_aux["Centro"], forecast_aux["Material"], forecast_aux["Fecha"]))

    stock_llegada_dict = {
        k: float(stock_value)
        for k, stock_value in zip(forecast_aux["key"], forecast_aux[col_stock_forecast])
    }

    print("\n💾 Guardando resultados en BigQuery...")

    out_f = forecast_final.copy()
    out_f["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")
    out_f["Material"] = pd.to_numeric(out_f["Material"], errors="coerce").astype("Int64")
    out_f = out_f.merge(df_art, on="Material", how="left")
    out_f = out_f.merge(df_cm_proveedor, on=["Centro", "Material"], how="left")

    proveedor_suffix = "ALL" if proveedor_id is None else str(proveedor_id)

    client.load_table_from_dataframe(
        out_f,
        f"{PROJECT_ID}.{DATASET}.Forecast_StockCentros_Proveedor{proveedor_suffix}_V2",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()

    if not pedidos_total.empty:
        out_p = pedidos_total.copy()
        out_p["Fecha_ejecucion"] = pd.Timestamp.now(tz="Europe/Madrid")

        iso = pd.to_datetime(out_p["Fecha_Entrega"]).dt.isocalendar()
        out_p["Ano"] = iso["year"].astype(int)
        out_p["Semana_Num"] = iso["week"].astype(int)
        out_p["Semana_ISO"] = out_p["Ano"].astype(str) + "-W" + out_p["Semana_Num"].astype(str).str.zfill(2)

        out_p["Fecha_Entrega"] = pd.to_datetime(out_p["Fecha_Entrega"]).dt.date
        out_p["Material"] = pd.to_numeric(out_p["Material"], errors="coerce").astype("Int64")

        out_p = out_p.merge(df_art, on="Material", how="left")
        out_p = out_p.merge(df_cm_proveedor, on=["Centro", "Material"], how="left")

        df_stock_info = stock_centros[["Centro", "Material", "Stock", "Stock_Actual", "Proveedor"]].copy()
        df_stock_info["Material"] = pd.to_numeric(df_stock_info["Material"], errors="coerce").astype("Int64")

        out_p = out_p.merge(df_stock_info, on=["Centro", "Material", "Proveedor"], how="left")

        def _cmd_sap(row):
            return cmd_sap_dict.get((row["Centro"], row["Material"]), None)

        def _cmd_ajustado(row):
            return consumo_diario.get((row["Centro"], row["Material"]), None)

        out_p["CMD_Sap"] = out_p.apply(_cmd_sap, axis=1)
        out_p["CMD_Ajustado"] = out_p.apply(_cmd_ajustado, axis=1)

        def _dias_stock_llegada(row):
            key = (row["Centro"], row["Material"], row["Fecha_Entrega"])
            stock_llegada = stock_llegada_dict.get(key, None)
            cmd_adj = row["CMD_Ajustado"]
            if stock_llegada is None or cmd_adj in (None, 0):
                return None
            return stock_llegada / cmd_adj

        out_p["Dias_stock_llegada"] = out_p.apply(_dias_stock_llegada, axis=1)

        client.load_table_from_dataframe(
            out_p,
            f"{PROJECT_ID}.{DATASET}.Tbl_Pedidos_Simples_V2",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        ).result()
    else:
        out_p = pd.DataFrame(columns=pedidos_total.columns)

    print(">>> OUT_P SHAPE:", out_p.shape)
    print(">>> OUT_P COLUMNS:", out_p.columns.tolist())
    print(out_p.head(5))

    print("📤 Preparando JSON de salida...")

    out_p_json = out_p.copy()

    if "Fecha_Entrega" in out_p_json.columns:
        out_p_json["Fecha_Entrega"] = pd.to_datetime(out_p_json["Fecha_Entrega"]).dt.date
    if "Fecha_Rotura" in out_p_json.columns:
        out_p_json["Fecha_Rotura"] = pd.to_datetime(out_p_json["Fecha_Rotura"]).dt.date

    if not out_p_json.empty:
        out_p_json = out_p_json.sort_values(
            by=["Ano", "Semana_Num", "Centro", "Codigo_Base"],
            ascending=[True, True, True, True]
        ).reset_index(drop=True)

    columnas_sheets = [
        "Ano",
        "Semana_Num",
        "Semana_ISO",
        "Centro",
        "Proveedor",
        "Codigo_Base",
        "Material",
        "Texto_breve",
        "N_antiguo_material",
        "Fecha_Rotura",
        "Fecha_Entrega",
        "Cantidad",
        "Stock",
        "Stock_Actual",
        "CMD_Sap",
        "CMD_Ajustado",
        "Dias_stock_llegada"
    ]

    columnas_presentes = [c for c in columnas_sheets if c in out_p_json.columns]
    pedidos_json = out_p_json[columnas_presentes].to_dict(orient="records")

    return {
        "proveedor": proveedor_id,
        "centro": centro,
        "fecha_corte": fecha_corte,
        "stock_seguridad_centro": stock_seguridad_centro,
        "dias_forecast": dias_forecast,
        "pedidos_rows": len(out_p),
        "forecast_rows": len(forecast_aux),
        "pedidos": pedidos_json
    }
