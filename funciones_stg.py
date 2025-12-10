from datetime import datetime, timedelta, date
import pandas as pd
import math
# ================================

# ================================
# Función auxiliar estándar
# ================================

def print_dfs(**dfs):
    for name, df in dfs.items():
        print(f"\n=== {name.upper()} ===")
        print(df.to_string(index=False) if not df.empty else "⚠️ DataFrame vacío")

# ================================
# Función 1: Generar Pedidos
# ================================

def generar_pedidos_centros_desde_forecast(forecast_df, consumo_diario, dias_stock_seguridad,
                                            dias_stock_objetivo, fechas_transporte):
    pedidos = []

    for (centro, material), grupo in forecast_df.groupby(["Centro", "Material"]):
        consumo = consumo_diario[(centro, material)]
        seg = dias_stock_seguridad[(centro, material)]
        obj = dias_stock_objetivo[(centro, material)]

        # Detectar la primera rotura
        grupo_rotura = grupo[grupo["Rotura"] == True]
        if grupo_rotura.empty:
            continue

        fecha_rotura = grupo_rotura["Fecha"].iloc[0]
        fecha_entrega_objetivo = fecha_rotura - timedelta(days=seg)
        fecha_entrega_min = grupo["Fecha"].min()

        # Buscar fechas de entrega válidas dentro del margen
        opciones_entrega = fechas_transporte[
            (fechas_transporte["Centro"] == centro) &
            (fechas_transporte["Fecha_Entrega"] >= fecha_entrega_min) &
            (fechas_transporte["Fecha_Entrega"] <= fecha_entrega_objetivo)
        ].sort_values("Fecha_Entrega")

        comentario = ""

        if not opciones_entrega.empty:
            # ✅ Entregas dentro del margen: coger la más tardía
            fila_entrega = opciones_entrega.iloc[-1]
        else:
            # ⚠️ No hay entregas en la ventana, buscar la primera tardía
            opciones_entrega = fechas_transporte[
                (fechas_transporte["Centro"] == centro) &
                (fechas_transporte["Fecha_Entrega"] > fecha_entrega_objetivo)
            ].sort_values("Fecha_Entrega")

            if not opciones_entrega.empty:
                fila_entrega = opciones_entrega.iloc[0]
                comentario = "⚠️ Entrega tardía"
            else:
                comentario = "❌ Sin fecha de entrega disponible"
                continue  # saltamos este material-centro

        # Calcular cantidad necesaria solo hasta el objetivo
        cantidad = consumo * (obj - seg)

        # Log detallado
        if cantidad > 0:
            pedidos.append({
                "Centro": centro,
                "Material": material,
                "Fecha_Carga": fila_entrega["Fecha_Carga"],
                "Fecha_Entrega": fila_entrega["Fecha_Entrega"],
                "Cantidad": cantidad,
                "Fecha_Rotura": fecha_rotura,
                "Comentarios": comentario
            })

    return pd.DataFrame(pedidos)
def generar_pedidos_centros_desde_forecastV2(
    forecast_df: pd.DataFrame,
    consumo_diario: dict,
    dias_stock_seguridad: dict,
    dias_stock_objetivo: dict,
) -> pd.DataFrame:
    """
    V2 (regla EPTY):
      - Fecha_Entrega objetivo = Fecha_Rotura - seg
      - Si cae antes del inicio del forecast, entonces Fecha_Entrega = día 0 del forecast
      - Fecha_Carga = Fecha_Entrega (modo simplificado sin calendario)
      - Cantidad = ceil(consumo * (obj - seg)), mínimo 0
    """
    if forecast_df is None or forecast_df.empty:
        return pd.DataFrame(columns=["Centro","Material","Fecha_Carga","Fecha_Entrega","Cantidad","Fecha_Rotura","Comentarios"])

    df = forecast_df.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date

    pedidos = []
    for (centro, material), g in df.groupby(["Centro","Material"]):
        g = g.sort_values("Fecha")
        fecha_inicio = g["Fecha"].min()

        rot = g[g["Rotura"] == True]
        if rot.empty:
            continue

        fecha_rotura = rot["Fecha"].iloc[0]
        cons = float(consumo_diario.get((centro, material), 0.0) or 0.0)
        seg  = int(dias_stock_seguridad.get((centro, material), 0) or 0)
        obj  = int(dias_stock_objetivo.get((centro, material), 0) or 0)

        dias_cubrir = max(0, obj - seg)
        cantidad = math.ceil(max(0.0, cons * dias_cubrir))
        if cantidad <= 0:
            continue

        # Regla: rotura - seg; si antes del inicio → día 0 del forecast
        fecha_entrega_obj = fecha_rotura - timedelta(days=seg)
        fecha_entrega = fecha_entrega_obj if fecha_entrega_obj >= fecha_inicio else fecha_inicio

        pedidos.append({
            "Centro": str(centro),
            "Material": int(material),
            "Fecha_Carga": fecha_entrega,
            "Fecha_Entrega": fecha_entrega,
            "Cantidad": cantidad,
            "Fecha_Rotura": fecha_rotura,
            "Comentarios": ""  # sin calendario -> sin tardíos
        })

    cols = ["Centro","Material","Fecha_Carga","Fecha_Entrega","Cantidad","Fecha_Rotura","Comentarios"]
    return pd.DataFrame(pedidos, columns=cols)

def ajustar_pedidos_a_fecha_trigger_desde_forecast(
    pedidos_df,
    forecast_df,
    consumo_diario,
    dias_objetivo,
    dias_seguridad
):

    out = []
    for _, r in pedidos_df.iterrows():
        centro = r["Centro"]; material = r["Material"]; f_ent = pd.to_datetime(r["Fecha_Entrega"]).date()
        cons = consumo_diario.get((centro, material), 0.0)
        obj_dias = dias_objetivo.get((centro, material), 0)
        seg_dias = dias_seguridad.get((centro, material), 0)

        # target = cubrir (obj - seg) días, pero saca en unidades
        objetivo_unidades = max(0.0, (obj_dias - seg_dias) * cons)

        # Stock estimado ese día (usa el Stock CLAMPED del forecast, no el raw)
        frow = forecast_df[(forecast_df["Centro"]==centro) &
                           (forecast_df["Material"]==material) &
                           (forecast_df["Fecha"]==pd.to_datetime(f_ent))]
        if not frow.empty:
            stock_dia = float(frow.iloc[0]["Stock"])  # OJO: columna Stock "clamped"
        else:
            # si no hay forecast para esa fecha → reprograma a primera del horizonte, como ya haces
            # y conserva la cantidad original redondeada
            out.append({**r, "Cantidad": math.ceil(float(r["Cantidad"]))})
            continue

        # baseline: nunca por debajo de 0
        baseline = max(0.0, stock_dia)

        # pedir lo que falta para llegar al objetivo (nunca negativo)
        cantidad_ajustada = max(0.0, objetivo_unidades - baseline)

        # redondeo hacia arriba
        cantidad_final = math.ceil(cantidad_ajustada)

        # fallback: si por alguna razón queda 0, usa tu regla (si procede)
        if cantidad_final <= 0 and float(r["Cantidad"] or 0) > 0:
            cantidad_final = math.ceil(float(r["Cantidad"]))

        r2 = r.copy()
        r2["Cantidad"] = cantidad_final
        out.append(r2)

    ajustados = pd.DataFrame(out).reindex(columns=pedidos_df.columns)
    return ajustados


def generar_ordenes_fabricacion(pedidos_df, stock_fabrica, dias_antelacion=2,
                                 cantidad_min_fabricacion=None, horizonte_dias=15):
    if pedidos_df.empty or "Material" not in pedidos_df.columns:
        print("\u26a0\ufe0f No se generan órdenes: DataFrame de pedidos vacío o mal formado.")
        return pd.DataFrame()

    pedidos_df = pedidos_df.sort_values(by=["Material", "Fecha_Carga"])
    ordenes = []

    materiales = pedidos_df["Material"].unique()

    for material in materiales:
        pedidos_material = pedidos_df[pedidos_df["Material"] == material]
        if pedidos_material.empty:
            continue

        fechas_carga = pedidos_material["Fecha_Carga"].sort_values().unique()
        stock_actual = stock_fabrica.get(material, 0)
        cantidad_minima = cantidad_min_fabricacion.get(material, 1)

        for fecha_carga in fechas_carga:
            pedidos_en_fecha = pedidos_material[pedidos_material["Fecha_Carga"] == fecha_carga]
            demanda_en_fecha = pedidos_en_fecha["Cantidad"].sum()

            stock_actual -= demanda_en_fecha

            if stock_actual < 0:
                fecha_orden = fecha_carga - timedelta(days=dias_antelacion)
                cantidad_fabricar = max(0, -stock_actual)
                cantidad_fabricar = math.ceil(cantidad_fabricar / cantidad_minima) * cantidad_minima

                id_orden = f"ORD-{material}-{fecha_orden.strftime('%Y%m%d')}"

                ordenes.append({
                    "id_orden": id_orden,
                    "Material": material,
                    "Fecha_Orden": fecha_orden,
                    "Fecha_Carga": fecha_carga,
                    "Cantidad": cantidad_fabricar,
                    "Comentarios": f"Producción para cubrir pedidos hasta {fecha_carga}"
                })

                stock_actual += cantidad_fabricar

        if stock_actual >= 0:
            print(f"\u2705 Stock suficiente para {material}, no se necesitan más órdenes.")

    if not ordenes:
        print("\u26a0\ufe0f No se generan órdenes: ninguna necesidad detectada.")
        return pd.DataFrame()

    return pd.DataFrame(ordenes)


def asignar_entregas_a_centros(pedidos_df, ordenes_df):
    if pedidos_df.empty or ordenes_df.empty:
        print("⚠️ No se asignan entregas: DataFrame de pedidos u órdenes vacío.")
        return pd.DataFrame()

    entregas = []

    pedidos_df = pedidos_df.sort_values(by=["Material", "Fecha_Carga"])
    ordenes_df = ordenes_df.sort_values(by=["Material", "Fecha_Carga"])

    for _, orden in ordenes_df.iterrows():
        material = orden["Material"]
        fecha_carga = orden["Fecha_Carga"]
        cantidad_disponible = orden["Cantidad"]
        id_orden = orden["id_orden"]

        # Filtrar solo pedidos del mismo material y cuya fecha de carga sea >= que la de la orden
        pedidos_material = pedidos_df[
            (pedidos_df["Material"] == material) &
            (pedidos_df["Fecha_Carga"] >= fecha_carga)
        ]

        for _, pedido in pedidos_material.iterrows():
            centro = pedido["Centro"]
            cantidad_pedido = pedido["Cantidad"]
            asignado = min(cantidad_pedido, cantidad_disponible)
            if asignado <= 0:
                continue

            comentario = "Asignación normal" if asignado == cantidad_pedido else "Asignación parcial"

            entregas.append({
                "id_orden": id_orden,
                "Centro": centro,
                "Material": material,
                "Fecha_Carga": pedido["Fecha_Carga"],
                "Fecha_Entrega": pedido["Fecha_Entrega"],  # 👈 ya viene de la lógica previa
                "Cantidad": asignado,
                "Comentarios": comentario
            })

            cantidad_disponible -= asignado
            if cantidad_disponible <= 0:
                break

        # Si sobra algo, va a 0801 con la fecha que ya venía marcada
        if cantidad_disponible > 0:
            entregas.append({
                "id_orden": id_orden,
                "Centro": "0801",
                "Material": material,
                "Fecha_Carga": fecha_carga,
                "Fecha_Entrega": fecha_carga,  # ⚠️ Usando Fecha_Entrega=Fecha_Carga porque es MbCold.
                "Cantidad": cantidad_disponible,
                "Comentarios": "Sobrante asignado por defecto a 0801"
            })

    return pd.DataFrame(entregas)


def forecast_stock_centros(
    stock_inicial: pd.DataFrame,
    consumo_diario: dict,
    entregas_planificadas: pd.DataFrame,
    dias_forecast: int = 45,
    clamp_cero: bool = True,
) -> pd.DataFrame:
    """
    Genera forecast por (Centro, Material) día a día.

    - 'Stock_estimado' queda CLAMPED (>=0) si clamp_cero=True → así no verás negativos en tablas/salidas.
    - Se añade 'Deficit' = max(0, -stock_raw_del_dia) para detectar roturas con precisión.
    - 'Rotura' = Deficit > 0.
    - La dinámica encadena con 'stock_visible' (clamped) si clamp_cero=True, de modo que no se propagan negativos.
    """

    forecast = []

    # === Fecha de inicio del forecast ===
    fechas_disponibles = []
    if "Fecha" in stock_inicial.columns:
        fechas_disponibles.append(pd.to_datetime(stock_inicial["Fecha"], errors="coerce").min())
    if entregas_planificadas is not None and not entregas_planificadas.empty and "Fecha_Entrega" in entregas_planificadas.columns:
        fechas_disponibles.append(pd.to_datetime(entregas_planificadas["Fecha_Entrega"], errors="coerce").min())

    hoy = date.today()
    fecha_disponible = min(fechas_disponibles).date() if fechas_disponibles else hoy
    fecha_inicio = max(fecha_disponible, hoy)

    # === Asegurar tipos/coherencia en entregas ===
    if entregas_planificadas is None or entregas_planificadas.empty:
        entregas_planificadas = pd.DataFrame(columns=["Centro", "Material", "Fecha_Entrega", "Cantidad"])
    else:
        tmp = entregas_planificadas.copy()
        tmp["Fecha_Entrega"] = pd.to_datetime(tmp["Fecha_Entrega"]).dt.date
        entregas_planificadas = tmp

    # Mapa: (centro, material) -> {fecha: cantidad_total_en_fecha}
    entregas_map = {}
    if not entregas_planificadas.empty:
        grp = (
            entregas_planificadas.groupby(["Centro", "Material", "Fecha_Entrega"])["Cantidad"]
            .sum()
            .reset_index()
        )
        for _, r in grp.iterrows():
            cm = (r["Centro"], r["Material"])
            entregas_map.setdefault(cm, {})[r["Fecha_Entrega"]] = float(r["Cantidad"])

    # Asegurar columnas mínimas en stock_inicial
    required_cols = {"Centro", "Material", "Stock"}
    if not required_cols.issubset(set(stock_inicial.columns)):
        raise ValueError(f"stock_inicial debe tener columnas {required_cols}, tiene {set(stock_inicial.columns)}")

    # Iterar por (Centro, Material)
    for _, row in stock_inicial.iterrows():
        centro = row["Centro"]
        material = row["Material"]
        try:
            stock_raw = float(row["Stock"] or 0)
        except Exception:
            stock_raw = 0.0

        cons = float(consumo_diario.get((centro, material), 0.0))
        entregas_cm = entregas_map.get((centro, material), {})

        fecha = fecha_inicio
        for _ in range(dias_forecast):
            # Entradas del día (si las hay)
            if fecha in entregas_cm:
                stock_raw += float(entregas_cm[fecha])

            # Consumo del día
            stock_raw -= cons

            # Déficit y stock visible
            deficit = max(0.0, -stock_raw)
            stock_visible = max(0.0, stock_raw) if clamp_cero else stock_raw

            forecast.append({
                "Fecha": fecha,
                "Centro": centro,
                "Material": material,
                "Stock_estimado": stock_visible,   # <- lo que usarán el resto de funciones
                "Deficit": deficit,
                "Rotura": deficit > 0,
                # opcional para trazas: "Stock_Raw": stock_raw,
            })

            # Encadenar con el clamped para no propagar negativos
            if clamp_cero:
                stock_raw = stock_visible

            fecha += timedelta(days=1)

    return pd.DataFrame(forecast)


def reasignar_pedidos_desde_stock(pedidos_ajustados, stock_forecast, stock_objetivo, centro_principal="0801"):
    entregas_directas = []
    pedidos_restantes = []

    for _, pedido in pedidos_ajustados.iterrows():
        centro = pedido["Centro"]; material = pedido["Material"]

        if centro == centro_principal:
            pedidos_restantes.append(pedido); continue

        fecha_carga = pedido["Fecha_Carga"]; cantidad = float(pedido["Cantidad"])

        stock_0801 = stock_forecast[
            (stock_forecast["Centro"] == centro_principal) &
            (stock_forecast["Material"] == material) &
            (stock_forecast["Fecha"] == fecha_carga)
        ]["Stock_estimado"]

        if stock_0801.empty:
            pedidos_restantes.append(pedido); continue

        stock_actual = float(stock_0801.values[0])
        stock_obj = stock_objetivo.get((centro_principal, material), 0)

        if stock_actual - cantidad >= stock_obj:
            # Entrada al destino
            entregas_directas.append({
                "id_orden": f"TRASPASO-{centro_principal}",
                "Centro": centro,
                "Material": material,
                "Fecha_Carga": fecha_carga,
                "Fecha_Entrega": pedido["Fecha_Entrega"],
                "Cantidad": cantidad,
                "Comentarios": f"Asignación directa desde stock {centro_principal}"
            })
            # Salida de 0801
            entregas_directas.append({
                "id_orden": f"TRASPASO-{centro_principal}",
                "Centro": centro_principal,
                "Material": material,
                "Fecha_Carga": fecha_carga,
                "Fecha_Entrega": pedido["Fecha_Entrega"],
                "Cantidad": -cantidad,
                "Comentarios": f"Salida por reasignación a {centro}"
            })
        else:
            pedidos_restantes.append(pedido)

    return pd.DataFrame(entregas_directas), pd.DataFrame(pedidos_restantes)


def _inferir_tipo_semana_desde_puesto(puesto: str | None) -> str | None:
    """
    Reglas:
      - Si Puesto_de_trabajo empieza por 'L01'  → Ultra
      - Si empieza por 'PRECO' o es 'BOLLERIA' → Preco
      - Si no se reconoce → None
    """
    if not puesto:
        return None
    p = str(puesto).strip().upper()
    if p.startswith("L01"):
        return "Ultra"
    if p.startswith("PRECO") or p == "BOLLERIA":
        return "Preco"
    return None

def _viernes_semana(d: date) -> date:
    # lunes=0 ... domingo=6 → queremos viernes=4
    return d + timedelta(days=(4 - d.weekday()))

def preparar_calendario_fabrica(df_calendario_fabrica: pd.DataFrame) -> pd.DataFrame:
    """
    Espera columnas: Lunes_Semana (DATE), Tipo_Semana ('Ultra'/'Preco')
    Devuelve un DF único por (Tipo_Semana, Viernes_Semana) con compatibilidad=True.
    """
    cal = df_calendario_fabrica.copy()
    cal["Lunes_Semana"] = pd.to_datetime(cal["Lunes_Semana"]).dt.date
    cal["Viernes_Semana"] = cal["Lunes_Semana"].apply(_viernes_semana)
    cal["Es_compatible"] = True
    cal_semana = (
        cal[["Tipo_Semana", "Viernes_Semana", "Es_compatible"]]
        .drop_duplicates()
        .sort_values(["Tipo_Semana", "Viernes_Semana"])
    )
    return cal_semana

def validar_calendario_fabrica_por_tipo(
    ordenes_df: pd.DataFrame,
    cal_semana: pd.DataFrame,
    *,
    hoy: date | None = None
) -> pd.DataFrame:
    """
    - Usa ordenes_df con columnas: Material, Fecha_Orden, Puesto_de_trabajo (para inferir Tipo_Semana_Material).
    - cal_semana: salida de preparar_calendario_fabrica() con columnas:
        ['Tipo_Semana','Viernes_Semana','Es_compatible'=True]
    """
    if hoy is None:
        hoy = date.today()

    df = ordenes_df.copy()
    df["Fecha_Orden"] = pd.to_datetime(df["Fecha_Orden"]).dt.date

    # Tipo_Semana del material desde el Puesto_de_trabajo
    if "Tipo_Semana_Material" not in df.columns:
        df["Tipo_Semana_Material"] = df["Puesto_de_trabajo"].apply(_inferir_tipo_semana_desde_puesto)

    # Índice: lista de viernes compatibles por tipo
    compatibles_por_tipo = (
        cal_semana[cal_semana["Es_compatible"]]
        .groupby("Tipo_Semana")["Viernes_Semana"].apply(list).to_dict()
    )

    nuevas_fechas, nuevos_comentarios = [], []

    for _, r in df.iterrows():
        f = r["Fecha_Orden"]
        tipo = r.get("Tipo_Semana_Material")
        comentario = (r.get("Comentarios") or "").strip()
        bits = []

        if not tipo or tipo not in compatibles_por_tipo:
            # Sin tipo o sin calendario para ese tipo → dejamos tal cual
            nuevas_fechas.append(f)
            bits.append("Sin Tipo_Semana o sin calendario; no se valida")
            nuevos_comentarios.append(_append_comentario(comentario, " | ".join(bits)))
            continue

        viernes_actual = _viernes_semana(f)
        lista = compatibles_por_tipo[tipo]

        if viernes_actual in lista:
            nuevas_fechas.append(f)
            bits.append("Calendario OK")
        else:
            # Buscar último viernes compatible ≤ fecha_orden
            candidatos = [v for v in lista if v <= f]
            if candidatos:
                v_prev = max(candidatos)
                nueva = v_prev
                if nueva < hoy:
                    nueva = hoy
                    bits.append("Fabricación tardía")
                nuevas_fechas.append(nueva)
                bits.append(f"Reprogramado a viernes compatible {v_prev}")
            else:
                # No hay viernes compatible previo → no mover (opcional: podríamos elegir el próximo futuro)
                nuevas_fechas.append(f)
                bits.append("⚠️ Sin semana compatible previa; se mantiene")

        nuevos_comentarios.append(_append_comentario(comentario, " | ".join(bits)))

    df["Fecha_Orden"] = nuevas_fechas
    df["Comentarios"] = nuevos_comentarios
    return df

def _append_comentario(actual: str | None, extra: str) -> str:
    actual = (actual or "").strip()
    return extra if not actual else f"{actual} | {extra}"

def generar_entregas_desde_stock_fabrica(pedidos_df, stock_fabrica):
    """
    Cubre pedidos con stock de fábrica (sin OF).
    Devuelve: entregas_df, stock_fabrica_actualizado, pedidos_pendientes_df
    """
    entregas = []
    stock_fab = stock_fabrica.copy()
    pendientes = []

    pedidos_df = pedidos_df.sort_values(["Material","Fecha_Carga"])
    for _, p in pedidos_df.iterrows():
        mat = p["Material"]; cant = float(p["Cantidad"])
        if cant <= 0:
            continue
        disp = float(stock_fab.get(mat, 0))

        if disp >= cant:
            # Solo entrada al centro destino
            entregas.append({
                "id_orden": f"STOCKFAB-{mat}-{p['Fecha_Carga']:%Y%m%d}",
                "Centro": p["Centro"],
                "Material": mat,
                "Fecha_Carga": p["Fecha_Carga"],
                "Fecha_Entrega": p["Fecha_Entrega"],
                "Cantidad": cant,
                "Comentarios": "Cobertura directa desde stock de fábrica (sin OF)"
            })
            # Baja del diccionario de fábrica
            stock_fab[mat] = disp - cant
        else:
            pendientes.append(p)

    return pd.DataFrame(entregas), stock_fab, pd.DataFrame(pendientes)

def _fallback_cantidad(cantidad_ajustada, cantidad_original):
    """
    Regla de fallback:
    - Si cantidad_ajustada <= 0 ⇒ forzar cantidad_original (redondeada hacia arriba si es float).
    - En cualquier otro caso ⇒ usar cantidad_ajustada normal.
    """
    if cantidad_ajustada <= 0:
        return int(math.ceil(float(cantidad_original or 0)))
    return int(cantidad_ajustada)

def ajustar_pedidos_a_minimos_logisticos(pedidos_df: pd.DataFrame, df_minimos: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta los pedidos (en cajas) al pedido mínimo logístico según Master_Pedidos_Min.
    Regla simplificada:
      → Redondear SIEMPRE al múltiplo superior de 'Cajas_capa'.
    Si el material no está en df_minimos, deja la cantidad original.
    """

    if pedidos_df.empty:
        return pedidos_df

    if df_minimos.empty:
        pedidos_df["Cantidad_ajustada"] = pedidos_df["Cantidad"]
        return pedidos_df

    # Normalizar columnas
    df_minimos.columns = [c.strip().lower() for c in df_minimos.columns]
    df_minimos["material"] = pd.to_numeric(df_minimos["material"], errors="coerce").astype("Int64")
    pedidos_df["Material"] = pd.to_numeric(pedidos_df["Material"], errors="coerce").astype("Int64")

    # Merge (trae solo Cajas_capa)
    merged = pd.merge(
        pedidos_df,
        df_minimos[["material", "cajas_capa"]],
        left_on="Material",
        right_on="material",
        how="left",
    )

    ajustes = []
    for _, row in merged.iterrows():
        cantidad = float(row["Cantidad"])
        capa = float(row.get("cajas_capa") or 0)

        if pd.isna(capa) or capa <= 0:
            # sin dato → dejar tal cual
            ajustes.append(cantidad)
        else:
            # redondear al múltiplo superior de capa
            cantidad_ajustada = math.ceil(cantidad / capa) * capa
            ajustes.append(cantidad_ajustada)

    merged["Cantidad_ajustada"] = ajustes

    # Limpieza
    merged.drop(columns=["material", "cajas_capa"], inplace=True, errors="ignore")
    merged = merged.loc[:, ~merged.columns.duplicated()]

    return merged


def ajustar_pedidos_por_restricciones_logisticas(pedidos_df: pd.DataFrame, dia_corte: int = 2) -> pd.DataFrame:
    """
    Ajusta las fechas de pedidos según reglas logísticas semanales.

    Parámetros:
      pedidos_df : pd.DataFrame
          DataFrame con columnas ['Fecha_Rotura', 'Fecha_Carga', 'Fecha_Entrega'].
      dia_corte : int (por defecto=2)
          Día de la semana que actúa como límite (0=lunes, 1=martes, 2=miércoles, ...).
          Si la rotura cae ANTES de este día, el pedido se adelanta al miércoles de la semana anterior.

    Regla:
      - Si la Fecha_Rotura cae antes del día 'dia_corte' → adelanta Fecha_Carga y Fecha_Entrega
        al miércoles (weekday=2) de la semana anterior.
      - Si cae en o después del 'dia_corte' → mantiene la fecha original.
      - ⚙️ Si la nueva fecha cae antes de hoy(), se ajusta a hoy().
    """

    if pedidos_df.empty:
        print("⚠️ No hay pedidos para ajustar por restricciones logísticas.")
        return pedidos_df

    pedidos = pedidos_df.copy()
    pedidos["Fecha_Rotura"] = pd.to_datetime(pedidos["Fecha_Rotura"]).dt.date
    pedidos["Fecha_Carga"] = pd.to_datetime(pedidos["Fecha_Carga"]).dt.date
    pedidos["Fecha_Entrega"] = pd.to_datetime(pedidos["Fecha_Entrega"]).dt.date

    nuevas_cargas, nuevas_entregas, comentarios = [], [], []
    hoy = date.today()

    for _, row in pedidos.iterrows():
        fecha_rotura = row["Fecha_Rotura"]
        dow = fecha_rotura.weekday()  # 0=lunes ... 6=domingo

        if dow < dia_corte:  # Ej. lunes/martes si dia_corte=2
            # Calcular miércoles de la semana anterior
            dias_retroceder = dow + 5
            nueva_fecha = fecha_rotura - timedelta(days=dias_retroceder)

            # 🚫 Nunca antes de hoy
            if nueva_fecha < hoy:
                nueva_fecha = hoy

            nuevas_cargas.append(nueva_fecha)
            nuevas_entregas.append(nueva_fecha)
            comentarios.append("📦 Adelantado por restricción logística (rotura temprana)")
        else:
            nuevas_cargas.append(row["Fecha_Carga"])
            nuevas_entregas.append(row["Fecha_Entrega"])
            comentarios.append(row.get("Comentarios", ""))

    pedidos["Fecha_Carga"] = nuevas_cargas
    pedidos["Fecha_Entrega"] = nuevas_entregas
    pedidos["Comentarios"] = comentarios

    return pedidos

def ajustar_pedidos_por_restricciones_logisticas_v2(
    pedidos_df: pd.DataFrame,
    dia_corte: int,
    consumo_diario: dict,
    dias_stock_objetivo: dict
):
    import pandas as pd
    from datetime import date, timedelta

    if pedidos_df.empty:
        return pedidos_df

    pedidos = pedidos_df.copy()

    pedidos["Fecha_Rotura"]  = pd.to_datetime(pedidos["Fecha_Rotura"]).dt.date
    pedidos["Fecha_Carga"]   = pd.to_datetime(pedidos["Fecha_Carga"]).dt.date
    pedidos["Fecha_Entrega"] = pd.to_datetime(pedidos["Fecha_Entrega"]).dt.date

    hoy = date.today()
    nuevas_filas = []

    for _, row in pedidos.iterrows():
        centro   = row["Centro"]
        material = row["Material"]

        consumo  = consumo_diario.get((centro, material))
        dias_obj = dias_stock_objetivo.get((centro, material))

        if consumo is None or dias_obj is None:
            nuevas_filas.append(row)
            continue

        fecha_rotura = row["Fecha_Rotura"]
        dow = fecha_rotura.weekday()

        # Caso donde NO adelanta
        if dow >= dia_corte:
            nuevas_filas.append(row)
            continue

        # Adelanto
        dias_retro = dow + 5
        nueva_fecha = fecha_rotura - timedelta(days=dias_retro)

        if nueva_fecha < hoy:
            nueva_fecha = hoy

        dias_adelantados = (row["Fecha_Carga"] - nueva_fecha).days
        dias_reales = max(1, dias_obj - dias_adelantados)

        nueva_cantidad = consumo * dias_reales

        final_row = row.copy()
        final_row["Fecha_Carga"]   = nueva_fecha
        final_row["Fecha_Entrega"] = nueva_fecha
        final_row["Cantidad"]      = nueva_cantidad
        # Mantener comentarios previos si existieran
        prev = final_row.get("Comentarios", "")
        nuevo = "📦 Adelantado por restricción logística (V2)"

        if prev:
            final_row["Comentarios"] = prev + " • " + nuevo
        else:
            final_row["Comentarios"] = nuevo


        nuevas_filas.append(final_row)

    return pd.DataFrame(nuevas_filas)

def ajustar_pedidos_a_minimos_logisticos_v2(
    pedidos_df: pd.DataFrame,
    df_minimos: pd.DataFrame,
    df_rotacion: pd.DataFrame
) -> pd.DataFrame:

    if pedidos_df.empty:
        return pedidos_df

    pedidos = pedidos_df.copy()
    pedidos["Material"] = pd.to_numeric(pedidos["Material"], errors="coerce").astype("Int64")

    df_minimos.columns = [c.strip().lower() for c in df_minimos.columns]
    df_minimos["material"] = pd.to_numeric(df_minimos["material"], errors="coerce").astype("Int64")

    df_rotacion.columns = [c.strip().lower() for c in df_rotacion.columns]
    df_rotacion["material"] = pd.to_numeric(df_rotacion["material"], errors="coerce").astype("Int64")

    merged = (
        pedidos
        .merge(df_minimos[["material", "cajas_capa"]], left_on="Material", right_on="material", how="left")
        .merge(df_rotacion[["centro", "material", "cajas_pal", "dias_stock_pal"]],
               left_on=["Centro","Material"],
               right_on=["centro","material"],
               how="left")
    )

    ajustes = []
    comentarios = []

    for _, row in merged.iterrows():
    
        cantidad = float(row["Cantidad"]) if pd.notna(row["Cantidad"]) else 0
    
        capa = float(row["cajas_capa"]) if pd.notna(row["cajas_capa"]) else 0
        palet = float(row["cajas_pal"]) if pd.notna(row["cajas_pal"]) else 0
        dias_stock_pal = float(row["dias_stock_pal"]) if pd.notna(row["dias_stock_pal"]) else None
        
        comentario_prev = row.get("Comentarios", "") if "Comentarios" in row else ""

        if cantidad <= 0:
            ajustes.append(cantidad)
            comentarios.append(comentario_prev)
            continue

        # 🔥 Ajuste a PALET (alta rotación)
        if palet > 0 and dias_stock_pal is not None and dias_stock_pal < 11:
            cantidad_ajustada = math.ceil(cantidad / palet) * palet
            
            nuevo = "Ajustado a PALET"
            if comentario_prev:
                comentarios.append(comentario_prev + " • " + nuevo)
            else:
                comentarios.append(nuevo)

            ajustes.append(cantidad_ajustada)
            continue

        # Ajuste a CAP
        if capa > 0:
            cantidad_ajustada = math.ceil(cantidad / capa) * capa
            ajustes.append(cantidad_ajustada)
            comentarios.append(comentario_prev)
        else:
            ajustes.append(cantidad)
            comentarios.append(comentario_prev)

    merged["Cantidad_ajustada"] = ajustes
    merged["Comentarios"] = comentarios

    merged.drop(columns=["material", "centro"], inplace=True, errors="ignore")
    merged = merged.loc[:, ~merged.columns.duplicated()]

    return merged





