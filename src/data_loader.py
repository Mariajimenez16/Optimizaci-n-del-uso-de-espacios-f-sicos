import pandas as pd
import os

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
BASE             = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_CATALOGOS    = os.path.join(BASE, "data", "input", "catalogos")
DIR_PROGRAMACION = os.path.join(BASE, "data", "input", "programacion")
DIR_DEMANDA      = os.path.join(BASE, "data", "input", "demanda")

ARCHIVOS = {
    "asignaturas":      (DIR_CATALOGOS,    "1. Asignaturas.xlsx",                                    "Sheet1"),
    "docentes_cat":     (DIR_CATALOGOS,    "4. Docentes_Catálogo.xlsx",                              "Sheet1"),
    "disponibilidad":   (DIR_CATALOGOS,    "6. Docentes_Disponibilidad.xlsx",                        "Sheet1"),
    "doc_asignaturas":  (DIR_CATALOGOS,    "7. Docentes_Asignaturas.xlsx",                           "Sheet1"),
    "restricciones_ed": (DIR_CATALOGOS,    "8. Restricciones de Edificio para Asignaturas.xlsx",     "Sheet 1"),
    "programacion":     (DIR_PROGRAMACION, "programacion_pregrado_sin docentes.xlsx",                "SZRPROG"),
    "prog_completo":    (DIR_PROGRAMACION, "PROGRAMACION_COMPLETO_2026_20260119 -sin docentes.xlsx", "Sheet 1"),
    "reporte":          (DIR_PROGRAMACION, "reporte_MCIB-MCCD-MCIC V3_mod.xlsx",                    "Hoja1"),
    "mallas":           (DIR_DEMANDA,      "0. Mallas.xlsx",                                         "Sheet1"),
    "semanas":          (DIR_DEMANDA,      "2. Relación_asignaturas_semanas.xlsx",                   "Sheet1"),
    "demandas":         (DIR_DEMANDA,      "3. Demandas.xlsx",                                       "Sheet 1"),
    "demanda_hist":     (DIR_DEMANDA,      "DEMANDA 202410 - MCIB-MCIC-MCCD_mod.xlsx",              "Hoja1"),
    "prematricula":     (DIR_DEMANDA,      "PREMATRICULA 202410 MCIB-MCIC-MCCD_mod.xlsx",           "INFORME"),
}

COLUMNAS_CRITICAS = {
    "asignaturas":      ["ASIGNATURA", "COMPONENTE", "NUM BLOQUES", "NUM SESIONES", "VAC MAX"],
    "docentes_cat":     ["ID DOCENTE", "TIPO CONTRATO", "PRIORIDAD", "MAX BLOQUES", "MAX SECCIONES"],
    "disponibilidad":   ["Unnamed: 0"],
    "doc_asignaturas":  ["ID DOCENTE", "ASIGNATURA", "MAX BLOQUES ASIGNATURA", "MAX SECCIONES ASIGNATURA"],
    "restricciones_ed": ["ASIGNATURA", "EDIFICIO", "PRIORIDAD"],
    "programacion":     ["NRC", "MATERIA", "SALON", "BLOQUE", "CAPACIDAD", "TIPO_HORARIO_SSASECT"],
    "prog_completo":    ["NRC", "MATERIA", "SALON", "BLOQUE", "CAPACIDAD"],
    "reporte":          ["NRC", "MATERIA_CURSO", "SALON", "BLOQUE", "CAPACIDAD", "DOCENTE"],
    "mallas":           ["CODIGO", "CARRERA", "CURRICULO", "NIVEL"],
    "semanas":          ["ASIGNATURA", "COMPONENTE"],
    "demandas":         ["ASIGNATURA", "DEMANDA", "USABLE"],
    "demanda_hist":     [],
    "prematricula":     ["Materia curso", "Total estudiantes prematriculados", "Grupos prematrícula redondeado"],
}

DIAS  = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
HORAS = ['06:00','07:00','08:00','09:00','10:00','11:00','12:00',
         '13:00','14:00','15:00','16:00','17:00','18:00','19:00','20:00','21:00']

# Mapeo de componente → tipo de sala requerida
TIPO_SALA = {
    **{t: 'AULA'             for t in ('TEO', 'TEC', 'TRA', 'DAS')},
    **{t: 'LABORATORIO'      for t in ('LAB', 'TAL')},
    **{t: 'TEORICO_PRACTICO' for t in ('TPR', 'PRT', 'PTR', 'PRA', 'FTP')},
    **{t: 'NINGUNA'          for t in ('VIR', 'PRE', 'PF',  'EVD')},
}


# ─────────────────────────────────────────────
# PASO 1 — CARGA Y AUDITORÍA
# ─────────────────────────────────────────────
def cargar_archivo(nombre):
    directorio, archivo, hoja = ARCHIVOS[nombre]
    ruta = os.path.join(directorio, archivo)
    if not os.path.exists(ruta):
        print(f"  ❌ ARCHIVO NO ENCONTRADO: {ruta}")
        return None
    try:
        return pd.read_excel(ruta, sheet_name=hoja)
    except Exception as e:
        print(f"  ❌ ERROR AL LEER {archivo}: {e}")
        return None


def auditar_archivo(nombre, df):
    print(f"\n{'─'*60}")
    print(f"📄 {nombre.upper()}")
    print(f"   Filas: {len(df):,}  |  Columnas: {len(df.columns)}")

    criticas = COLUMNAS_CRITICAS.get(nombre, [])
    if criticas:
        print("   Columnas críticas:")
        for col in criticas:
            if col not in df.columns:
                print(f"      ⚠️  '{col}' NO EXISTE en el archivo")
            else:
                nans  = df[col].isna().sum()
                pct   = nans / len(df) * 100
                icono = "✅" if nans == 0 else ("⚠️ " if pct < 10 else "❌")
                print(f"      {icono} '{col}': {nans} NaN ({pct:.1f}%)")

    vacias = [c for c in df.columns
              if df[c].isna().sum() / len(df) > 0.5 and c not in criticas]
    if vacias:
        print(f"   Columnas >50% vacías (no críticas): {len(vacias)}")

    dups = df.duplicated().sum()
    if dups:
        print(f"   ⚠️  Filas duplicadas: {dups}")


def cargar_todos():
    print("=" * 60)
    print("  AUDITORÍA DE DATOS — FASE 0 PASO 1")
    print("=" * 60)

    dfs, errores = {}, []
    for nombre in ARCHIVOS:
        df = cargar_archivo(nombre)
        if df is not None:
            auditar_archivo(nombre, df)
            dfs[nombre] = df
        else:
            errores.append(nombre)

    print(f"\n{'='*60}")
    print(f"  RESUMEN")
    print(f"  ✅ Cargados correctamente: {len(dfs)}/{len(ARCHIVOS)}")
    if errores:
        print(f"  ❌ Con errores: {errores}")
    print("=" * 60)
    return dfs


# ─────────────────────────────────────────────
# PASO 2 — CATÁLOGO DE SALONES
# ─────────────────────────────────────────────
def construir_catalogo_salones(df_programacion):
    df = (df_programacion[['BLOQUE', 'SALON', 'DESC_SALON', 'CAPACIDAD']]
          .dropna(subset=['SALON', 'BLOQUE', 'CAPACIDAD'])
          .copy())
    df['LLAVE'] = df['BLOQUE'].astype(str) + '_' + df['SALON'].astype(str)

    # Alertar si el mismo salón aparece con distintas capacidades en el histórico
    problemas = df.groupby('LLAVE')['CAPACIDAD'].nunique()
    problemas = problemas[problemas > 1]
    if len(problemas):
        print(f"\n⚠️  Salones con capacidad inconsistente: {len(problemas)}")
        print(df[df['LLAVE'].isin(problemas.index)][['LLAVE', 'CAPACIDAD']]
              .drop_duplicates().to_string())

    catalogo = (df.drop_duplicates(subset=['LLAVE'])
                  [['LLAVE', 'BLOQUE', 'SALON', 'DESC_SALON', 'CAPACIDAD']]
                  .reset_index(drop=True))

    print(f"\n📦 CATÁLOGO DE SALONES")
    print(f"   Total salones únicos: {len(catalogo)}")
    print(f"   Bloques: {sorted(catalogo['BLOQUE'].unique())}")
    print(f"\n   Muestra:")
    print(catalogo[['BLOQUE', 'SALON', 'DESC_SALON', 'CAPACIDAD']].head(10).to_string())
    return catalogo


# ─────────────────────────────────────────────
# PASO 3 — CATÁLOGO DE DOCENTES
# ─────────────────────────────────────────────
def _parsear_disponibilidad(df_disp):
    """
    El archivo de disponibilidad tiene los headers en la fila 0
    y columnas duplicadas por díaxhora. Se accede por posición:
    col_idx = dia_idx * len(HORAS) + hora_idx
    """
    disp = df_disp.copy()
    disp.columns = disp.iloc[0]
    disp = disp.iloc[1:].reset_index(drop=True)
    disp = disp.rename(columns={disp.columns[0]: 'ID DOCENTE'})
    disp['ID DOCENTE'] = pd.to_numeric(disp['ID DOCENTE'], errors='coerce')
    disp = disp.dropna(subset=['ID DOCENTE'])
    disp['ID DOCENTE'] = disp['ID DOCENTE'].astype(int)

    n_horas = len(HORAS)
    resultado = {}
    for _, row in disp.iterrows():
        doc_id  = int(row.iloc[0])
        valores = row.iloc[1:].tolist()
        resultado[doc_id] = {
            dia: {
                hora: int(valores[i * n_horas + j]) if pd.notna(valores[i * n_horas + j]) else 0
                for j, hora in enumerate(HORAS)
            }
            for i, dia in enumerate(DIAS)
        }
    return resultado


def construir_catalogo_docentes(df_cat, df_disp, df_doc_asig):
    disponibilidad = _parsear_disponibilidad(df_disp)

    asig_por_docente = {}
    for _, row in df_doc_asig.iterrows():
        try:
            doc_id = int(row['ID DOCENTE'])
        except (ValueError, TypeError):
            continue
        asig_por_docente.setdefault(doc_id, []).append({
            'asignatura':    row['ASIGNATURA'],
            'max_bloques':   row['MAX BLOQUES ASIGNATURA'],
            'max_secciones': row['MAX SECCIONES ASIGNATURA'],
            'prioridad':     row['PRIORIDAD ASIGNATURA'],
        })

    catalogo, pendientes = [], []
    sin_disponibilidad, sin_asignaturas = [], []

    for _, row in df_cat.iterrows():
        try:
            doc_id = int(row['ID DOCENTE'])
        except (ValueError, TypeError):
            pendientes.append(str(row['ID DOCENTE']))
            continue

        catalogo.append({
            'id':             doc_id,
            'tipo':           row['TIPO CONTRATO'],
            'prioridad':      row['PRIORIDAD'],
            'min_bloques':    row['MIN BLOQUES'],
            'max_bloques':    row['MAX BLOQUES'],
            'max_secciones':  row['MAX SECCIONES'],
            'disponibilidad': disponibilidad.get(doc_id, {}),
            'asignaturas':    asig_por_docente.get(doc_id, []),
        })

        if doc_id not in disponibilidad:   sin_disponibilidad.append(doc_id)
        if doc_id not in asig_por_docente: sin_asignaturas.append(doc_id)

    tipos = pd.Series([d['tipo'] for d in catalogo]).value_counts().to_dict()

    print(f"\n👥 CATÁLOGO DE DOCENTES")
    print(f"   Total docentes: {len(catalogo)}")
    print(f"   Por tipo de contrato: {tipos}")
    print(f"   Sin disponibilidad registrada: {len(sin_disponibilidad)}")
    print(f"   Sin asignaturas asignadas:     {len(sin_asignaturas)}")
    if pendientes:
        print(f"   ⚠️  Docentes con ID pendiente (excluidos): {len(pendientes)}")
        print(f"       Ejemplos: {pendientes[:5]}")

    if catalogo:
        ej = catalogo[0]
        horas_disp = sum(v for dia in ej['disponibilidad'].values() for v in dia.values())
        print(f"\n   Ejemplo docente ID {ej['id']}:")
        print(f"   Tipo: {ej['tipo']} | Prioridad: {ej['prioridad']}")
        print(f"   Max bloques: {ej['max_bloques']} | Max secciones: {ej['max_secciones']}")
        print(f"   Asignaturas habilitadas: {len(ej['asignaturas'])}")
        print(f"   Horas disponibles totales: {horas_disp}")

    return catalogo


# ─────────────────────────────────────────────
# PASO 4 — VALIDACIÓN DE JOINS
# ─────────────────────────────────────────────
def validar_joins(dfs):
    def _reporte_join(titulo, set_a, set_b, label_faltantes, info=False):
        faltantes = set_a - set_b
        print(f"\n🔗 {titulo}")
        print(f"   {label_faltantes[0]}: {len(set_a)}")
        print(f"   {label_faltantes[1]}: {len(set_b)}")
        print(f"   {label_faltantes[2]}: {len(faltantes)}")
        if faltantes:
            icono = "ℹ️ " if info else "⚠️ "
            print(f"   {icono} {'Pueden ir a cualquier edificio' if info else f'Ejemplos: {list(faltantes)[:5]}'}")

    print(f"\n{'='*60}")
    print(f"  VALIDACIÓN DE JOINS — FASE 0 PASO 4")
    print(f"{'='*60}")

    asig_demanda  = set(dfs['demandas']['ASIGNATURA'].dropna().unique())
    asig_catalogo = set(dfs['asignaturas']['ASIGNATURA'].dropna().unique())
    asig_restricc = set(dfs['restricciones_ed']['ASIGNATURA'].dropna().unique())
    ids_catalogo  = set(dfs['docentes_cat']['ID DOCENTE'].dropna().astype(str))
    ids_doc_asig  = set(dfs['doc_asignaturas']['ID DOCENTE'].dropna().astype(str))
    ids_disp      = set(dfs['disponibilidad'].iloc[1:][dfs['disponibilidad'].columns[0]]
                        .dropna().astype(str))

    _reporte_join(
        "Demandas → Asignaturas", asig_demanda, asig_catalogo,
        ["Asignaturas en demanda:", "Asignaturas en catálogo:", "En demanda pero SIN catálogo:"]
    )
    _reporte_join(
        "Demandas → Restricciones de edificio", asig_demanda, asig_restricc,
        ["Asignaturas en demanda:", "Asignaturas con restricción:", "En demanda sin restricción:"],
        info=True
    )
    _reporte_join(
        "Docentes catálogo → Docentes asignaturas", ids_catalogo, ids_doc_asig,
        ["IDs en catálogo:", "IDs en doc_asignaturas:", "En catálogo sin asignaturas:"]
    )
    _reporte_join(
        "Docentes catálogo → Disponibilidad", ids_catalogo, ids_disp,
        ["IDs en catálogo:", "IDs en disponibilidad:", "En catálogo sin disponibilidad:"]
    )
    print(f"\n{'='*60}")


# ─────────────────────────────────────────────
# PASO 5 — CATÁLOGO DE ASIGNATURAS
# ─────────────────────────────────────────────
def _tipo_sala(componente):
    """TEO1 → 'AULA', LAB2 → 'LABORATORIO', PF3 → 'NINGUNA', etc."""
    if pd.isna(componente):
        return None
    prefijo = ''.join(filter(str.isalpha, str(componente)))
    # PF1–PF4 se normalizan a PF
    clave = prefijo[:2] if prefijo.startswith('PF') else prefijo
    return TIPO_SALA.get(clave, 'AULA')  # default conservador


def construir_catalogo_asignaturas(df_asignaturas, df_demandas):
    """
    Llave única: ASIGNATURA + COMPONENTE.
    Una misma asignatura puede tener TEO1 (aula) y LAB2 (laboratorio)
    como entradas separadas — el solver las trata independientemente.
    """
    cols_lab  = [c for c in df_asignaturas.columns
                 if c.startswith('L') and c != 'LUNES']
    cols_sala = ['STIC', 'SWACOM']

    df = (df_asignaturas
          .drop_duplicates(subset=['ASIGNATURA', 'COMPONENTE'])
          .reset_index(drop=True))

    demanda_por_asig = df_demandas.groupby('ASIGNATURA')['DEMANDA'].sum().to_dict()

    catalogo = []
    for _, row in df.iterrows():
        asig_id    = row['ASIGNATURA']
        componente = row['COMPONENTE']
        t_sala     = _tipo_sala(componente)

        catalogo.append({
            'id':               f"{asig_id}-{componente}",
            'asignatura':       asig_id,
            'componente':       componente,
            'nombre':           row['NOMBRE'],
            'tipo_horario':     ''.join(filter(str.isalpha, str(componente))),
            'tipo_sala':        t_sala,
            'requiere_sala':    t_sala != 'NINGUNA',
            'labs_requeridos':  [c for c in cols_lab  if row.get(c, 0) == 1],
            'salas_requeridas': [c for c in cols_sala if row.get(c, 0) == 1],
            'num_bloques':      row['NUM BLOQUES'],
            'num_sesiones':     row['NUM SESIONES'],
            'vac_max':          row['VAC MAX'],
            'jornada':          row.get('JORNADA', None),
            'demanda':          demanda_por_asig.get(asig_id, 0),
        })

    tipos_sala = pd.Series([a['tipo_sala'] for a in catalogo]).value_counts().to_dict()
    multi      = df.groupby('ASIGNATURA')['COMPONENTE'].count()
    multi      = multi[multi > 1]

    print(f"\n📚 CATÁLOGO DE ASIGNATURAS")
    print(f"   Total entradas (asignatura+componente): {len(catalogo)}")
    print(f"   Asignaturas únicas:                     {df['ASIGNATURA'].nunique()}")
    print(f"   Por tipo de sala requerida:             {tipos_sala}")
    print(f"   Con laboratorio específico:             {sum(1 for a in catalogo if a['labs_requeridos'])}")
    print(f"   Con sala especial requerida:            {sum(1 for a in catalogo if a['salas_requeridas'])}")
    print(f"   Asignaturas con múltiples componentes: {len(multi)}")

    if len(multi):
        ej_id = multi.index[0]
        print(f"\n   Ejemplo ({ej_id}):")
        print(df[df['ASIGNATURA'] == ej_id][['ASIGNATURA', 'COMPONENTE', 'NUM BLOQUES']]
              .to_string(index=False))

    return catalogo


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    dfs = cargar_todos()
    catalogo_salones     = construir_catalogo_salones(dfs['programacion'])
    catalogo_docentes    = construir_catalogo_docentes(
                               dfs['docentes_cat'],
                               dfs['disponibilidad'],
                               dfs['doc_asignaturas'])
    validar_joins(dfs)
    catalogo_asignaturas = construir_catalogo_asignaturas(
                               dfs['asignaturas'],
                               dfs['demandas'])