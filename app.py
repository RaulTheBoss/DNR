# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os, zipfile, tempfile, csv
from datetime import datetime, date
import dropbox
from dropbox.exceptions import AuthError, ApiError
import geopandas as gpd
from shapely.geometry import Point
from streamlit_folium import st_folium
import folium

# === CONFIG STREAMLIT ===
st.set_page_config(page_title="Encuesta proyectos DNR", layout="wide")

# --------- Rutas locales ----------
DATA_DIR = "data"
RESP_DIR = os.path.join(DATA_DIR, "respuestas")
AOI_DIR = os.path.join(DATA_DIR, "aoi")
LIMS_DIR = os.path.join(DATA_DIR, "limites")
GEOM_DIR = os.path.join(DATA_DIR, "geom_guardadas")
os.makedirs(RESP_DIR, exist_ok=True)
os.makedirs(AOI_DIR, exist_ok=True)
os.makedirs(LIMS_DIR, exist_ok=True)
os.makedirs(GEOM_DIR, exist_ok=True)

USERS_PATH = "users_12.csv"
RESP_CSV   = os.path.join(RESP_DIR, "respuestas.csv")
GEO_CSV    = os.path.join(RESP_DIR, "respuestas_geo.csv")
AOI_GEOJSON = os.path.join(AOI_DIR, "aoi.geojson")
MUN_CAR_GEOJSON = os.path.join(LIMS_DIR, "municipios_car.geojson")
MUN_CAR_SHP     = os.path.join(LIMS_DIR, "municipios_car.shp")

# --------- Estado sesión ----------
if "auth" not in st.session_state: st.session_state.auth = None
if "muni_sel" not in st.session_state: st.session_state.muni_sel = []
if "_mun_sel_all" not in st.session_state: st.session_state._mun_sel_all = False
if "uploaded_geom" not in st.session_state: st.session_state.uploaded_geom = None
if "lonlat_pt" not in st.session_state: st.session_state.lonlat_pt = (None, None)
if "mun_detectados" not in st.session_state: st.session_state.mun_detectados = []

COLUMNS_SCHEMA = [
    "timestamp","fecha_diligenciamiento","username","name","proyecto_key",
    "proyecto_nombre","municipios_proyecto","costo_proyecto_cop","avance_proyecto_pct",
    "comentario","modo_municipios","archivo_geo_nombre","archivo_geo_dropbox_path",
    "archivo_geo_link","inversion_equidad","inversion_distribucion"
]

# ========= Dropbox =========
@st.cache_resource
def _dbx():
    app_key = st.secrets.get("dropbox_app_key")
    app_secret = st.secrets.get("dropbox_app_secret")
    refresh_token = st.secrets.get("dropbox_refresh_token")
    if not (app_key and app_secret and refresh_token):
        raise RuntimeError("Credenciales de Dropbox faltantes en secrets.")
    dbx = dropbox.Dropbox(app_key=app_key, app_secret=app_secret, oauth2_refresh_token=refresh_token)
    dbx.users_get_current_account()
    return dbx

def _ensure_folder(dbx, path):
    try: dbx.files_create_folder_v2(path)
    except: pass

def _join_path(*parts):
    return "/" + "/".join(p.strip("/") for p in parts if p)

def dropbox_upload_or_update(local_path, dest_name=None, dest_folder=None):
    if not os.path.exists(local_path): return None, "archivo local no existe"
    if dest_folder is None:
        dest_folder = st.secrets.get("dropbox_folder", "/ENCUESTA DNR FINAL")
    if not dest_folder.startswith("/"): dest_folder = "/" + dest_folder
    if dest_name is None: dest_name = os.path.basename(local_path)
    dbx = _dbx(); _ensure_folder(dbx, dest_folder)
    subdir = os.path.dirname(dest_name).strip("/")
    if subdir: _ensure_folder(dbx, _join_path(dest_folder, subdir))
    dest_path = _join_path(dest_folder, dest_name)
    with open(local_path, "rb") as f:
        dbx.files_upload(f.read(), dest_path, mode=dropbox.files.WriteMode("overwrite"), mute=True)
    try: tlink = dbx.files_get_temporary_link(dest_path).link
    except: tlink = None
    return tlink, "uploaded"

# ========= Utilidades =========
@st.cache_data
def load_users(path=None):
    if "users" in st.secrets and st.secrets["users"]:
        df = pd.DataFrame(st.secrets["users"]).astype(str)
        return df[["name","username","password"]]
    if path and os.path.exists(path):
        return pd.read_csv(path, dtype=str)[["name","username","password"]]
    return pd.DataFrame(columns=["name","username","password"])

def auth(df, u, p):
    row = df[df["username"].str.strip().eq(str(u).strip())]
    if len(row)==1 and row.iloc[0]["password"]==str(p):
        return dict(name=row.iloc[0]["name"], username=row.iloc[0]["username"])
    return None

def save_response(row_dict):
    df = pd.DataFrame([row_dict])
    for c in COLUMNS_SCHEMA:
        if c not in df.columns: df[c] = pd.NA
    df = df[COLUMNS_SCHEMA]
    if "comentario" in df.columns:
        df["comentario"] = df["comentario"].astype(str).str.replace("\r"," ").str.replace("\n"," ")
    if os.path.exists(RESP_CSV):
        df.to_csv(RESP_CSV, mode="a", header=False, index=False, encoding="utf-8",
                  lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    else:
        df.to_csv(RESP_CSV, index=False, encoding="utf-8",
                  lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

def save_geo_point(username, lon, lat, ts):
    gdf = pd.DataFrame([{"timestamp":ts,"username":username,"lon":lon,"lat":lat}])
    if os.path.exists(GEO_CSV):
        gdf.to_csv(GEO_CSV, mode="a", header=False, index=False)
    else:
        gdf.to_csv(GEO_CSV, index=False)

@st.cache_data
def load_municipios_car_auto():
    path = MUN_CAR_GEOJSON if os.path.exists(MUN_CAR_GEOJSON) else MUN_CAR_SHP if os.path.exists(MUN_CAR_SHP) else None
    if path:
        gdf = gpd.read_file(path)
        gdf = gdf.to_crs(4326) if gdf.crs else gdf.set_crs(4326)
        cand = [c for c in ["MPIO_CNMBR","MUNICIPIO","NOMBRE","name"] if c in gdf.columns]
        name_col = cand[0] if cand else gdf.columns[0]
        nombres = sorted(gdf[name_col].dropna().astype(str).unique())
        return gdf, nombres, name_col
    return None, ["Bogotá D.C.","Soacha"], "MUNICIPIO"

def read_geo_upload(uploaded_file):
    suffix = os.path.splitext(uploaded_file.name.lower())[1]
    with tempfile.TemporaryDirectory() as td:
        fpath = os.path.join(td, uploaded_file.name)
        with open(fpath,"wb") as f: f.write(uploaded_file.getbuffer())
        if suffix == ".zip":
            with zipfile.ZipFile(fpath,'r') as zf: zf.extractall(td)
            shp = next((os.path.join(r,f) for r,_,fs in os.walk(td) for f in fs if f.lower().endswith(".shp")), None)
            if not shp: raise RuntimeError("El .zip no contiene un .shp.")
            gdf = gpd.read_file(shp)
        elif suffix in [".kmz",".kml"]:
            if suffix==".kmz":
                with zipfile.ZipFile(fpath,'r') as zf: zf.extractall(td)
                kml_path = next((os.path.join(r,f) for r,_,fs in os.walk(td) for f in fs if f.lower().endswith(".kml")), None)
                if not kml_path: raise RuntimeError("KMZ sin KML interno.")
                gdf = gpd.read_file(kml_path)
            else:
                gdf = gpd.read_file(fpath)
        else:
            raise RuntimeError("Formato no soportado (.zip .kmz .kml).")
        gdf = gdf.to_crs(4326) if gdf.crs else gdf.set_crs(4326)
        return gdf

def municipios_por_interseccion(gdf_geom, mun_gdf, name_col):
    try:
        inter = gpd.sjoin(gdf_geom, mun_gdf[[name_col,'geometry']], how="inner", predicate="intersects")
        return sorted(inter[name_col].dropna().astype(str).unique())
    except: return []

def representative_point(geom):
    if geom.is_empty: return None
    return geom if geom.geom_type=="Point" else geom.representative_point()

def _drop_datetime_cols_for_folium(gdf):
    import pandas as pd
    for c in gdf.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf[c]):
            gdf = gdf.drop(columns=[c], errors="ignore")
    return gdf

# ========= Login =========
st.title("Encuesta proyectos DNR - Historial y Última Respuesta")
users_df = load_users(USERS_PATH)

if st.session_state.auth is None:
    with st.form("login"):
        st.subheader("Iniciar sesión")
        u = st.text_input("Usuario")
        p = st.text_input("Contraseña", type="password")
        if st.form_submit_button("Ingresar"):
            user = auth(users_df, u, p)
            if user:
                st.session_state.auth = user
                st.success(f"Bienvenido/a {user['name']}")
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos")
    st.stop()

user = st.session_state.auth
st.sidebar.success(f"Sesión: {user['name']} ({user['username']})")
if st.sidebar.button("Cerrar sesión"):
    st.session_state.auth = None
    st.rerun()

# ========= Datos de municipios =========
mun_gdf, mun_list, mun_name_col = load_municipios_car_auto()

# ========= Formulario principal =========
st.header("Encuesta")
with st.form("encuesta_form"):
    proyecto_nombre = st.text_input("Nombre del proyecto")
    costo_proyecto = st.number_input("Costo del proyecto (COP)", min_value=0.0, step=1000.0)
    avance_proyecto = st.number_input("Avance físico (%)", min_value=0, max_value=100, step=1)
    fecha_dilig = st.date_input("Fecha de diligenciamiento", value=date.today())

    st.markdown("**Municipios del proyecto**")
    geo_file = st.file_uploader("Archivo geográfico (opcional)", type=["zip","kmz","kml"])
    preview_clicked = st.form_submit_button("🔎 Previsualizar capa y detectar municipios", type="secondary")
    if preview_clicked and geo_file:
        try:
            gdf = read_geo_upload(geo_file)
            st.session_state.uploaded_geom = gdf
            b = gdf.total_bounds
            m_prev = folium.Map(location=[(b[1]+b[3])/2, (b[0]+b[2])/2], zoom_start=10)
            folium.GeoJson(_drop_datetime_cols_for_folium(gdf).to_json(),
                           style_function=lambda x: {"fillOpacity":0.15,"weight":2}).add_to(m_prev)
            m_prev.fit_bounds([[b[1], b[0]], [b[3], b[2]]])
            st_folium(m_prev, height=450, width=900)
            rp = representative_point(gdf.geometry.iloc[0])
            st.session_state.lonlat_pt = (rp.x, rp.y) if rp is not None else (None,None)
            if mun_gdf is not None:
                muni_det = municipios_por_interseccion(gdf, mun_gdf, mun_name_col)
                st.session_state.mun_detectados = muni_det
                if muni_det:
                    st.success("Municipios detectados: " + ", ".join(muni_det))
                    st.session_state.muni_sel = sorted(list(set(st.session_state.muni_sel) | set(muni_det)))
        except Exception as e:
            st.error(f"No se pudo procesar el archivo: {e}")

    municipios_seleccionados = st.multiselect(
        "Municipios CAR (usa los botones debajo para seleccionar todos o limpiar)",
        options=mun_list,
        default=st.session_state.muni_sel,
        key="muni_sel"
    )
    if set(municipios_seleccionados) != set(mun_list):
        st.session_state._mun_sel_all = False

    inversion_equidad = st.radio("¿Inversión equitativa?", options=["Si","No"], horizontal=True)
    comentario = st.text_area("Comentarios sobre distribución de la inversión")

    submitted = st.form_submit_button("Enviar respuesta")

# ========= Botones fuera del formulario =========
st.markdown("#### Acciones rápidas sobre municipios")
col1b, col2b = st.columns([1,1])
with col1b:
    if st.button("Toda la jurisdicción"):
        st.session_state.muni_sel = mun_list[:]
        st.session_state._mun_sel_all = True
        st.success("Se seleccionaron todos los municipios.")
with col2b:
    if st.button("Limpiar selección"):
        st.session_state.muni_sel = []
        st.session_state._mun_sel_all = False
        st.info("Selección de municipios limpiada.")

# ========= Guardado de respuestas =========
if submitted:
    if not proyecto_nombre: st.error("Debes ingresar el nombre del proyecto."); st.stop()
    if len(st.session_state.muni_sel)==0: st.error("Debes seleccionar al menos un municipio."); st.stop()

    ts = datetime.utcnow().isoformat(); ts_tag = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    geo_name = geo_file.name if geo_file else ""
    geo_link = geo_path = ""
    if geo_file:
        tmp = os.path.join(tempfile.gettempdir(), f"{ts_tag}_{geo_name}")
        with open(tmp,"wb") as f: f.write(geo_file.getbuffer())
        geo_link, _ = dropbox_upload_or_update(tmp, dest_name=f"uploads/{user['username']}_{ts_tag}_{geo_name}")
        geo_path = "/ENCUESTA DNR FINAL/uploads/" + f"{user['username']}_{ts_tag}_{geo_name}"

    lon_pt, lat_pt = st.session_state.lonlat_pt
    save_geo_point(user["username"], lon_pt, lat_pt, ts)

    resp = {
        "timestamp":ts,"fecha_diligenciamiento":fecha_dilig.isoformat(),
        "username":user["username"],"name":user["name"],
        "proyecto_key":"", "proyecto_nombre":proyecto_nombre,
        "municipios_proyecto":"; ".join(st.session_state.muni_sel),
        "costo_proyecto_cop":costo_proyecto, "avance_proyecto_pct":avance_proyecto,
        "comentario":comentario, "modo_municipios":"archivo" if geo_file else "manual",
        "archivo_geo_nombre":geo_name, "archivo_geo_dropbox_path":geo_path,
        "archivo_geo_link":geo_link, "inversion_equidad":inversion_equidad,
        "inversion_distribucion":""
    }
    save_response(resp)
    st.success("Respuesta guardada correctamente.")
