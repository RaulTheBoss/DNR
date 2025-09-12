# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import os
import zipfile
import tempfile
from datetime import datetime, date
import csv  # para escritura robusta de CSV

# ==== Dropbox (con refresh token) ====
import dropbox
from dropbox.exceptions import AuthError, ApiError

import geopandas as gpd
from shapely.geometry import Point
from streamlit_folium import st_folium
import folium

# === TÍTULO / CONFIG ===
st.set_page_config(page_title="Encuesta proyectos DNR", layout="wide")

# --------- Rutas ----------
DATA_DIR = "data"
RESP_DIR = os.path.join(DATA_DIR, "respuestas")
AOI_DIR = os.path.join(DATA_DIR, "aoi")
LIMS_DIR = os.path.join(DATA_DIR, "limites")
GEOM_DIR = os.path.join(DATA_DIR, "geom_guardadas")
os.makedirs(RESP_DIR, exist_ok=True)
os.makedirs(AOI_DIR, exist_ok=True)
os.makedirs(LIMS_DIR, exist_ok=True)
os.makedirs(GEOM_DIR, exist_ok=True)

USERS_PATH = "users_12.csv"  # respaldo local solo para desarrollo
RESP_CSV   = os.path.join(RESP_DIR, "respuestas.csv")
GEO_CSV    = os.path.join(RESP_DIR, "respuestas_geo.csv")

AOI_GEOJSON = os.path.join(AOI_DIR, "aoi.geojson")

# Rutas posibles para municipios CAR (auto-detección)
MUN_CAR_GEOJSON = os.path.join(LIMS_DIR, "municipios_car.geojson")
MUN_CAR_SHP     = os.path.join(LIMS_DIR, "municipios_car.shp")

# --------- Estado / defaults seguros ----------
if "auth" not in st.session_state:
    st.session_state.auth = None

if "muni_sel" not in st.session_state:
    st.session_state.muni_sel = []

if "_mun_sel_all" not in st.session_state:
    st.session_state._mun_sel_all = False

if "uploaded_geom" not in st.session_state:
    st.session_state.uploaded_geom = None
if "lonlat_pt" not in st.session_state:
    st.session_state.lonlat_pt = (None, None)
if "mun_detectados" not in st.session_state:
    st.session_state.mun_detectados = []

# --------- Esquema de columnas (para CSV de respuestas) ----------
COLUMNS_SCHEMA = [
    "timestamp",
    "fecha_diligenciamiento",
    "username",
    "name",
    "proyecto_key",
    "proyecto_nombre",
    "municipios_proyecto",
    "costo_proyecto_cop",
    "avance_proyecto_pct",  # KPI
    "comentario",
    "modo_municipios",
    # === Vinculación del archivo original subido (solo si se envía) ===
    "archivo_geo_nombre",        # nombre original
    "archivo_geo_dropbox_path",  # ruta final en dropbox
    "archivo_geo_link",          # link compartido (temporal)
    # === columnas de inversión ===
    "inversion_equidad",         # "Si" / "No"
    "inversion_distribucion",    # se mantiene vacía (compatibilidad)
]

# ========= Helpers de Dropbox (REFRESH TOKEN) =========
@st.cache_resource
def _dbx():
    app_key = st.secrets.get("dropbox_app_key")
    app_secret = st.secrets.get("dropbox_app_secret")
    refresh_token = st.secrets.get("dropbox_refresh_token")
    if not (app_key and app_secret and refresh_token):
        raise RuntimeError("Faltan credenciales de Dropbox en secrets: dropbox_app_key, dropbox_app_secret, dropbox_refresh_token")
    dbx = dropbox.Dropbox(
        app_key=app_key,
        app_secret=app_secret,
        oauth2_refresh_token=refresh_token,
    )
    dbx.users_get_current_account()
    return dbx

def _ensure_folder(dbx: dropbox.Dropbox, path: str):
    try:
        dbx.files_create_folder_v2(path)
    except Exception:
        pass

def _join_path(*parts):
    txt = "/".join(p.strip("/") for p in parts if p is not None and p != "")
    return "/" + txt

def dropbox_upload_or_update(local_path, dest_name=None, dest_folder=None):
    if not os.path.exists(local_path):
        return None, "archivo local no existe"
    if dest_folder is None:
        dest_folder = st.secrets.get("dropbox_folder", "/ENCUESTA DNR FINAL")
    if not dest_folder.startswith("/"):
        dest_folder = "/" + dest_folder
    if dest_name is None:
        dest_name = os.path.basename(local_path)
    dbx = _dbx()
    _ensure_folder(dbx, dest_folder)
    subdir = os.path.dirname(dest_name).strip("/")
    if subdir:
        _ensure_folder(dbx, _join_path(dest_folder, subdir))
    dest_path = _join_path(dest_folder, dest_name)
    with open(local_path, "rb") as f:
        try:
            dbx.files_upload(f.read(), dest_path, mode=dropbox.files.WriteMode("overwrite"), mute=True)
        except AuthError as e:
            raise RuntimeError(f"AuthError de Dropbox: {e}") from e
        except ApiError as e:
            raise RuntimeError(f"ApiError de Dropbox: {e}") from e
    try:
        tlink = dbx.files_get_temporary_link(dest_path).link
    except Exception:
        tlink = None
    return tlink, "uploaded"

# --------- Utilidades ----------
@st.cache_data
def load_users(path=None):
    """Primero intenta leer usuarios de st.secrets['users']; si no, CSV de respaldo."""
    if "users" in st.secrets and st.secrets["users"]:
        df = pd.DataFrame(st.secrets["users"]).astype(str)
        req = {"username", "password", "name"}
        if not req.issubset(df.columns):
            raise RuntimeError("st.secrets['users'] debe tener columnas: name, username, password")
        return df[["name", "username", "password"]]
    if path and os.path.exists(path):
        df = pd.read_csv(path, dtype=str)
        assert {"username","password","name"}.issubset(df.columns)
        return df
    return pd.DataFrame(columns=["name", "username", "password"])

def auth(df_users, u, p):
    row = df_users[df_users["username"].str.strip().eq(str(u).strip())]
    if len(row) == 1 and row.iloc[0]["password"] == str(p):
        return dict(name=row.iloc[0]["name"], username=row.iloc[0]["username"])
    return None

def save_response(row_dict):
    df = pd.DataFrame([row_dict])
    for c in COLUMNS_SCHEMA:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[COLUMNS_SCHEMA]
    if "comentario" in df.columns:
        df["comentario"] = df["comentario"].astype(str).str.replace("\r", " ").str.replace("\n", " ")
    if os.path.exists(RESP_CSV):
        df.to_csv(RESP_CSV, mode="a", header=False, index=False, encoding="utf-8", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    else:
        df.to_csv(RESP_CSV, index=False, encoding="utf-8", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

def save_geo_point(username, lon, lat, ts):
    gdf = pd.DataFrame([{"timestamp": ts, "username": username, "lon": float(lon) if lon is not None else None, "lat": float(lat) if lat is not None else None}])
    if os.path.exists(GEO_CSV):
        gdf.to_csv(GEO_CSV, mode="a", header=False, index=False)
    else:
        gdf.to_csv(GEO_CSV, index=False)

@st.cache_data
def load_aoi():
    if os.path.exists(AOI_GEOJSON):
        gdf = gpd.read_file(AOI_GEOJSON)
        if gdf.crs is None:
            gdf.set_crs(4326, inplace=True)
        else:
            gdf = gdf.to_crs(4326)
        bounds = gdf.total_bounds
        center_lon = (bounds[0] + bounds[2]) / 2
        center_lat = (bounds[1] + bounds[3]) / 2
        span = max(bounds[2] - bounds[0], bounds[3] - bounds[1])
        zoom = 6 if span > 5 else 7 if span > 2 else 8 if span > 1 else 9
        return gdf, center_lat, center_lon, zoom
    return None, 4.6, -74.08, 6

@st.cache_data
def load_municipios_car_auto():
    path = None
    if os.path.exists(MUN_CAR_GEOJSON):
        path = MUN_CAR_GEOJSON
    elif os.path.exists(MUN_CAR_SHP):
        path = MUN_CAR_SHP
    if path:
        try:
            gdf = gpd.read_file(path, engine="pyogrio")
        except Exception:
            gdf = gpd.read_file(path)
        if gdf.crs is None:
            gdf.set_crs(4326, inplace=True)
        else:
            gdf = gdf.to_crs(4326)
        cand_cols = ["MPIO_CNMBR", "MUNICIPIO", "NOMBRE", "name", "mpio", "mpio_nmbr"]
        name_col = None
        for c in cand_cols:
            if c in gdf.columns:
                name_col = c
                break
        if name_col is None:
            name_col = next((c for c in gdf.columns if gdf[c].dtype == object), gdf.columns[0])
        nombres = sorted(gdf[name_col].dropna().astype(str).unique().tolist())
        return gdf, nombres, name_col
    demo = ["Bogota D.C.", "Chia", "Zipaquira", "Facatativa", "Soacha", "Choconta", "Guatavita"]
    return None, demo, "MUNICIPIO"

def representative_point(geom):
    try:
        if geom.is_empty:
            return None
        if geom.geom_type == "Point":
            return geom
        return geom.representative_point()
    except Exception:
        return None

# === Helper de lectura: usa pyogrio si existe; si no, fiona ===
def _gpd_read(path):
    try:
        return gpd.read_file(path, engine="pyogrio")
    except Exception:
        return gpd.read_file(path)

def read_geo_upload(uploaded_file):
    suffix = os.path.splitext(uploaded_file.name.lower())[1]
    with tempfile.TemporaryDirectory() as td:
        fpath = os.path.join(td, uploaded_file.name)
        with open(fpath, "wb") as f:
            f.write(uploaded_file.getbuffer())
        if suffix == ".zip":
            with zipfile.ZipFile(fpath, 'r') as zf:
                zf.extractall(td)
            shp = None
            for root, _, files in os.walk(td):
                for fn in files:
                    if fn.lower().endswith(".shp"):
                        shp = os.path.join(root, fn)
                        break
            if shp is None:
                raise RuntimeError("El .zip no contiene un .shp.")
            gdf = _gpd_read(shp)
        elif suffix in [".kmz", ".kml"]:
            if suffix == ".kmz":
                with zipfile.ZipFile(fpath, 'r') as zf:
                    zf.extractall(td)
                kml_path = None
                for root, _, files in os.walk(td):
                    for fn in files:
                        if fn.lower().endswith(".kml"):
                            kml_path = os.path.join(root, fn)
                            break
                if not kml_path:
                    raise RuntimeError("El KMZ no contiene un KML interno.")
                gdf = _gpd_read(kml_path)
            else:
                gdf = _gpd_read(fpath)
        else:
            raise RuntimeError("Formato no soportado. Sube .zip (SHP) o .kmz/.kml.")
        if gdf.crs is None:
            gdf.set_crs(4326, inplace=True)
        else:
            gdf = gdf.to_crs(4326)
        return gdf

def municipios_por_interseccion(gdf_geom, mun_gdf, name_col):
    try:
        inter = gpd.sjoin(gdf_geom, mun_gdf[[name_col, 'geometry']], how="inner", predicate="intersects")
        muni = sorted(inter[name_col].dropna().astype(str).unique().tolist())
        return muni
    except Exception:
        return []

# === Helper para Folium: eliminar columnas datetime antes de serializar ===
def _drop_datetime_cols_for_folium(gdf):
    import pandas as pd
    dt_cols = []
    for c in gdf.columns:
        s = gdf[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            dt_cols.append(c)
        else:
            try:
                if s.map(lambda v: isinstance(v, (pd.Timestamp, datetime))).any():
                    dt_cols.append(c)
            except Exception:
                pass
    if dt_cols:
        gdf = gdf.drop(columns=dt_cols, errors="ignore")
    return gdf

# --------- Login ----------
st.title("Encuesta proyectos DNR - Historial y Última Respuesta")
users_df = load_users(USERS_PATH)

# --- Sanitizar CSV (opcional, idempotente)
def _read_csv_robusto(path):
    try:
        return pd.read_csv(path, encoding="utf-8")
    except Exception:
        pass
    try:
        return pd.read_csv(path, encoding="utf-8-sig", engine="python", on_bad_lines="skip")
    except Exception:
        return pd.DataFrame(columns=COLUMNS_SCHEMA)

def sanitizar_respuestas_csv():
    try:
        if os.path.exists(RESP_CSV) and os.path.getsize(RESP_CSV) > 0:
            df = _read_csv_robusto(RESP_CSV)
            for c in COLUMNS_SCHEMA:
                if c not in df.columns:
                    df[c] = pd.NA
            df = df[COLUMNS_SCHEMA]
            bkp = RESP_CSV + ".bkp"
            if not os.path.exists(bkp):
                import shutil
                shutil.copy2(RESP_CSV, bkp)
            df.to_csv(RESP_CSV, index=False, encoding="utf-8", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        st.warning(f"No se pudo sanitizar respuestas.csv: {e}")

sanitizar_respuestas_csv()

if st.session_state.auth is None:
    with st.form("login"):
        st.subheader("Iniciar sesión")
        u = st.text_input("Usuario")
        p = st.text_input("Contraseña", type="password")
        submit = st.form_submit_button("Ingresar")
        if submit:
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

# Carga AOI y Municipios CAR
aoi_gdf, _, _, _ = load_aoi()
mun_gdf, mun_list, mun_name_col = load_municipios_car_auto()

# ---------------- Botones globales ----------------
colA, colB, colC = st.columns([2, 1, 1])
with colB:
    if st.button("Toda la jurisdicción", key="btn_toda_jurisdiccion"):
        st.session_state.muni_sel = mun_list[:]
        st.session_state._mun_sel_all = True
        st.success("Se seleccionaron todos los municipios.")
with colC:
    if st.button("Limpiar selección", key="btn_limpiar_sel"):
        st.session_state.muni_sel = []
        st.session_state._mun_sel_all = False
        st.info("Selección de municipios limpiada.")

# --------- Formulario ----------
st.header("Encuesta")
st.caption("Puedes responder varias veces; se guarda el historial pero se usa tu última respuesta para el mapa y KPIs.")

uploaded_geom = None
lon_pt, lat_pt = None, None
municipios_detectados = []

with st.form("encuesta_form"):
    # Proyecto
    opciones_proyecto = {
        "1":"2024CSE2735","2":"2024CSE1227","3":"2024CSE822","4":"2024CSE1154","5":"2024CSE1151","6":"2024CSE1495",
        "7":"2024CSE886","8":"2024CSE1182","9":"2024CSE1106","10":"2024CSE684","11":"2024CSE2712","12":"2021CCO3364",
        "13":"2024CSE1164","14":"2024CSE812","15":"2024CSE2022","16":"2024CSE1259","17":"2024CSE1848","18":"2024CSE1989",
        "19":"2024CSE1569","20":"2024CSE1821","21":"2024CSE675","22":"2024CSE1031","23":"2024CSE1282","24":"2024CSE1532",
        "25":"2024CSE2054","26":"2024CSE1308","27":"2024CSE896","28":"2024CSE2714","29":"2024CSE1080","30":"2024CSE2536",
        "31":"2024CSE1058","32":"2024CSE1163","33":"2024CSE1146","34":"2024CSE1101","35":"2024CSE2684","36":"2024CSE2818",
        "37":"2024CSE2761","38":"2024CSE1885","39":"2024CSE815","40":"2024CSE2617","41":"2024CSE1131","42":"2024CSE2717",
        "43":"2024CSE846","44":"2024CSE881","45":"2024CSE1264","46":"2024CSE1631","47":"2024CSE2736","48":"2024CSE2670",
        "49":"2024CSE1073","50":"2024CSE2630","51":"2024CSE1274","52":"2024CSE1611","53":"2024CSE992","54":"2024CSE1442",
        "55":"2024CSE2631","56":"2024CSE443","57":"2024CSE1424","58":"2024CSE1302","59":"2024CSE1122","60":"2024CSE1121",
        "61":"2024CSE1277","62":"2024CSE2673","63":"2024CSE391","64":"2024CSE1837","65":"2024CSE1281","66":"2024CSE1145",
        "67":"2024CSE1533","68":"2024CSE969","69":"2024CSE1872","70":"2024CSE662","71":"2024CSE2709","72":"2024CSE374",
        "73":"2024CSE1678","74":"2024CSE1708","75":"2024CSE824","76":"2024CSE651","77":"2024CSE1572","78":"2020CSE2628",
        "79":"2024CSE920","80":"2024CSE851","81":"2024CSE1959","82":"2024CSE1060","83":"2024CSE1437","84":"2024CSE2353",
        "85":"2024CSE1657","86":"2024CSE2756","87":"2024CSE1090","88":"2024CSE1271","89":"2024CSE2551","90":"2024CSE981",
        "91":"2024CSE2758","92":"2024CSE2534","93":"2024CSE1219","94":"2024CSE2016","95":"2024CSE1373","96":"2024CSE1847",
        "97":"2024CSE1408","98":"2024CSE1028","99":"2024CSE396","100":"2024CSE2753","101":"2024CSE2535","102":"2024CSE1248",
        "103":"2024CSE1585","104":"2024CSE2791","105":"2024CSE882","106":"2024CSE2802","107":"2024CSE2606","108":"2024CSE2718",
        "109":"2024CSE2720","110":"2024CSE829","111":"2024CSE2675","112":"2024CSE1413","113":"2024CSE524","114":"2024CSE1104",
        "115":"2024CSE1011","116":"2024CSE666","117":"2024CSE1194","118":"2023CSE2849","119":"2024CSE2708","120":"2024CSE1617",
        "121":"2024CSE894","122":"2024CSE2819","123":"2024CSE2724","124":"2024CSE2719","125":"2024CSE2607","126":"2024CSE2778",
        "127":"2024CSE1416","128":"2024CSE1388","129":"2024CSE1977","130":"2024CSE798","131":"2024CSE1136","132":"2024CSE1160",
        "133":"2024CSE380","134":"2024CSE1952","135":"2024CSE795","136":"2024CSE1304","137":"2021COV2533","138":"2024CSE985",
        "139":"2024CSE1486","140":"2024CSE468","141":"2024CSE626","142":"2021COB3365","143":"2024CIN1630","144":"2022CIN1630",
        "145":"2024CSE681","146":"2024CSE1110","147":"2024CSE653","148":"2024CSE830","149":"2024CSE990","150":"2024CSE2864",
        "151":"2024CSE2895","152":"2024CSE2902","153":"2024CSE2913","154":"2024CSE2856","155":"2024CSE2824","156":"2024CSE2896",
        "157":"2024CSE2865","158":"2024CSE2870","159":"2024CSE2906","160":"2024CSE2951","161":"2024CSE2905","162":"2024CSE2884",
        "163":"2024CSE2953","164":"2024CSE2996","165":"2024CSE3103","166":"2024COV3062","167":"2025CSE240","168":"2025CSE648",
        "169":"2025CSE46","170":"2025CSE75","171":"2025CSE647","172":"2025CSE564","173":"2025CSE586","174":"2025CSE633",
        "175":"2025CSE429","176":"2025CSE37","177":"2025CSE95","178":"2025CSE180","179":"2025CSE410","180":"2025CSE350",
        "181":"2025CSE617","182":"2025CSE279","183":"2025CSE664","184":"2025CSE683","185":"2025CSE78","186":"2025CSE319",
        "187":"2025CSE428","188":"2025CSE378","189":"2025CSE441","190":"2025CSE546","191":"2025CSE318","192":"2025CSE778",
        "193":"2025CSE81","194":"2025CSE563","195":"2025CSE782","196":"2025CSE430","197":"2025CSE423","198":"2025CSE800",
        "199":"2025CSE236","200":"2025CSE59","201":"2025CSE115","202":"2025CSE73","203":"2025CSE431","204":"2025CSE494",
        "205":"2025CSE406","206":"2025CSE497","207":"2025CSE575","208":"2025CSE731","209":"2025CSE748","210":"2025CSE351",
        "211":"2025CSE418","212":"2025CSE416","213":"2025CSE696","214":"2025CSE730","215":"2025CSE762","216":"2025CSE468",
        "217":"2025CSE593","218":"2025CSE220","219":"2025CSE432","220":"2025CSE72","221":"2025CSE253","222":"2025CSE625",
        "223":"2025CSE774","224":"2025CSE415","225":"2025CSE732","226":"2025CSE498","227":"2025CSE632","228":"2025CSE746",
        "229":"2025CSE1009","230":"2025CSE1264","231":"2025CSE902","232":"2025CSE1093","233":"2025CSE1416","234":"2025CSE1002",
        "235":"2025CSE893","236":"2025CSE977","237":"2025CSE1366","238":"2025CSE868","239":"2025CSE839","240":"2025CSE858",
        "241":"2025CSE1006","242":"2025CSE849","243":"2025CSE841","244":"2025CSE891","245":"2025CSE1003","246":"2025CSE1007",
        "247":"2025CSE1014","248":"2025CSE1232","249":"2025CSE1207","250":"2025CSE1263","251":"2025CSE840","252":"2025CSE913",
        "253":"2025CSE937","254":"2025CSE1205","255":"2025CSE966","256":"2025CSE987","257":"2025CSE1191","258":"2025CSE823",
        "259":"2025CSE1334","260":"2025CSE846","261":"2025CSE813","262":"2025CSE967","263":"2025CSE851","264":"2025CSE847",
        "265":"2025CSE985","266":"2025CSE900","267":"2025CSE1206","268":"2025CSE1365","269":"2025CSE869","270":"2025CSE951",
        "271":"2025CSE845","272":"2024COB3104","273":"2025CSE871","274":"2025CSE1154","275":"2025CSE1290","276":"2025CSE1405",
        "277":"2025CSE964","278":"2025CSE922","279":"2025CSE944","280":"2025CSE1018","281":"2025CSE1020","282":"2025CSE1335",
        "283":"2025CSE1375","284":"2025CSE1566","285":"2025CSE895","286":"2025CSE1430","287":"2025CSE850","288":"2025CSE1623",
        "289":"2025CSE1851","290":"2025CIN1737","291":"2025COB1736","292":"2025CIN974","293":"2025COB975","294":"2025CIN1174",
        "295":"2025CIN1166","296":"2025CCO1024","297":"2025CSE1596","298":"2025CSE1696","299":"2025COB1173","300":"2025COB1761",
        "301":"2024CSE3083","302":"2025CSE1616","303":"2025CSE1855","304":"2025CSE1610","305":"2025CSE1750","306":"2025CSE1832",
        "307":"2025CSE1622","308":"2025CIN1306","309":"2025COB1225","310":"2025CIN2222","311":"2025CIN1268","312":"2025CSE2260",
        "313":"2025CSE1138","314":"2025CSE2240","315":"2025CSE2037","316":"2025CIN1725","317":"2025CSE2199","318":"2025CSE2242",
        "319":"2025CSE2185","320":"2025CSE2126","321":"2025CSE2179","322":"2025CSE2252","323":"2025CSE2292","324":"2025CSE2220",
        "325":"2025CSE2374","326":"2025CSE2398","327":"2022CCO3647","328":"2025CSE2481","329":"2025CSE2597","330":"2025CSE2694",
        "331":"2025CSE2782","332":"2025CSE2752","333":"2025CSE2839","334":"2025CSE2835","335":"2025CSE2838","336":"2025CSE2853",
        "337":"2025CSE2864","338":"2025CSE2846","339":"2025COB1225-C1","340":"2025CSE2836","341":"2025CSE1610-C1",
        "342":"2025CSE2854","343":"2025CSE2848","344":"2025CSE2837","345":"2025CSE2852","346":"2025CSE2847","347":"2025CSE2843",
        "348":"2025CSU2936","349":"2025CSE2867","350":"2025CSE2926","351":"2025CSE2868","352":"2025CSE1623-C1",
        "353":"2025CSE1263-C1","354":"2025CSE3010","355":"2025CSE3049","356":"2025CSE3056","357":"2020CSE2628","358":"2021CCO3364",
        "359":"2021COV2533","360":"2023CSE3086","361":"2022CCO3747","362":"2023CSE2850","363":"2023CSE906","364":"2022CCO3647"
    }
    proyecto_valores = list(opciones_proyecto.values())
    proyecto_nombre = st.selectbox("Nùmero del contrato", proyecto_valores)
    proyecto_key = [k for k, v in opciones_proyecto.items() if v == proyecto_nombre][0]

    costo_proyecto = st.number_input("Costo del proyecto (COP)", min_value=0.0, step=1000.0, format="%.2f")

    avance_proyecto = st.number_input("Avance fisico del proyecto (%)", min_value=0, max_value=100, step=1)

    fecha_dilig = st.date_input("Fecha de diligenciamiento", value=date.today())

    # Municipios
    st.markdown("**Municipios del proyecto**")
    st.write("Opcional: sube un .zip con shp,dbf,shx,prj o un .kmz/.kml para detectar municipios automáticamente.")
    geo_file = st.file_uploader("Archivo geográfico (opcional)", type=["zip", "kmz", "kml"])
    preview_clicked = st.form_submit_button("🔎 Previsualizar capa y detectar municipios", type="secondary")

    if preview_clicked:
        if geo_file is None:
            st.warning("Sube un archivo geográfico para previsualizar.")
        else:
            try:
                gdf = read_geo_upload(geo_file)
                st.session_state.uploaded_geom = gdf
                try:
                    b = gdf.total_bounds
                    m_prev = folium.Map(location=[(b[1]+b[3])/2, (b[0]+b[2])/2], zoom_start=10)
                    gdf_viz = _drop_datetime_cols_for_folium(gdf)
                    folium.GeoJson(gdf_viz.to_json(), name="Capa cargada", style_function=lambda x: {"fillOpacity": 0.15, "weight": 2}).add_to(m_prev)
                    m_prev.fit_bounds([[b[1], b[0]], [b[3], b[2]]])
                    st_folium(m_prev, height=450, width=900)
                except Exception as e:
                    st.warning(f"No se pudo dibujar vista previa: {e}")
                rp = representative_point(gdf.geometry.iloc[0])
                if rp is not None:
                    lon_pt, lat_pt = rp.x, rp.y
                    st.session_state.lonlat_pt = (lon_pt, lat_pt)
                else:
                    st.session_state.lonlat_pt = (None, None)
                if mun_gdf is not None:
                    municipios_detectados = municipios_por_interseccion(gdf, mun_gdf, mun_name_col)
                    st.session_state.mun_detectados = municipios_detectados[:]
                    if municipios_detectados:
                        st.success("Municipios detectados: " + ", ".join(municipios_detectados))
                        st.session_state.muni_sel = sorted(list(set(st.session_state.muni_sel) | set(municipios_detectados)))
                    else:
                        st.warning("No se detectaron municipios por intersección.")
                else:
                    st.info("No hay archivo de municipios CAR. Usando lista de ejemplo.")
            except Exception as e:
                st.error(f"No se pudo leer el archivo: {e}")

    municipios_seleccionados = st.multiselect(
        "Municipios CAR (En caso de que sea toda la jurisdicciòn seleccionar la opciòn del botòn Toda la jurisdicciòn al inicio del formulario)",
        options=mun_list,
        default=st.session_state.muni_sel,
        key="muni_sel"
    )
    if set(municipios_seleccionados) != set(mun_list):
        st.session_state._mun_sel_all = False

    # ---- inversión: solo radio, sin pregunta condicional ----
    st.markdown("**Inversión por municipios**")
    inversion_equidad = st.radio(
        "¿La inversión se distribuye equitativamente entre los municipios? por favor mencionar por municipio el valor de la inversiòn de la suiguiente manera municipio, valor ( Ejemplo: SOACHA 1250000; SUTATAUSA 1000000; SIMIJICA 13000000)  ",
        options=["Si", "No"],
        index=0,
        horizontal=True
    )
    inversion_distribucion = ""  # se mantiene vacío (sin campo adicional)

    # Comentario libre (puedes usarlo para detallar distribución si quieres)
    comentario = st.text_area("Si no se distribuye equitativamente el recurso ¿Cómo se distribuye?")

    submitted = st.form_submit_button("Enviar respuesta")

# Validaciones
if submitted and not proyecto_nombre:
    st.error("Debes seleccionar un nombre de proyecto.")
    st.stop()
if submitted and len(st.session_state.muni_sel) == 0:
    st.error("Debes seleccionar al menos un municipio (o subir un archivo y previsualizar para detectar municipios).")
    st.stop()

# Guardado (y subida a Dropbox si aplica)
if submitted:
    ts = datetime.utcnow().isoformat()
    ts_tag = datetime.utcnow().strftime('%Y%m%dT%H%M%S')

    uploaded_geom = st.session_state.uploaded_geom
    if uploaded_geom is None and 'geo_file' in locals() and geo_file is not None:
        try:
            uploaded_geom = read_geo_upload(geo_file)
            st.session_state.uploaded_geom = uploaded_geom
        except Exception:
            uploaded_geom = None

    lon_pt, lat_pt = st.session_state.lonlat_pt

    archivo_geo_nombre = None
    archivo_geo_dropbox_path = None
    archivo_geo_link = None
    folder = st.secrets.get("dropbox_folder", "/ENCUESTA DNR FINAL")

    if 'geo_file' in locals() and geo_file is not None:
        try:
            archivo_geo_nombre = geo_file.name
            drop_name = f"uploads/{user['username']}_{ts_tag}_{archivo_geo_nombre}"
            tmp_path = os.path.join(tempfile.gettempdir(), f"{ts_tag}_{archivo_geo_nombre}")
            with open(tmp_path, "wb") as f:
                f.write(geo_file.getbuffer())
            archivo_geo_link, _ = dropbox_upload_or_update(tmp_path, dest_name=drop_name, dest_folder=folder)
            archivo_geo_dropbox_path = ("/" + folder.strip("/")) + "/" + drop_name.strip("/")
        except Exception as e:
            st.warning(f"No se pudo subir el archivo geográfico original: {e}")

    if (lon_pt is not None) and (lat_pt is not None):
        save_geo_point(user["username"], lon_pt, lat_pt, ts)
    else:
        save_geo_point(user["username"], None, None, ts)

    resp = {
        "timestamp": ts,
        "fecha_diligenciamiento": fecha_dilig.isoformat(),
        "username": user["username"],
        "name": user["name"],
        "proyecto_key": proyecto_key,
        "proyecto_nombre": proyecto_nombre,
        "municipios_proyecto": "; ".join(st.session_state.muni_sel) if st.session_state.muni_sel else "",
        "costo_proyecto_cop": costo_proyecto,
        "avance_proyecto_pct": avance_proyecto,
        "comentario": comentario,
        "modo_municipios": "archivo" if uploaded_geom is not None else "manual",
        "archivo_geo_nombre": archivo_geo_nombre or "",
        "archivo_geo_dropbox_path": archivo_geo_dropbox_path or "",
        "archivo_geo_link": archivo_geo_link or "",
        "inversion_equidad": inversion_equidad,
        "inversion_distribucion": inversion_distribucion,  # queda vacío
    }
    save_response(resp)

    try:
        if os.path.exists(RESP_CSV):
            link, status = dropbox_upload_or_update(RESP_CSV, dest_name="respuestas.csv", dest_folder=folder)
            st.success(f"respuestas.csv en Dropbox ({status}). " + (f"Descargar (temporal): {link}" if link else ""))
        if os.path.exists(GEO_CSV):
            link, status = dropbox_upload_or_update(GEO_CSV, dest_name="respuestas_geo.csv", dest_folder=folder)
            st.success(f"respuestas_geo.csv en Dropbox ({status}). " + (f"Descargar (temporal): {link}" if link else ""))
    except Exception as e:
        st.warning(f"No se pudo subir a Dropbox: {e}")

    st.success("Respuesta guardada.")
    st.session_state._mun_sel_all = False

# --------- Resultados / lectura robusta ----------
def load_historial():
    if os.path.exists(RESP_CSV) and os.path.getsize(RESP_CSV) > 0:
        df = _read_csv_robusto(RESP_CSV)
        for c in COLUMNS_SCHEMA:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[COLUMNS_SCHEMA]
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        return df.sort_values("timestamp", na_position="first")
    return pd.DataFrame(columns=COLUMNS_SCHEMA)

def latest_by_user(df):
    if df.empty:
        return df
    return df.sort_values("timestamp").drop_duplicates("username", keep="last")

try:
    df_hist = load_historial()
except Exception as e:
    st.warning(f"No se pudo leer respuestas.csv (se omitieron líneas problemáticas): {e}")
    df_hist = pd.DataFrame(columns=COLUMNS_SCHEMA)

df_latest = latest_by_user(df_hist)

def _fmt_cop(v):
    try:
        v = float(v)
        return f"${v:,.0f}".replace(",", ".")
    except Exception:
        return "—"

st.header("Resultados (última por usuario)")
col1, col2, col3 = st.columns(3)
if not df_latest.empty:
    col1.metric("Usuarios con última respuesta", df_latest["username"].nunique())
    if "municipios_proyecto" in df_latest.columns:
        muni_total = set()
        for s in df_latest["municipios_proyecto"].dropna():
            for m in [x.strip() for x in str(s).split(";") if x.strip()]:
                muni_total.add(m)
        col2.metric("Municipios (últimas)", len(muni_total))
    if "avance_proyecto_pct" in df_latest.columns:
        try:
            avance_prom = pd.to_numeric(df_latest["avance_proyecto_pct"], errors="coerce").mean()
            if pd.notna(avance_prom):
                col3.metric("Avance promedio", f"{avance_prom:.1f}%")
            else:
                col3.metric("Avance promedio", "—")
        except Exception:
            col3.metric("Avance promedio", "—")
else:
    st.info("Aún no hay respuestas.")

st.subheader("Tabla - Última respuesta por usuario")
if not df_latest.empty:
    try:
        st.dataframe(
            df_latest,
            use_container_width=True,
            hide_index=True,
            column_config={"archivo_geo_link": st.column_config.LinkColumn("Archivo geográfico (Dropbox)")}
        )
    except Exception:
        st.dataframe(df_latest, use_container_width=True, hide_index=True)

st.subheader("Historial completo")
if not df_hist.empty:
    try:
        st.dataframe(
            df_hist,
            use_container_width=True,
            hide_index=True,
            column_config={"archivo_geo_link": st.column_config.LinkColumn("Archivo geográfico (Dropbox)")}
        )
    except Exception:
        st.dataframe(df_hist, use_container_width=True, hide_index=True)

# --------- Mapa general ----------
st.header("Mapa general")

CUNDINAMARCA_CENTER_LAT = 4.85
CUNDINAMARCA_CENTER_LON = -74.30
CUNDINAMARCA_ZOOM = 8

m = folium.Map(location=[CUNDINAMARCA_CENTER_LAT, CUNDINAMARCA_CENTER_LON], zoom_start=CUNDINAMARCA_ZOOM)

if aoi_gdf is not None:
    folium.GeoJson(_drop_datetime_cols_for_folium(aoi_gdf).to_json(), name="AOI", style_function=lambda x: {"fillOpacity": 0.08, "weight": 2}).add_to(m)

if os.path.isdir(GEOM_DIR):
    for fn in os.listdir(GEOM_DIR):
        if fn.endswith("_last.geojson"):
            try:
                gtmp = gpd.read_file(os.path.join(GEOM_DIR, fn))
                if gtmp.crs is None:
                    gtmp.set_crs(4326, inplace=True)
                else:
                    gtmp = gtmp.to_crs(4326)
                folium.GeoJson(_drop_datetime_cols_for_folium(gtmp).to_json(), name=fn.replace("_last.geojson",""), style_function=lambda x: {"fillOpacity": 0.08, "weight": 2}).add_to(m)
            except Exception:
                pass

pts = {}
if os.path.exists(GEO_CSV) and not df_latest.empty:
    g = pd.read_csv(GEO_CSV)
    if "timestamp" in g.columns:
        g["timestamp"] = pd.to_datetime(g["timestamp"], errors="coerce")
        g_last = g.sort_values("timestamp").drop_duplicates("username", keep="last")
        pts = {r["username"]: (r.get("lon"), r.get("lat")) for _, r in g_last.iterrows()}

if not df_latest.empty:
    for _, r in df_latest.iterrows():
        u = r["username"]
        lon, lat = None, None
        if u in pts and pd.notna(pts[u][0]) and pd.notna(pts[u][1]):
            lon, lat = pts[u]
        popup = folium.Popup(
            f"<b>{r.get('name','')}</b> ({u})"
            f"<br/>{r.get('proyecto_nombre','')}"
            f"<br/>Fecha: {r.get('fecha_diligenciamiento','')}"
            f"<br/>Municipios: {r.get('municipios_proyecto','')}"
            f"<br/>Costo: {_fmt_cop(r.get('costo_proyecto_cop'))}"
            f"<br/>Avance: {r.get('avance_proyecto_pct','')}%",
            max_width=350
        )
        if (lon is not None) and (lat is not None):
            folium.CircleMarker(location=[lat, lon], radius=6, popup=popup).add_to(m)

st_folium(m, height=520, width=1000)





