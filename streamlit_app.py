# streamlit_app.py
import difflib
from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Muse Motion", page_icon=":bar_chart:", layout="wide")

EXPECTED_COLS = [
    "vin",
    "city",
    "year",
    "make",
    "model",
    "vehicle_type",
    "eligibility",
    "electric_range",
    "vehicle_id",
    "location",
    "utility",
]

def normalize_cols(cols):
    """Force to string, strip, lowercase, replace spaces with underscores."""
    cols = list(map(str, cols))
    cols = [c.strip().lower().replace(" ", "_") for c in cols]
    return cols

def score_header_candidate(cols, expected=EXPECTED_COLS):
    """Score candidate header row based on matches to expected columns."""
    cols_norm = normalize_cols(cols)
    existing = set(cols_norm)
    score = 0
    for e in expected:
        if e in existing:
            score += 2
        else:
            if difflib.get_close_matches(e, cols_norm, n=1, cutoff=0.7):
                score += 1
    return score

@st.cache_data
def get_data_from_excel_auto_header(path: str = "musemotion_data.xlsx", sheet_name="Sheet1"):
    """Read Excel and auto-detect header row."""
    if not Path(path).exists():
        raise FileNotFoundError(f"Excel file not found at: {path}")

    best_score = -1
    best_header = None

    for header_row in range(0, 11):
        try:
            sample = pd.read_excel(io=path, engine="openpyxl", sheet_name=sheet_name, header=header_row, nrows=0)
        except Exception:
            continue
        cols = list(sample.columns)
        score = score_header_candidate(cols)
        if score > best_score:
            best_score = score
            best_header = header_row

    if best_score > 0 and best_header is not None:
        try:
            df = pd.read_excel(io=path, engine="openpyxl", sheet_name=sheet_name, header=best_header)
            df.columns = normalize_cols(df.columns)
            return df
        except Exception as e:
            raise RuntimeError(f"Failed reading with detected header {best_header}: {e}")

    # fallback
    try:
        df = pd.read_excel(io=path, engine="openpyxl", sheet_name=sheet_name, skiprows=3)
        df.columns = normalize_cols(df.columns)
        return df
    except Exception as e:
        raise RuntimeError(f"Could not detect header row automatically. Also failed fallback read: {e}")

# --- Load data ---
try:
    df = get_data_from_excel_auto_header()
except Exception as e:
    st.error(str(e))
    st.stop()

# Sidebar debug
st.sidebar.header("Debug / Display Options")
show_sample = st.sidebar.checkbox("Show dataframe columns & sample", value=False)
show_mapping = st.sidebar.checkbox("Show column mapping", value=False)
show_header_detection = st.sidebar.checkbox("Show header-detection info", value=False)

if show_sample:
    st.write("Columns found:", df.columns.tolist())
    st.write(df.head())

if show_header_detection:
    st.info("Header detection attempted header rows 0..10 and picked the best match.")

# Defensive column mapping
existing = set(map(str, df.columns))
mapping = {}
for col in EXPECTED_COLS:
    if col in existing:
        mapping[col] = col
    else:
        matches = difflib.get_close_matches(col, list(existing), n=1, cutoff=0.6)
        mapping[col] = matches[0] if matches else None

if show_mapping:
    st.write("Column mapping (expected -> actual):", mapping)

required = ["city", "model", "make"]
missing_required = [r for r in required if not mapping.get(r)]
if missing_required:
    st.error(
        "Missing required column(s): "
        f"{missing_required}. Columns available: {sorted(list(existing))}. "
        "Check header row in Excel or adjust skiprows."
    )
    st.stop()

# Assign columns
vin_col = mapping.get("vin")
city_col = mapping.get("city")
year_col = mapping.get("year")
make_col = mapping.get("make")
model_col = mapping.get("model")
vehicle_type_col = mapping.get("vehicle_type")
eligibility_col = mapping.get("eligibility")
range_col = mapping.get("electric_range")
vehicle_id_col = mapping.get("vehicle_id")
location_col = mapping.get("location")
utility_col = mapping.get("utility")

# Coerce numeric columns
if year_col and year_col in df.columns:
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
if range_col and range_col in df.columns:
    df[range_col] = pd.to_numeric(df[range_col], errors="coerce")

# --- Sidebar filters ---
st.sidebar.header("Please Filter Here:")

def sorted_unique_or_empty(col_name):
    if col_name and col_name in df.columns:
        vals = df[col_name].dropna().unique().tolist()
        try:
            return sorted(vals)
        except Exception:
            return list(vals)
    return []

city_options = sorted_unique_or_empty(city_col)
model_options = sorted_unique_or_empty(model_col)
make_options = sorted_unique_or_empty(make_col)

if not city_options or not model_options or not make_options:
    st.error("Required filter columns have no valid values. Check dataset or toggle debug options.")
    st.stop()

city = st.sidebar.multiselect("Select the City:", options=city_options, default=city_options)
model = st.sidebar.multiselect("Select the Model Type:", options=model_options, default=model_options)
make = st.sidebar.multiselect("Select the Make:", options=make_options, default=make_options)

# --- Filter dataframe ---
df_selection = df[
    df[city_col].isin(city) &
    df[model_col].isin(model) &
    df[make_col].isin(make)
].copy()

if df_selection.empty:
    st.warning("No data available based on the current filter settings!")
    st.stop()

# --- Main page ---
st.title(":bar_chart: Muse Motion Electric Vehicles")
st.markdown("##")

total_vehicles = len(df_selection)
average_year = (
    round(df_selection[year_col].mean(), 1)
    if (year_col and year_col in df_selection.columns and not df_selection[year_col].dropna().empty)
    else "N/A"
)
average_electric_range = (
    round(df_selection[range_col].mean(), 2)
    if (range_col and range_col in df_selection.columns and not df_selection[range_col].dropna().empty)
    else "N/A"
)

left_column, middle_column, right_column = st.columns(3)
with left_column:
    st.subheader("Total Vehicles:")
    st.subheader(f"{total_vehicles:,}")
with middle_column:
    st.subheader("Average Year:")
    st.subheader(f"{average_year}")
with right_column:
    st.subheader("Average Electric Range:")
    st.subheader(f"{average_electric_range}")

st.markdown("---")

vehicles_by_make = df_selection.groupby(by=make_col).size().sort_values(ascending=True)
fig_vehicle_make = px.bar(
    x=vehicles_by_make.values,
    y=vehicles_by_make.index,
    orientation="h",
    title="<b>Vehicles by Make</b>",
    template="plotly_white",
)
fig_vehicle_make.update_layout(plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(showgrid=False))

make_by_city = df_selection.groupby(by=city_col).size().sort_values(ascending=False)
fig_make_city = px.bar(
    x=make_by_city.values,
    y=make_by_city.index,
    orientation="h",
    title="<b>Vehicle counts by City</b>",
    template="plotly_white",
)
fig_make_city.update_layout(plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(showgrid=False))

left_column, right_column = st.columns(2)
left_column.plotly_chart(fig_make_city, use_container_width=True)
right_column.plotly_chart(fig_vehicle_make, use_container_width=True)

# ---- Hide Streamlit style ----
hide_st_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)
