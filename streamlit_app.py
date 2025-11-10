import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
import plotly.express as px 
import altair as alt
from sqlalchemy import create_engine, text
from typing import Optional

# emojis: https://www.webfx.com/tools/emoji-cheat-sheet/
st.set_page_config(page_title="Muse Motion", page_icon=":bar_chart:", layout="wide")

# read excel
@st.cache_data
def get_data_from_excel():
    df = pd.read_excel(
        io="musemotion_data.xlsx",
        engine="openpyxl",
        sheet_name="Sheet1",
        skiprows=3,
        usecols="A:K",
        nrows=1499,
    )

df = get_data_from_excel()

# sidebar
st.sidebar.header("Please Filter Here:")
city = st.sidebar.multiselect(
    "Select the City:",
    options=df["city"].unique(),
    default=df["city"].unique()
)

customer_type = st.sidebar.multiselect(
    "Select the Model Type:",
    options=df["model"].unique(),
    default=df["model"].unique(),
)

gender = st.sidebar.multiselect(
    "Select the Make:",
    options=df["make"].unique(),
    default=df["make"].unique()
)

df_selection = df.query(
    "city == @city & model ==@model & make == @make"
)

# Check if the dataframe is empty:
if df_selection.empty:
    st.warning("No data available based on the current filter settings!")
    st.stop() # This will halt the app from further execution.

# app mainpage
st.title(":bar_chart: Muse Motion Electric Vehicles")
st.markdown("##")

# kpi
total_vehicles = int(df_selection["model"].sum())
average_year = round(df_selection["year"].mean(), 1)
average_electric_range = round(df_selection["electric_range"].mean(), 2)

left_column, middle_column, right_column = st.columns(3)
with left_column:
    st.subheader("Total Vehicles:")
    st.subheader(f"{total_vehicles:,}")
with middle_column:
    st.subheader("Average Year:")
    st.subheader(f"{average_year}")
with right_column:
    st.subheader("Average Electric Range:")
    st.subheader(f" {average_electric_range}")

st.markdown("""---""")

# Electric vehicles by make (bar chart)
vehicles_by_make = df_selection.groupby(by=["make"])[["model"]].sum().sort_values(by="model")
fig_vehicle_make = px.bar(
    vehicles_by_make,
    x="model",
    y=vehicles_by_make.index,
    orientation="h",
    title="<b>Vehicle by Make/b>",
    color_discrete_sequence=["#0083B8"] * len(vehicles_by_make),
    template="plotly_white",
)
fig_vehicle_make.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis=(dict(showgrid=False))
)

# make by city
make_by_city = df_selection.groupby(by=["city"])[["make"]].sum()
fig_make_city = px.bar(
    make_by_city,
    x=make_by_city.index,
    y="make",
    title="<b>Vehicle makes in each city</b>",
    color_discrete_sequence=["#0083B8"] * len(sales_by_hour),
    template="plotly_white",
)
fig_make_city.update_layout(
    xaxis=dict(tickmode="linear"),
    plot_bgcolor="rgba(0,0,0,0)",
    yaxis=(dict(showgrid=False)),
)

left_column, right_column = st.columns(2)
left_column.plotly_chart(fig_make_city, use_container_width=True)
right_column.plotly_chart(fig_vehicle_make, use_container_width=True)


# ---- HIDE STREAMLIT STYLE ----
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)
