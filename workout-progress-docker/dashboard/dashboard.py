import os
import psycopg2
import pandas as pd
import streamlit as st
import plotly.express as px

from datetime import datetime

# configure the default layout to Wide
st.set_page_config(layout="wide")


def get_db_connection():
    # read the required env vars
    WORKOUT_DB_HOST = os.getenv("WORKOUT_DB_HOST")
    WORKOUT_DB_PORT = os.getenv("WORKOUT_DB_PORT")
    WORKOUT_DB_NAME = os.getenv("WORKOUT_DB_NAME")
    WORKOUT_DB_USER = os.getenv("WORKOUT_DB_USER")
    WORKOUT_DB_PASSWORD = os.getenv("WORKOUT_DB_PASSWORD")
    
    # establish connection to the database
    pg_conn = psycopg2.connect(
        dbname=WORKOUT_DB_NAME,
        user=WORKOUT_DB_USER,
        password=WORKOUT_DB_PASSWORD,
        host=WORKOUT_DB_HOST,
        port=WORKOUT_DB_PORT,
        sslmode="require"
    )

    return pg_conn


# function to fetch data for the MoM changes
@st.cache_data
def load_data_mom_changes():
    conn = get_db_connection()
    current_year = datetime.now().year
    current_month = datetime.now().month
    query = f"SELECT year, month_int, mcr.exercise, added_weight, last_month_reps, total_reps, mom_percentage_change, mom_absolute_change FROM prod.mom_changes_reps mcr WHERE ((year < {current_year}) OR (year = {current_year} AND month_int <= {current_month})) AND exercise IN ('pull-ups', 'chin-ups', 'australian pull-ups', 'push-ups', 'dips', 'straight bar dips', 'knee raises', 'leg raises', 'pike push-ups');"
    df_mom_changes = pd.read_sql(query, conn)
    conn.close()  # close connection to avoid leaks
    return df_mom_changes


# function to fetch data for the YoY changes
@st.cache_data
def load_data_yoy_changes():
    conn = get_db_connection()
    current_year = datetime.now().year
    query = f"SELECT year, ycr.exercise, added_weight, total_reps FROM prod.yearly_reps_per_exercise_weight ycr WHERE ((year <= {current_year}) AND exercise IN ('pull-ups', 'chin-ups', 'australian pull-ups', 'push-ups', 'dips', 'straight bar dips', 'knee raises', 'leg raises', 'pike push-ups'));"
    df_yoy_changes = pd.read_sql(query, conn)
    conn.close()
    return df_yoy_changes


# function to fetch data from the weekday frequency model
@st.cache_data
def load_weekday_frequency():
    conn = get_db_connection()
    query = "SELECT * FROM prod.workout_frequency_weekday WHERE year<2025;"
    df_weekday_frequency = pd.read_sql(query, conn)
    conn.close()
    return df_weekday_frequency


df_mom_changes = load_data_mom_changes()
df_yoy_changes = load_data_yoy_changes()
df_weekday_frequency = load_weekday_frequency()

# dashboard title
st.title("Workout Progress Dashboard 🤸🏼‍♂️")

# filters sidebar
st.sidebar.header("Filters")

# weight unit selection (kg/lbs)
weight_unit_selected = st.sidebar.radio("Select Weight Unit:", ["kg", "lbs"])

# setting defaults for display_unit and display_weight
df_mom_changes["display_unit"] = "kg"
df_yoy_changes["display_unit"] = "kg"
df_mom_changes["display_weight"] = df_mom_changes["added_weight"]  # defaults to kg
df_yoy_changes["display_weight"] = df_yoy_changes["added_weight"]  # defaults to kg

# correct value would be added_weight * 2.20462, but I want to keep it simple
if weight_unit_selected == "lbs":
    df_mom_changes["display_weight"] = (df_mom_changes["added_weight"] * 2.2).round(2)  # converts kg → lbs
    df_mom_changes["display_unit"] = "lbs"
    df_yoy_changes["display_weight"] = (df_yoy_changes["added_weight"] * 2.2).round(2)  # converts kg → lbs
    df_yoy_changes["display_unit"] = "lbs"

# apply weight and year filters
df_mom_changes_converted = df_mom_changes
df_yoy_changes_converted = df_yoy_changes

# weight filter dropdown
weights = sorted(df_mom_changes_converted["display_weight"].unique().tolist())  # Get unique weights
weight_selected = st.sidebar.selectbox("Select Added Weight (Total Reps/MoM):", weights)

# year selection
year_selected = st.sidebar.selectbox("Select a Year (Scatterplot/Stacked Bar Chart):", df_mom_changes["year"].unique())
df_mom_changes_filtered = df_mom_changes_converted[df_mom_changes_converted["display_weight"] == weight_selected]
df_yoy_changes_filtered = df_yoy_changes_converted[df_yoy_changes_converted["year"] == year_selected]

# for clearing cache
if st.sidebar.button('Refresh Data'):
    st.cache_data.clear()  # Clear the cache manually
st.sidebar.write("*DAGs run at 22:00 UTC.*")

# total monthly reps
st.subheader("Total Monthly Reps per Exercise")
df_mom_changes_filtered.loc[:, "date"] = pd.to_datetime(
    df_mom_changes_filtered["year"].astype(int).astype(str) + "-" + 
    df_mom_changes_filtered["month_int"].astype(int).astype(str), 
    format="%Y-%m"
)
fig = px.line(
    df_mom_changes_filtered,
    x="date",
    y="total_reps",
    color="exercise",
    title="Total Monthly Reps per Exercise",
    labels={"total_reps": "Total Reps", "date": "Date", "exercise": "Exercise"},
    hover_data=["exercise", "total_reps", "display_weight", "display_unit"],  # include weight in hover
    markers=True,
)

fig.update_traces(mode="lines+markers")
st.plotly_chart(fig)

# MoM % change
st.subheader("MoM % Change for each Exercise")
fig = px.line(
    df_mom_changes_filtered,
    x="date",
    y="mom_percentage_change",
    color="exercise",
    title="Month-on-Month % Change for Each Exercise",
    labels={"mom_percentage_change": "MoM % Change", "date": "Date", "exercise": "Exercise"},
    hover_data=["exercise", "display_weight", "display_unit"],  # include weight in hover
    markers=True
)

fig.update_traces(mode="lines+markers", connectgaps=True)
st.plotly_chart(fig)


# weight progression over time
st.subheader("Weight Progression per Exercise Bubble Chart")
df_mom_changes_converted = df_mom_changes_converted[df_mom_changes_converted["year"] == year_selected]
fig = px.scatter(
    df_mom_changes_converted,
    x="month_int", 
    y="display_weight", 
    size="total_reps", 
    color="exercise", 
    labels={"month_int": "Month", "display_weight": "Added Weight", "exercise": "Exercise"},
    title="Added Weight Progression Over Time",
    opacity=0.8,
    color_discrete_sequence=px.colors.qualitative.Dark24
)

# updates the x-axis to treat 'month_int' as a categorical variable
fig.update_layout(
    xaxis=dict(
        type='category',
        tickmode='array',
        tickvals=df_mom_changes_converted['month_int'].unique(),
        categoryorder='array',
        categoryarray=df_mom_changes_converted['month_int'].unique().tolist()
    )
)

st.plotly_chart(fig, use_container_width=True)

# total yearly reps
st.subheader("Total Yearly Reps per Exercise/Added Weight")
df_yoy_changes_filtered = df_yoy_changes_filtered.sort_values(by=["exercise","total_reps","display_weight"], ascending=True)

fig = px.bar(
    df_yoy_changes_filtered,
    x="total_reps",
    y="exercise",
    color="display_weight",
    text="total_reps",
    title="Stacked Bar Chart of Exercises by Weight",
    labels={"exercise": "Exercise", "total_reps": "Total Reps", "display_weight": "Added Weight"},
    color_continuous_scale=px.colors.sequential.Sunset[::-1]
)

st.plotly_chart(fig, use_container_width=True)

# workout weekday frequency
st.subheader("Workout Frequency per Weekday")
weekday_order = ["sunday", "saturday", "friday", "thursday", "wednesday", "tuesday", "monday"]

# treat weekday as a categorical variable with the custom order defined above
df_weekday_frequency["weekday"] = pd.Categorical(df_weekday_frequency["weekday"], categories=weekday_order, ordered=True)
fig_heatmap = px.density_heatmap(
    df_weekday_frequency, x="year", y="weekday", z="total_workouts",
    color_continuous_scale=px.colors.sequential.Teal, title="Workout Weekday Frequency Heatmap",
    category_orders={"weekday": weekday_order},
    labels={"year": "Year", "weekday": "Weekday", "total_workouts": "Total Workouts"}
)

# update the z-axis label explicitly, so it does not spell out 'sum of Total Workouts'
fig_heatmap.update_layout(
    coloraxis_colorbar_title="Total Workouts"
)
fig_heatmap.update_xaxes(type="category")
st.plotly_chart(fig_heatmap)
