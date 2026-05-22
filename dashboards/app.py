import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------------------------
# Page Configuration & Styling
# ---------------------------------------------------------
st.set_page_config(page_title="Happiness KPIs", layout="wide")

# ---------------------------------------------------------
# Database Connection
# ---------------------------------------------------------
DB_URI = "postgresql://user:password@localhost:5432/streaming_etl"

@st.cache_data(ttl=3) # Cache for 3 seconds for real-time feel
def load_data(query):
    engine = create_engine(DB_URI)
    return pd.read_sql(query, engine)

# ---------------------------------------------------------
# Dashboard Layout
# ---------------------------------------------------------
st.title("🌍 Happiness Predictions KPIs")

try:
    # --- ROW 1: Top Metrics & Donut Chart ---
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        st.subheader("Average Prediction Error")
        df_avg_error = load_data("SELECT * FROM vw_avg_prediction_error")
        avg_error = df_avg_error['avg_error'].iloc[0] if not df_avg_error.empty and pd.notnull(df_avg_error['avg_error'].iloc[0]) else 0.0
        st.metric(label="Mean Absolute Error", value=f"{avg_error:.4f}")

    with col2:
        st.subheader("Records Status")
        df_status = load_data("SELECT * FROM vw_processing_status")
        if not df_status.empty:
            total = df_status['count'].sum()
            st.metric(label="Total Processed", value=f"{total}")
        else:
            st.metric(label="Total Processed", value="0")

    with col3:
        if not df_status.empty:
            fig_donut = px.pie(
                df_status, 
                names='processing_status', 
                values='count', 
                hole=0.6,
                color='processing_status',
                color_discrete_map={'VALID': '#5BC0BE', 'INVALID_SCHEMA': '#E76F51', 'INVALID_VALUES': '#F4A261', 'PENDING': '#8D99AE'}
            )
            fig_donut.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', 
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#E0E0E0'),
                margin=dict(t=0, b=0, l=0, r=0),
                height=150
            )
            st.plotly_chart(fig_donut, use_container_width=True)

    st.markdown("<hr/>", unsafe_allow_html=True)

    # --- ROW 2: Features Drift & Predictions by Country ---
    col_l, col_r = st.columns([1, 1])

    with col_l:
        st.subheader("Features Drift Over Time")
        df_drift = load_data("SELECT * FROM vw_features_drift")
        if not df_drift.empty:
            # Melt dataframe for plotly express line chart
            df_melted = df_drift.melt(id_vars=['year'], value_vars=['avg_gdp', 'avg_family', 'avg_health', 'avg_freedom'], 
                                      var_name='Feature', value_name='Average Score')
            
            # Clean up feature names
            df_melted['Feature'] = df_melted['Feature'].str.replace('avg_', '')
            
            fig_drift = px.line(
                df_melted, x='year', y='Average Score', color='Feature', markers=True,
                color_discrete_sequence=['#5BC0BE', '#6FFFE9', '#3A506B', '#E76F51']
            )
            fig_drift.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#E0E0E0'),
                xaxis=dict(showgrid=True, gridcolor='#1C2541'), yaxis=dict(showgrid=True, gridcolor='#1C2541')
            )
            st.plotly_chart(fig_drift, use_container_width=True)

    with col_r:
        st.subheader("Predictions by Country")
        df_country = load_data("SELECT * FROM vw_predictions_by_country LIMIT 20")
        if not df_country.empty:
            # Combo Chart: Bars for Actual, Line for Predicted
            fig_country = go.Figure()
            fig_country.add_trace(go.Bar(
                x=df_country['country_name'], y=df_country['avg_actual_score'],
                name='Actual Score', marker_color='#3A506B'
            ))
            fig_country.add_trace(go.Scatter(
                x=df_country['country_name'], y=df_country['avg_predicted_score'],
                name='Predicted Score', mode='lines+markers', marker_color='#5BC0BE'
            ))
            fig_country.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#E0E0E0'),
                xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor='#1C2541'),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_country, use_container_width=True)

    st.markdown("<hr/>", unsafe_allow_html=True)

    # --- ROW 3: Happiness Trends & Scatter Plot ---
    col_bl, col_br = st.columns([1, 1])

    with col_bl:
        st.subheader("Happiness Score Trends")
        df_trends = load_data("SELECT * FROM vw_prediction_trends")
        if not df_trends.empty:
            fig_trends = go.Figure()
            fig_trends.add_trace(go.Scatter(
                x=df_trends['year'], y=df_trends['avg_actual_score'],
                name='Actual Score', mode='lines+markers', line=dict(color='#E76F51', width=3)
            ))
            fig_trends.add_trace(go.Scatter(
                x=df_trends['year'], y=df_trends['avg_predicted_score'],
                name='Predicted Score', mode='lines+markers', line=dict(color='#5BC0BE', width=3, dash='dot')
            ))
            # Dynamically adjust Y-axis to NOT start at 0 so variance is visible
            min_y = min(df_trends['avg_actual_score'].min(), df_trends['avg_predicted_score'].min()) * 0.95
            max_y = max(df_trends['avg_actual_score'].max(), df_trends['avg_predicted_score'].max()) * 1.05
            
            fig_trends.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#E0E0E0'),
                xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor='#1C2541', range=[min_y, max_y]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_trends, use_container_width=True)

    with col_br:
        st.subheader("Predicted vs Actual Scores")
        df_scatter = load_data("SELECT * FROM vw_predicted_vs_actual")
        if not df_scatter.empty:
            fig_scatter = px.scatter(
                df_scatter, x='actual_score', y='predicted_score', 
                color='prediction_error', size='prediction_error',
                color_continuous_scale='Tealgrn', opacity=0.7
            )
            fig_scatter.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#E0E0E0'),
                xaxis=dict(showgrid=True, gridcolor='#1C2541'), yaxis=dict(showgrid=True, gridcolor='#1C2541')
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

except Exception as e:
    st.error(f"Error fetching data. Please ensure Docker is running and Consumer has processed events. Details: {e}")
