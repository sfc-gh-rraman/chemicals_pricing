"""
Chemicals Pricing Dashboard
Page 3: Data Science Workbench (Technical View)

Persona: Pricing Analyst / Data Scientist
Action: "Validate the Demand Model"
DRD Reference: Section 5 - Page 3: Data Science Workbench

Visuals:
- Forecast Accuracy: Actual vs. Predicted Sales Volume
- Feature Importance: Impact of "Crude Price" and "Competitor Index" on Demand
- Hidden Discovery: The "Lagged Correlation" chart

Hidden Discovery:
"Demand for 'Polypropylene Grade B' in Southeast Asia is 90% correlated with 
'Crude Oil Brent' prices with a 3-week lag, allowing for predictive inventory 
positioning before the spike."
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from snowflake.snowpark.context import get_active_session

from utils.theme import (
    COLORS, PRODUCT_FAMILY_COLORS, REGION_COLORS,
    apply_dark_theme, metric_card, section_header, info_box, get_page_css
)

# Page configuration
st.set_page_config(
    page_title="Chemicals Pricing - Data Science",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown(get_page_css(), unsafe_allow_html=True)


@st.cache_resource
def get_session():
    return get_active_session()


@st.cache_data(ttl=300)
def load_demand_forecasts(_session):
    """Load demand forecast data."""
    query = """
    SELECT 
        product_id,
        region,
        forecast_period_start,
        predicted_volume_mt,
        prediction_lower_bound,
        prediction_upper_bound,
        prediction_confidence,
        historical_avg_volume,
        forecast_vs_history_pct,
        top_driver_1,
        top_driver_1_impact,
        model_mape
    FROM CHEMICALS_DB.CHEMICAL_OPS.DEMAND_FORECAST
    ORDER BY forecast_period_start DESC
    LIMIT 100
    """
    return _session.sql(query).to_pandas()


@st.cache_data(ttl=300)
def load_price_elasticity(_session):
    """Load price elasticity data."""
    query = """
    SELECT 
        product_id,
        region,
        elasticity_coefficient,
        elasticity_category,
        r_squared,
        data_points
    FROM CHEMICALS_DB.CHEMICAL_OPS.PRICE_ELASTICITY
    ORDER BY ABS(elasticity_coefficient) DESC
    """
    return _session.sql(query).to_pandas()


@st.cache_data(ttl=300)
def load_lagged_correlations(_session):
    """Load lagged correlation data for hidden discovery."""
    query = """
    SELECT 
        product_id,
        region,
        lag_weeks,
        correlation_coefficient,
        is_significant,
        is_optimal_lag,
        predicted_demand_change_pct
    FROM CHEMICALS_DB.CHEMICAL_OPS.LAGGED_CORRELATION
    ORDER BY product_id, region, lag_weeks
    """
    return _session.sql(query).to_pandas()


@st.cache_data(ttl=300)
def load_demand_history(_session, product_id: str = None, region: str = None):
    """Load historical demand with crude oil prices."""
    where_clause = "WHERE 1=1"
    if product_id:
        where_clause += f" AND product_id = '{product_id}'"
    if region:
        where_clause += f" AND region = '{region}'"
    
    query = f"""
    SELECT 
        demand_date,
        product_id,
        region,
        volume_mt,
        avg_price_per_mt,
        crude_oil_price,
        manufacturing_pmi
    FROM CHEMICALS_DB.ATOMIC.DEMAND_HISTORY
    {where_clause}
    ORDER BY demand_date
    """
    return _session.sql(query).to_pandas()


@st.cache_data(ttl=300)
def load_model_performance(_session):
    """Load model performance metrics."""
    query = """
    SELECT 
        model_name,
        AVG(model_mape) as avg_mape,
        AVG(prediction_confidence) as avg_confidence,
        COUNT(*) as forecast_count
    FROM CHEMICALS_DB.CHEMICAL_OPS.DEMAND_FORECAST
    GROUP BY model_name
    """
    return _session.sql(query).to_pandas()


def render_header():
    st.markdown(f"""
    <div style="margin-bottom: 2rem;">
        <h1 style="margin-bottom: 0.5rem;">🔬 Data Science Workbench</h1>
        <p style="color: {COLORS['text_muted']}; font-size: 1.1rem;">
            Technical View • Pricing Analyst • Model Validation & Hidden Insights
        </p>
    </div>
    """, unsafe_allow_html=True)


def render_model_metrics(performance_df):
    """Render model performance metrics."""
    st.markdown(section_header("📊 Model Performance", "Demand forecast accuracy metrics"), unsafe_allow_html=True)
    
    if len(performance_df) == 0:
        # Show placeholder metrics
        cols = st.columns(4)
        with cols[0]:
            st.markdown(metric_card("Model MAPE", "8.5%", icon="🎯"), unsafe_allow_html=True)
        with cols[1]:
            st.markdown(metric_card("Avg Confidence", "87%", icon="✓"), unsafe_allow_html=True)
        with cols[2]:
            st.markdown(metric_card("Forecasts Generated", "1,250", icon="📈"), unsafe_allow_html=True)
        with cols[3]:
            st.markdown(metric_card("Products Covered", "20", icon="🧪"), unsafe_allow_html=True)
        return
    
    row = performance_df.iloc[0]
    
    cols = st.columns(4)
    with cols[0]:
        mape = float(row['AVG_MAPE']) if pd.notna(row['AVG_MAPE']) else 8.5
        st.markdown(metric_card("Model MAPE", f"{mape:.1f}%", icon="🎯"), unsafe_allow_html=True)
    with cols[1]:
        conf = float(row['AVG_CONFIDENCE']) * 100 if pd.notna(row['AVG_CONFIDENCE']) else 87
        st.markdown(metric_card("Avg Confidence", f"{conf:.0f}%", icon="✓"), unsafe_allow_html=True)
    with cols[2]:
        count = int(row['FORECAST_COUNT']) if pd.notna(row['FORECAST_COUNT']) else 1250
        st.markdown(metric_card("Forecasts Generated", f"{count:,}", icon="📈"), unsafe_allow_html=True)
    with cols[3]:
        st.markdown(metric_card("Products Covered", "20", icon="🧪"), unsafe_allow_html=True)


def render_actual_vs_predicted(demand_df, forecasts_df):
    """Render actual vs predicted chart."""
    st.markdown(section_header("📈 Actual vs Predicted Volume", "Forecast accuracy validation"), unsafe_allow_html=True)
    
    # Create synthetic comparison if no data
    if len(demand_df) == 0:
        dates = pd.date_range(start='2025-01-01', periods=52, freq='W')
        actual = 500 + np.cumsum(np.random.randn(52) * 20)
        predicted = actual + np.random.randn(52) * 30
        
        demand_df = pd.DataFrame({
            'DEMAND_DATE': dates,
            'VOLUME_MT': actual
        })
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=dates,
            y=actual,
            name='Actual Volume',
            line=dict(color=COLORS['chart_1'], width=2),
            mode='lines'
        ))
        
        fig.add_trace(go.Scatter(
            x=dates,
            y=predicted,
            name='Predicted Volume',
            line=dict(color=COLORS['chart_2'], width=2, dash='dash'),
            mode='lines'
        ))
        
        # Confidence interval
        upper = predicted + 50
        lower = predicted - 50
        
        fig.add_trace(go.Scatter(
            x=list(dates) + list(dates[::-1]),
            y=list(upper) + list(lower[::-1]),
            fill='toself',
            fillcolor='rgba(139, 92, 246, 0.13)',  # chart_2 with 20% opacity
            line=dict(color='rgba(0,0,0,0)'),
            name='95% CI'
        ))
    else:
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=demand_df['DEMAND_DATE'],
            y=demand_df['VOLUME_MT'],
            name='Actual Volume',
            line=dict(color=COLORS['chart_1'], width=2),
            mode='lines+markers'
        ))
    
    fig = apply_dark_theme(fig, height=350)
    fig.update_layout(
        yaxis_title='Volume (MT)',
        legend=dict(orientation='h', yanchor='bottom', y=1.02),
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_feature_importance():
    """Render feature importance chart."""
    st.markdown(section_header("🔑 Feature Importance", "Key drivers of demand prediction"), unsafe_allow_html=True)
    
    # DRD-aligned features
    features = [
        ('Crude Oil Price', 0.35),
        ('Historical Demand', 0.25),
        ('Manufacturing PMI', 0.15),
        ('Competitor Index', 0.12),
        ('Seasonality', 0.08),
        ('GDP Growth', 0.05),
    ]
    
    df = pd.DataFrame(features, columns=['Feature', 'Importance'])
    
    fig = go.Figure(go.Bar(
        x=df['Importance'],
        y=df['Feature'],
        orientation='h',
        marker_color=[COLORS['chart_1'], COLORS['chart_2'], COLORS['chart_3'], 
                      COLORS['chart_4'], COLORS['chart_5'], COLORS['chart_6']],
        text=[f"{v:.0%}" for v in df['Importance']],
        textposition='outside'
    ))
    
    fig = apply_dark_theme(fig, height=300)
    fig.update_layout(
        xaxis_title='Importance Score',
        yaxis=dict(autorange='reversed'),
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_hidden_discovery():
    """
    Render the Hidden Discovery: Lagged Correlation
    
    DRD: "Demand for 'Polypropylene Grade B' in Southeast Asia is 90% correlated 
    with 'Crude Oil Brent' prices with a 3-week lag, allowing for predictive 
    inventory positioning before the spike."
    """
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {COLORS['primary']}20, {COLORS['secondary']}20);
        border: 2px solid {COLORS['primary']};
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
    ">
        <h3 style="color: {COLORS['primary']}; margin: 0 0 0.5rem 0;">
            🔍 Hidden Discovery: Lagged Correlation
        </h3>
        <p style="color: {COLORS['text']}; margin: 0;">
            <strong>Surface Appearance:</strong> Demand looks random or seasonal.<br>
            <strong>Revealed Reality:</strong> Demand for <em>Polypropylene</em> in <em>Asia Pacific</em> 
            is <strong>90% correlated</strong> with Crude Oil Brent prices with a <strong>3-week lag</strong>.
        </p>
        <p style="color: {COLORS['success']}; margin: 1rem 0 0 0; font-weight: 600;">
            💰 Business Impact: Prevents stock-outs during price spikes, capturing an additional $2M in quarterly margin.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Lagged correlation chart
    st.markdown(section_header("📊 Demand-Crude Price Lagged Correlation", "Correlation at different time lags"), unsafe_allow_html=True)
    
    # Simulated lagged correlation data (DRD: peak at 3 weeks = 90%)
    lags = list(range(1, 9))
    correlations = [0.45, 0.62, 0.78, 0.90, 0.85, 0.72, 0.58, 0.42]  # Peak at week 3-4
    
    fig = go.Figure()
    
    # Correlation line
    fig.add_trace(go.Scatter(
        x=lags,
        y=correlations,
        mode='lines+markers',
        line=dict(color=COLORS['primary'], width=3),
        marker=dict(size=10),
        name='Correlation'
    ))
    
    # Highlight optimal lag (3 weeks)
    fig.add_trace(go.Scatter(
        x=[4],  # Peak at 3-4 weeks
        y=[0.90],
        mode='markers',
        marker=dict(size=20, color=COLORS['success'], symbol='star'),
        name='Optimal Lag (3 weeks)'
    ))
    
    # Significance threshold
    fig.add_hline(y=0.7, line_dash="dash", line_color=COLORS['warning'], 
                  annotation_text="Significant (r > 0.7)")
    
    fig = apply_dark_theme(fig, height=300)
    fig.update_layout(
        xaxis_title='Lag (Weeks)',
        yaxis_title='Correlation Coefficient',
        xaxis=dict(tickmode='linear', tick0=1, dtick=1),
        yaxis=dict(range=[0, 1]),
        legend=dict(orientation='h', yanchor='bottom', y=1.02)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Interpretation
    cols = st.columns(3)
    with cols[0]:
        st.markdown(metric_card("Optimal Lag", "3 weeks", icon="⏱️"), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(metric_card("Correlation", "90%", icon="📈"), unsafe_allow_html=True)
    with cols[2]:
        st.markdown(metric_card("Inventory Lead Time", "21 days", icon="📦"), unsafe_allow_html=True)


def render_elasticity_analysis(elasticity_df):
    """Render price elasticity analysis."""
    st.markdown(section_header("📉 Price Elasticity Analysis", "How demand responds to price changes"), unsafe_allow_html=True)
    
    if len(elasticity_df) == 0:
        # Synthetic data
        products = ['Ethylene', 'Polypropylene', 'HDPE', 'Benzene', 'MEG']
        elasticities = [-0.8, -1.2, -0.6, -1.5, -0.9]
        categories = ['Inelastic', 'Elastic', 'Inelastic', 'Elastic', 'Inelastic']
        
        elasticity_df = pd.DataFrame({
            'PRODUCT_ID': products,
            'ELASTICITY_COEFFICIENT': elasticities,
            'ELASTICITY_CATEGORY': categories
        })
    
    # Bar chart
    colors = [COLORS['success'] if c == 'Inelastic' else COLORS['warning'] 
              for c in elasticity_df['ELASTICITY_CATEGORY']]
    
    fig = go.Figure(go.Bar(
        x=elasticity_df['PRODUCT_ID'],
        y=elasticity_df['ELASTICITY_COEFFICIENT'],
        marker_color=colors,
        text=[f"{v:.2f}" for v in elasticity_df['ELASTICITY_COEFFICIENT']],
        textposition='outside'
    ))
    
    # Add reference lines
    fig.add_hline(y=-1, line_dash="dash", line_color=COLORS['text_muted'],
                  annotation_text="Unit Elastic")
    
    fig = apply_dark_theme(fig, height=300)
    fig.update_layout(
        yaxis_title='Elasticity Coefficient',
        xaxis_title='Product',
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Legend
    st.markdown(f"""
    <div style="display: flex; gap: 2rem; justify-content: center; margin-top: 1rem;">
        <span style="color: {COLORS['success']};">● Inelastic (|e| < 1): Price increases have less impact on demand</span>
        <span style="color: {COLORS['warning']};">● Elastic (|e| > 1): Demand is sensitive to price changes</span>
    </div>
    """, unsafe_allow_html=True)


def render_what_if_simulator():
    """Render what-if price simulator."""
    st.markdown(section_header("🎮 What-If Simulator", "Simulate impact of feedstock price changes"), unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        feedstock_change = st.slider("Feedstock Price Change (%)", -20, 20, 5)
        pass_through = st.slider("Price Pass-Through (%)", 0, 100, 80)
    
    with col2:
        # Calculate impacts
        base_margin = 15  # Base margin %
        elasticity = -0.8
        
        price_increase = feedstock_change * (pass_through / 100)
        volume_change = price_increase * elasticity
        new_margin = base_margin + feedstock_change * ((pass_through - 100) / 100) * 0.6
        
        st.markdown(f"""
        <div style="
            background: {COLORS['card']};
            border: 1px solid {COLORS['border']};
            border-radius: 8px;
            padding: 1rem;
        ">
            <h4 style="color: {COLORS['text']}; margin: 0 0 1rem 0;">Impact Analysis</h4>
            <p style="color: {COLORS['text_muted']};">
                <strong>Price Change:</strong> {price_increase:+.1f}%<br>
                <strong>Volume Impact:</strong> {volume_change:+.1f}%<br>
                <strong>New Margin:</strong> {new_margin:.1f}%
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        if feedstock_change > 0 and pass_through < 50:
            st.warning("⚠️ Low pass-through may compress margins significantly")
        elif feedstock_change > 0 and pass_through > 90:
            st.success("✅ Good pass-through protects margins")


def render_sidebar():
    with st.sidebar:
        st.markdown(f"""
        <div style="padding: 1rem 0; border-bottom: 1px solid {COLORS['border']}; margin-bottom: 1rem;">
            <h2 style="margin: 0; color: {COLORS['text']};">🧪 Chemicals Pricing</h2>
            <p style="color: {COLORS['text_muted']}; margin: 0.5rem 0 0 0; font-size: 0.9rem;">Supply Chain Optimization</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("### ML Models")
        st.markdown("""
        - **Demand Forecasting**: XGBoost
        - **Price Elasticity**: Linear Regression
        - **Optimization**: scipy.optimize
        """)
        
        st.markdown("---")
        
        st.markdown("### DRD Requirements")
        st.markdown("""
        ✓ Demand sensing from macro indicators  
        ✓ Price elasticity by product/region  
        ✓ Profit-maximizing optimization  
        ✓ Forecast accuracy > 85% MAPE
        """)


def main():
    session = get_session()
    
    render_sidebar()
    render_header()
    
    # Load data
    forecasts_df = load_demand_forecasts(session)
    elasticity_df = load_price_elasticity(session)
    performance_df = load_model_performance(session)
    demand_df = load_demand_history(session)
    
    # Model performance KPIs
    render_model_metrics(performance_df)
    
    st.markdown("---")
    
    # Hidden Discovery - highlight this prominently
    render_hidden_discovery()
    
    st.markdown("---")
    
    # Two column layout for other analyses
    col1, col2 = st.columns(2)
    
    with col1:
        render_actual_vs_predicted(demand_df, forecasts_df)
        render_what_if_simulator()
    
    with col2:
        render_feature_importance()
        render_elasticity_analysis(elasticity_df)


if __name__ == "__main__":
    main()
