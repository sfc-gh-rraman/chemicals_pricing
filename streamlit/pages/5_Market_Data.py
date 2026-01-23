"""
Market Data Dashboard - Real-time Commodity & Economic Indicators
Powered by Snowflake Marketplace (Cybersyn)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from snowflake.snowpark.context import get_active_session

# Theme colors
COLORS = {
    'primary': '#0ea5e9',
    'secondary': '#64748b', 
    'success': '#22c55e',
    'warning': '#f59e0b',
    'danger': '#ef4444',
    'background': '#0f172a',
    'surface': '#1e293b',
    'text': '#f1f5f9',
    'chart_1': '#0ea5e9',
    'chart_2': '#8b5cf6',
    'chart_3': '#22c55e',
    'chart_4': '#f59e0b',
}

def apply_dark_theme():
    """Apply dark theme styling."""
    st.markdown("""
    <style>
        .stApp { background-color: #0f172a; }
        [data-testid="stSidebar"] { background-color: #1e293b; }
        h1, h2, h3, p, span, label { color: #f1f5f9 !important; }
        .metric-card {
            background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid #334155;
            margin: 10px 0;
        }
        .metric-value { font-size: 2rem; font-weight: bold; color: #0ea5e9; }
        .metric-label { font-size: 0.9rem; color: #94a3b8; }
        .trend-up { color: #22c55e; }
        .trend-down { color: #ef4444; }
        .data-source-badge {
            background: #1e40af;
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.75rem;
            display: inline-block;
            margin-left: 10px;
        }
    </style>
    """, unsafe_allow_html=True)

def check_marketplace_available(session) -> dict:
    """Check which marketplace data sources are available."""
    status = {
        'snowflake_public_data': False,
        'fuel_oil_cpi': False,
        'core_cpi': False,
        'industrial_production': False,
        'economic': False
    }
    
    # Check if SNOWFLAKE_PUBLIC_DATA_FREE database exists
    try:
        result = session.sql("SHOW DATABASES LIKE 'SNOWFLAKE_PUBLIC_DATA_FREE'").collect()
        status['snowflake_public_data'] = len(result) > 0
    except:
        pass
    
    # Check if our marketplace views exist
    try:
        session.sql("SELECT 1 FROM CHEMICALS_DB.CHEMICAL_OPS.MARKETPLACE_FUEL_OIL_CPI LIMIT 1").collect()
        status['fuel_oil_cpi'] = True
    except:
        pass
    
    try:
        session.sql("SELECT 1 FROM CHEMICALS_DB.CHEMICAL_OPS.MARKETPLACE_CORE_CPI LIMIT 1").collect()
        status['core_cpi'] = True
    except:
        pass
    
    try:
        session.sql("SELECT 1 FROM CHEMICALS_DB.CHEMICAL_OPS.MARKETPLACE_INDUSTRIAL_PRODUCTION LIMIT 1").collect()
        status['industrial_production'] = True
    except:
        pass
    
    try:
        session.sql("SELECT 1 FROM CHEMICALS_DB.CHEMICAL_OPS.MARKETPLACE_ECONOMIC_INDICATORS LIMIT 1").collect()
        status['economic'] = True
    except:
        pass
    
    return status

def load_fuel_oil_cpi(session) -> pd.DataFrame:
    """Load Fuel Oil CPI data from marketplace."""
    try:
        df = session.sql("""
            SELECT price_date, index_code, index_value, data_source
            FROM CHEMICALS_DB.CHEMICAL_OPS.MARKETPLACE_FUEL_OIL_CPI
            ORDER BY price_date DESC
        """).to_pandas()
        return df
    except:
        return pd.DataFrame()

def load_core_cpi(session) -> pd.DataFrame:
    """Load Core CPI data."""
    try:
        df = session.sql("""
            SELECT price_date, index_code, index_value, data_source
            FROM CHEMICALS_DB.CHEMICAL_OPS.MARKETPLACE_CORE_CPI
            ORDER BY price_date DESC
        """).to_pandas()
        return df
    except:
        return pd.DataFrame()

def load_industrial_production(session) -> pd.DataFrame:
    """Load Industrial Production data."""
    try:
        df = session.sql("""
            SELECT price_date, index_code, index_value, data_source
            FROM CHEMICALS_DB.CHEMICAL_OPS.MARKETPLACE_INDUSTRIAL_PRODUCTION
            ORDER BY price_date DESC
        """).to_pandas()
        return df
    except:
        return pd.DataFrame()

def load_economic_indicators(session) -> pd.DataFrame:
    """Load all economic indicators."""
    try:
        df = session.sql("""
            SELECT price_date, index_code, index_value, data_source
            FROM CHEMICALS_DB.CHEMICAL_OPS.MARKETPLACE_ECONOMIC_INDICATORS
            ORDER BY price_date DESC, index_code
        """).to_pandas()
        return df
    except:
        return pd.DataFrame()

def load_enhanced_margin_data(session) -> pd.DataFrame:
    """Load enhanced margin data with marketplace indicators."""
    try:
        df = session.sql("""
            SELECT 
                product_name,
                region,
                ROUND(AVG(gross_margin_pct), 2) as avg_margin_pct,
                ROUND(AVG(current_fuel_cpi), 2) as fuel_cpi,
                ROUND(AVG(current_core_cpi), 2) as core_cpi,
                MAX(energy_cost_trend) as energy_trend,
                data_quality
            FROM CHEMICALS_DB.CHEMICAL_OPS.MARGIN_ANALYZER_ENHANCED
            GROUP BY product_name, region, data_quality
            ORDER BY avg_margin_pct DESC
            LIMIT 50
        """).to_pandas()
        return df
    except:
        return pd.DataFrame()

def render_cpi_chart(fuel_df: pd.DataFrame, core_df: pd.DataFrame):
    """Render CPI comparison chart."""
    if fuel_df.empty and core_df.empty:
        st.warning("No CPI data available")
        return
    
    fig = go.Figure()
    
    if not fuel_df.empty:
        fuel_df = fuel_df.sort_values('PRICE_DATE')
        fig.add_trace(go.Scatter(
            x=fuel_df['PRICE_DATE'],
            y=fuel_df['INDEX_VALUE'],
            name='Fuel Oil CPI',
            mode='lines',
            line=dict(width=2, color=COLORS['chart_1'])
        ))
    
    if not core_df.empty:
        core_df = core_df.sort_values('PRICE_DATE')
        fig.add_trace(go.Scatter(
            x=core_df['PRICE_DATE'],
            y=core_df['INDEX_VALUE'],
            name='Core CPI (less food & energy)',
            mode='lines',
            line=dict(width=2, color=COLORS['chart_2'])
        ))
    
    fig.update_layout(
        title='Consumer Price Index Trends (Index: 1982-1984 = 100)',
        xaxis_title='Date',
        yaxis_title='Index Value',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def render_economic_dashboard(economic_df: pd.DataFrame):
    """Render economic indicators dashboard."""
    if economic_df.empty:
        st.info("Economic indicators not available.")
        return
    
    # Create subplots for different indicators
    indicators = economic_df['INDEX_CODE'].unique()
    
    fig = make_subplots(
        rows=1, cols=len(indicators),
        subplot_titles=[i.replace('_', ' ').title() for i in indicators],
        horizontal_spacing=0.1
    )
    
    colors = [COLORS['chart_1'], COLORS['chart_2'], COLORS['chart_3'], COLORS['chart_4']]
    
    for idx, indicator in enumerate(indicators):
        col = idx + 1
        
        ind_df = economic_df[economic_df['INDEX_CODE'] == indicator].sort_values('PRICE_DATE')
        
        fig.add_trace(
            go.Scatter(
                x=ind_df['PRICE_DATE'],
                y=ind_df['INDEX_VALUE'],
                name=indicator.replace('_', ' ').title(),
                line=dict(color=colors[idx % len(colors)], width=2),
                showlegend=True
            ),
            row=1, col=col
        )
    
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=400,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def render_industrial_production_chart(ip_df: pd.DataFrame):
    """Render Industrial Production chart."""
    if ip_df.empty:
        st.info("Industrial Production data not available.")
        return
    
    ip_df = ip_df.sort_values('PRICE_DATE')
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Scatter(
            x=ip_df['PRICE_DATE'],
            y=ip_df['INDEX_VALUE'],
            name='Distillate Fuel Production',
            line=dict(color=COLORS['chart_3'], width=2),
            fill='tozeroy',
            fillcolor='rgba(34, 197, 94, 0.2)'
        )
    )
    
    fig.update_layout(
        title='Industrial Production: Distillate Fuel Oil (Index: 2017 = 100)',
        xaxis_title='Date',
        yaxis_title='Index Value',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=350
    )
    
    st.plotly_chart(fig, use_container_width=True)

def main():
    st.set_page_config(
        page_title="Market Data | Chemicals Pricing",
        page_icon="📈",
        layout="wide"
    )
    
    apply_dark_theme()
    
    # Header
    st.markdown("## 📈 Market Data Dashboard")
    st.markdown("*Real-time Commodity Prices & Economic Indicators from Snowflake Marketplace*")
    
    try:
        session = get_active_session()
    except:
        st.error("Could not connect to Snowflake. Please run this app in Snowflake.")
        return
    
    # Check data availability
    status = check_marketplace_available(session)
    
    # Status bar
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if status['snowflake_public_data']:
            st.success("✅ Marketplace DB")
        else:
            st.warning("⚠️ Not Installed")
    
    with col2:
        if status['fuel_oil_cpi']:
            st.success("✅ Fuel Oil CPI")
        else:
            st.info("❌ Not Available")
    
    with col3:
        if status['core_cpi']:
            st.success("✅ Core CPI")
        else:
            st.info("❌ Not Available")
    
    with col4:
        if status['industrial_production']:
            st.success("✅ Industrial Prod")
        else:
            st.info("❌ Not Available")
    
    st.divider()
    
    # Show data source info
    if status['snowflake_public_data'] and status['fuel_oil_cpi']:
        st.success("🎉 **Real marketplace data active!** Data from Snowflake Public Data (Free)")
    else:
        with st.expander("📥 Marketplace Data Status", expanded=False):
            st.markdown("""
            **Data Source:** Snowflake Public Data (Free)
            
            Integration views have been created to pull:
            - Fuel Oil CPI (energy cost proxy)
            - Core CPI (inflation indicator)
            - Industrial Production Index
            """)
    
    # Main content tabs
    tab1, tab2, tab3 = st.tabs(["📈 Price Indices", "📊 All Indicators", "🏭 Enhanced Margins"])
    
    with tab1:
        st.markdown("### Consumer Price Index Trends")
        st.markdown("*Energy costs and core inflation from BLS data*")
        
        fuel_df = load_fuel_oil_cpi(session)
        core_df = load_core_cpi(session)
        
        # Current values
        col1, col2, col3 = st.columns(3)
        
        if not fuel_df.empty:
            latest_fuel = fuel_df.head(1)
            with col1:
                value = latest_fuel['INDEX_VALUE'].iloc[0]
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Fuel Oil CPI</div>
                    <div class="metric-value">{value:.1f}</div>
                    <span class="data-source-badge">REAL DATA</span>
                </div>
                """, unsafe_allow_html=True)
        
        if not core_df.empty:
            latest_core = core_df.head(1)
            with col2:
                value = latest_core['INDEX_VALUE'].iloc[0]
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Core CPI</div>
                    <div class="metric-value">{value:.1f}</div>
                    <span class="data-source-badge">REAL DATA</span>
                </div>
                """, unsafe_allow_html=True)
        
        ip_df = load_industrial_production(session)
        if not ip_df.empty:
            latest_ip = ip_df.head(1)
            with col3:
                value = latest_ip['INDEX_VALUE'].iloc[0]
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Industrial Production</div>
                    <div class="metric-value">{value:.1f}</div>
                    <span class="data-source-badge">REAL DATA</span>
                </div>
                """, unsafe_allow_html=True)
        
        render_cpi_chart(fuel_df, core_df)
        
        st.divider()
        st.markdown("### Industrial Production")
        render_industrial_production_chart(ip_df)
    
    with tab2:
        st.markdown("### All Economic Indicators")
        st.markdown("*Combined view of marketplace data*")
        
        economic_df = load_economic_indicators(session)
        
        if not economic_df.empty:
            st.dataframe(
                economic_df.head(30),
                use_container_width=True,
                hide_index=True
            )
            
            render_economic_dashboard(economic_df)
        else:
            st.info("No economic indicator data available.")
    
    with tab3:
        st.markdown("### Enhanced Margin Analysis")
        st.markdown("*Margins enriched with real marketplace indicators*")
        
        enhanced_df = load_enhanced_margin_data(session)
        
        if not enhanced_df.empty:
            # Show energy trend
            trend = enhanced_df['ENERGY_TREND'].iloc[0] if 'ENERGY_TREND' in enhanced_df.columns else 'STABLE'
            trend_color = COLORS['success'] if trend == 'FALLING' else (COLORS['danger'] if trend == 'RISING' else COLORS['warning'])
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Energy Cost Trend</div>
                    <div class="metric-value" style="color: {trend_color};">{trend}</div>
                    <span class="data-source-badge">MARKETPLACE</span>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                fuel_cpi = enhanced_df['FUEL_CPI'].iloc[0] if 'FUEL_CPI' in enhanced_df.columns else 0
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Current Fuel CPI</div>
                    <div class="metric-value">{fuel_cpi:.1f}</div>
                    <span class="data-source-badge">BLS DATA</span>
                </div>
                """, unsafe_allow_html=True)
            
            st.divider()
            st.markdown("#### Top Products by Margin (with Market Context)")
            st.dataframe(
                enhanced_df[['PRODUCT_NAME', 'REGION', 'AVG_MARGIN_PCT', 'FUEL_CPI', 'ENERGY_TREND']].head(20),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Enhanced margin data not available. Run the marketplace integration SQL.")

if __name__ == "__main__":
    main()
