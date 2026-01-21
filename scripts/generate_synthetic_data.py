"""
Synthetic Data Generator for Chemicals Pricing & Supply Chain Demo
===================================================================
Generates DRD-aligned synthetic data for the Chemicals Integrated Pricing demo:
- Products catalog (chemicals with CAS numbers)
- Market indices (ICIS/Platts feedstock prices)
- Sales orders (transaction history)
- Production runs (batch data with costs)
- Inventory balances
- Contract terms
- Market reports (for Cortex Search)

Usage:
    python generate_synthetic_data.py --output-dir ../data/synthetic
    
The generated CSV files can be loaded into Snowflake using COPY INTO commands.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import argparse
from pathlib import Path

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# =============================================================================
# Configuration
# =============================================================================

# Chemical Products
PRODUCTS = [
    # Olefins
    {'id': 'ETH-001', 'name': 'Ethylene', 'cas': '74-85-1', 'family': 'Olefins', 'category': 'Ethylene', 'base_cost': 950},
    {'id': 'PP-001', 'name': 'Propylene Polymer Grade', 'cas': '115-07-1', 'family': 'Olefins', 'category': 'Propylene', 'base_cost': 1100},
    {'id': 'PP-002', 'name': 'Propylene Chemical Grade', 'cas': '115-07-1', 'family': 'Olefins', 'category': 'Propylene', 'base_cost': 1050},
    {'id': 'BUT-001', 'name': 'Butadiene', 'cas': '106-99-0', 'family': 'Olefins', 'category': 'Butadiene', 'base_cost': 1200},
    
    # Polymers
    {'id': 'HDPE-001', 'name': 'HDPE Film Grade', 'cas': '9002-88-4', 'family': 'Polymers', 'category': 'Polyethylene', 'base_cost': 1300},
    {'id': 'LDPE-001', 'name': 'LDPE Injection Grade', 'cas': '9002-88-4', 'family': 'Polymers', 'category': 'Polyethylene', 'base_cost': 1350},
    {'id': 'LLDPE-001', 'name': 'LLDPE C4', 'cas': '9002-88-4', 'family': 'Polymers', 'category': 'Polyethylene', 'base_cost': 1280},
    {'id': 'PP-HOMO', 'name': 'Polypropylene Homopolymer', 'cas': '9003-07-0', 'family': 'Polymers', 'category': 'Polypropylene', 'base_cost': 1400},
    {'id': 'PP-COPOLY', 'name': 'Polypropylene Copolymer', 'cas': '9003-07-0', 'family': 'Polymers', 'category': 'Polypropylene', 'base_cost': 1450},
    {'id': 'PS-001', 'name': 'Polystyrene GPPS', 'cas': '9003-53-6', 'family': 'Polymers', 'category': 'Polystyrene', 'base_cost': 1250},
    {'id': 'PVC-001', 'name': 'PVC Suspension Grade', 'cas': '9002-86-2', 'family': 'Polymers', 'category': 'PVC', 'base_cost': 1100},
    
    # Aromatics
    {'id': 'BEN-001', 'name': 'Benzene', 'cas': '71-43-2', 'family': 'Aromatics', 'category': 'Benzene', 'base_cost': 900},
    {'id': 'TOL-001', 'name': 'Toluene', 'cas': '108-88-3', 'family': 'Aromatics', 'category': 'Toluene', 'base_cost': 850},
    {'id': 'XYL-001', 'name': 'Mixed Xylenes', 'cas': '1330-20-7', 'family': 'Aromatics', 'category': 'Xylenes', 'base_cost': 880},
    {'id': 'STY-001', 'name': 'Styrene Monomer', 'cas': '100-42-5', 'family': 'Aromatics', 'category': 'Styrene', 'base_cost': 1150},
    
    # Intermediates
    {'id': 'MEG-001', 'name': 'Monoethylene Glycol', 'cas': '107-21-1', 'family': 'Intermediates', 'category': 'Glycols', 'base_cost': 750},
    {'id': 'DEG-001', 'name': 'Diethylene Glycol', 'cas': '111-46-6', 'family': 'Intermediates', 'category': 'Glycols', 'base_cost': 800},
    {'id': 'ACN-001', 'name': 'Acrylonitrile', 'cas': '107-13-1', 'family': 'Intermediates', 'category': 'Acrylonitrile', 'base_cost': 1400},
    {'id': 'MMA-001', 'name': 'Methyl Methacrylate', 'cas': '80-62-6', 'family': 'Intermediates', 'category': 'MMA', 'base_cost': 1600},
]

# Market Indices
MARKET_INDICES = [
    {'code': 'CRUDE-BRENT', 'name': 'Crude Oil Brent', 'base_price': 75, 'unit': 'BBL', 'region': 'Global'},
    {'code': 'CRUDE-WTI', 'name': 'Crude Oil WTI', 'base_price': 72, 'unit': 'BBL', 'region': 'North America'},
    {'code': 'NAPHTHA-CFR-NEA', 'name': 'Naphtha CFR NE Asia', 'base_price': 650, 'unit': 'MT', 'region': 'Asia Pacific'},
    {'code': 'NAPHTHA-CIF-NWE', 'name': 'Naphtha CIF NW Europe', 'base_price': 620, 'unit': 'MT', 'region': 'Europe'},
    {'code': 'ETH-CFR-NEA', 'name': 'Ethylene CFR NE Asia', 'base_price': 1050, 'unit': 'MT', 'region': 'Asia Pacific'},
    {'code': 'ETH-FD-NWE', 'name': 'Ethylene FD NW Europe', 'base_price': 1000, 'unit': 'MT', 'region': 'Europe'},
    {'code': 'ETH-DEL-USG', 'name': 'Ethylene DEL US Gulf', 'base_price': 950, 'unit': 'MT', 'region': 'North America'},
    {'code': 'PP-CFR-NEA', 'name': 'Propylene CFR NE Asia', 'base_price': 1150, 'unit': 'MT', 'region': 'Asia Pacific'},
    {'code': 'PP-FD-NWE', 'name': 'Propylene FD NW Europe', 'base_price': 1100, 'unit': 'MT', 'region': 'Europe'},
    {'code': 'PP-DEL-USG', 'name': 'Propylene DEL US Gulf', 'base_price': 1080, 'unit': 'MT', 'region': 'North America'},
    {'code': 'BEN-FOB-USG', 'name': 'Benzene FOB US Gulf', 'base_price': 950, 'unit': 'MT', 'region': 'North America'},
    {'code': 'BEN-CFR-NEA', 'name': 'Benzene CFR NE Asia', 'base_price': 980, 'unit': 'MT', 'region': 'Asia Pacific'},
    {'code': 'STY-CFR-NEA', 'name': 'Styrene CFR NE Asia', 'base_price': 1200, 'unit': 'MT', 'region': 'Asia Pacific'},
    {'code': 'MEG-CFR-NEA', 'name': 'MEG CFR NE Asia', 'base_price': 800, 'unit': 'MT', 'region': 'Asia Pacific'},
]

# Regions and their characteristics
REGIONS = {
    'North America': {'demand_multiplier': 1.0, 'freight_base': 50, 'currency': 'USD'},
    'Europe': {'demand_multiplier': 0.9, 'freight_base': 80, 'currency': 'USD'},
    'Asia Pacific': {'demand_multiplier': 1.3, 'freight_base': 100, 'currency': 'USD'},
    'Latin America': {'demand_multiplier': 0.5, 'freight_base': 120, 'currency': 'USD'},
    'Middle East': {'demand_multiplier': 0.4, 'freight_base': 90, 'currency': 'USD'},
}

# Customer Segments
CUSTOMER_SEGMENTS = ['Industrial', 'Consumer', 'Agricultural', 'Automotive', 'Construction']
CUSTOMER_TIERS = ['Platinum', 'Gold', 'Silver', 'Standard']
SALES_CHANNELS = ['Direct', 'Distributor', 'Spot', 'Contract']

# Manufacturing Sites
SITES = [
    {'id': 'SITE-USG-01', 'name': 'Houston Complex', 'region': 'North America', 'type': 'MANUFACTURING', 'capacity': 500000},
    {'id': 'SITE-USG-02', 'name': 'Texas City Plant', 'region': 'North America', 'type': 'MANUFACTURING', 'capacity': 350000},
    {'id': 'SITE-NWE-01', 'name': 'Rotterdam Terminal', 'region': 'Europe', 'type': 'MANUFACTURING', 'capacity': 400000},
    {'id': 'SITE-NWE-02', 'name': 'Antwerp Complex', 'region': 'Europe', 'type': 'MANUFACTURING', 'capacity': 450000},
    {'id': 'SITE-NEA-01', 'name': 'Singapore Hub', 'region': 'Asia Pacific', 'type': 'MANUFACTURING', 'capacity': 600000},
    {'id': 'SITE-NEA-02', 'name': 'Shanghai Plant', 'region': 'Asia Pacific', 'type': 'MANUFACTURING', 'capacity': 500000},
]

# Report sources and types for Cortex Search
REPORT_SOURCES = ['Goldman Sachs', 'Morgan Stanley', 'ICIS', 'Platts', 'ChemWeek', 'Wood Mackenzie', 'IHS Markit']
REPORT_TYPES = ['ANALYST_REPORT', 'EARNINGS_CALL', 'TRADE_JOURNAL', 'MARKET_OUTLOOK', 'SUPPLY_ANALYSIS']


def generate_products() -> pd.DataFrame:
    """Generate product catalog."""
    print("Generating product catalog...")
    
    products = []
    for p in PRODUCTS:
        products.append({
            'product_id': p['id'],
            'product_name': p['name'],
            'cas_number': p['cas'],
            'product_family': p['family'],
            'product_category': p['category'],
            'product_grade': random.choice(['Grade A', 'Grade B', 'Industrial', 'Food Grade']),
            'molecular_weight': round(random.uniform(28, 200), 2),
            'density_kg_m3': round(random.uniform(800, 1200), 1),
            'hazard_class': random.choice(['3', '4.1', '6.1', '8', '9']),
            'un_number': f'UN{random.randint(1000, 3500)}',
            'base_unit': 'MT',
            'standard_cost_per_mt': p['base_cost'],
            'target_margin_pct': random.choice([12, 15, 18, 20]),
            'min_order_quantity_mt': random.choice([1, 5, 10, 20]),
            'lead_time_days': random.randint(3, 14),
            'is_active': True
        })
    
    df = pd.DataFrame(products)
    print(f"Generated {len(df)} products")
    return df


def generate_customers(n_customers: int = 200) -> pd.DataFrame:
    """Generate customer master data."""
    print("Generating customer data...")
    
    company_prefixes = ['Global', 'Pacific', 'Atlantic', 'United', 'National', 'Premier', 'Advanced', 'Industrial']
    company_suffixes = ['Industries', 'Manufacturing', 'Chemicals', 'Corp', 'LLC', 'Holdings', 'Group', 'Partners']
    
    customers = []
    for i in range(n_customers):
        region = random.choice(list(REGIONS.keys()))
        segment = random.choice(CUSTOMER_SEGMENTS)
        tier = random.choices(CUSTOMER_TIERS, weights=[0.1, 0.2, 0.3, 0.4])[0]
        
        customers.append({
            'customer_id': f'CUST-{i+1:05d}',
            'customer_name': f'{random.choice(company_prefixes)} {random.choice(company_suffixes)}',
            'customer_segment': segment,
            'customer_tier': tier,
            'credit_limit': {'Platinum': 5000000, 'Gold': 2000000, 'Silver': 500000, 'Standard': 100000}[tier],
            'payment_terms_days': {'Platinum': 45, 'Gold': 30, 'Silver': 30, 'Standard': 15}[tier],
            'primary_region': region,
            'primary_contact': f'contact{i}@example.com',
            'annual_volume_mt': round(np.random.exponential(500) * {'Platinum': 10, 'Gold': 5, 'Silver': 2, 'Standard': 1}[tier], 1),
            'customer_since': (datetime.now() - timedelta(days=random.randint(365, 3650))).strftime('%Y-%m-%d'),
            'is_active': random.random() > 0.05
        })
    
    df = pd.DataFrame(customers)
    print(f"Generated {len(df)} customers")
    return df


def generate_sites() -> pd.DataFrame:
    """Generate manufacturing sites."""
    print("Generating site data...")
    
    sites = []
    for s in SITES:
        sites.append({
            'site_id': s['id'],
            'site_name': s['name'],
            'site_type': s['type'],
            'region': s['region'],
            'country': {'North America': 'USA', 'Europe': 'Netherlands', 'Asia Pacific': 'Singapore'}[s['region']],
            'city': s['name'].split()[0],
            'latitude': round(random.uniform(25, 55), 4),
            'longitude': round(random.uniform(-100, 120), 4),
            'capacity_mt_year': s['capacity'],
            'utilization_target_pct': 85,
            'storage_capacity_mt': int(s['capacity'] * 0.1),
            'is_active': True
        })
    
    df = pd.DataFrame(sites)
    print(f"Generated {len(df)} sites")
    return df


def generate_market_indices(days: int = 730) -> pd.DataFrame:
    """
    Generate historical market index data with realistic price movements.
    Includes correlations between related indices.
    """
    print("Generating market index data...")
    
    records = []
    record_id = 1
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Generate base crude oil prices first (driver for others)
    crude_prices = {}
    current_price = 75
    
    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        # Skip weekends
        if current_date.weekday() >= 5:
            continue
        
        # Random walk with mean reversion
        daily_return = np.random.normal(0, 0.02) + 0.001 * (75 - current_price)
        current_price = max(50, min(120, current_price * (1 + daily_return)))
        crude_prices[current_date.strftime('%Y-%m-%d')] = current_price
    
    # Generate all indices
    for idx in MARKET_INDICES:
        current_price = idx['base_price']
        price_history = []
        
        for day_offset in range(days):
            current_date = start_date + timedelta(days=day_offset)
            if current_date.weekday() >= 5:
                continue
            
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Price movement correlated with crude
            crude_change = 0
            if date_str in crude_prices and len(price_history) > 0:
                crude_prev = crude_prices.get((current_date - timedelta(days=1)).strftime('%Y-%m-%d'), crude_prices[date_str])
                crude_change = (crude_prices[date_str] - crude_prev) / crude_prev if crude_prev else 0
            
            # Different correlation strengths
            correlation = 0.6 if 'CRUDE' not in idx['code'] else 1.0
            daily_return = correlation * crude_change + np.random.normal(0, 0.015)
            
            # Mean reversion
            daily_return += 0.001 * (idx['base_price'] - current_price) / idx['base_price']
            
            current_price = max(idx['base_price'] * 0.5, min(idx['base_price'] * 2, current_price * (1 + daily_return)))
            price_history.append({'date': date_str, 'price': current_price})
            
            # Calculate moving averages and changes
            price_1d_ago = price_history[-2]['price'] if len(price_history) >= 2 else current_price
            price_7d_ago = price_history[-8]['price'] if len(price_history) >= 8 else current_price
            price_30d_ago = price_history[-31]['price'] if len(price_history) >= 31 else current_price
            
            prices_7d = [p['price'] for p in price_history[-7:]]
            prices_30d = [p['price'] for p in price_history[-30:]]
            
            records.append({
                'record_id': record_id,
                'index_name': idx['name'],
                'index_code': idx['code'],
                'price_date': date_str,
                'price_value': round(current_price, 2),
                'price_currency': 'USD',
                'price_unit': idx['unit'],
                'price_type': 'SPOT',
                'region': idx['region'],
                'source': random.choice(['ICIS', 'PLATTS', 'CME']),
                'price_change_1d': round(current_price - price_1d_ago, 2),
                'price_change_7d': round(current_price - price_7d_ago, 2),
                'price_change_30d': round(current_price - price_30d_ago, 2),
                'price_change_pct_1d': round((current_price - price_1d_ago) / price_1d_ago * 100, 2) if price_1d_ago else 0,
                'price_change_pct_7d': round((current_price - price_7d_ago) / price_7d_ago * 100, 2) if price_7d_ago else 0,
                'price_change_pct_30d': round((current_price - price_30d_ago) / price_30d_ago * 100, 2) if price_30d_ago else 0,
                'price_avg_7d': round(np.mean(prices_7d), 2),
                'price_avg_30d': round(np.mean(prices_30d), 2),
                'price_volatility_30d': round(np.std(prices_30d), 2) if len(prices_30d) > 1 else 0
            })
            record_id += 1
    
    df = pd.DataFrame(records)
    print(f"Generated {len(df)} market index records")
    return df


def generate_sales_orders(products_df: pd.DataFrame, customers_df: pd.DataFrame, 
                          sites_df: pd.DataFrame, days: int = 365) -> pd.DataFrame:
    """Generate sales order history."""
    print("Generating sales order data...")
    
    orders = []
    order_id = 1
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Generate ~50 orders per day
    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        if current_date.weekday() >= 5:
            continue
        
        num_orders = int(np.random.poisson(50))
        
        for _ in range(num_orders):
            product = products_df.sample(1).iloc[0]
            customer = customers_df.sample(1).iloc[0]
            site = sites_df[sites_df['region'] == customer['primary_region']]
            if len(site) == 0:
                site = sites_df.sample(1)
            else:
                site = site.sample(1)
            site = site.iloc[0]
            
            # Volume based on customer tier
            base_volume = {'Platinum': 500, 'Gold': 200, 'Silver': 50, 'Standard': 20}[customer['customer_tier']]
            quantity = round(np.random.exponential(base_volume), 1)
            
            # Price based on product cost + margin + variation
            base_price = product['standard_cost_per_mt'] * (1 + product['target_margin_pct'] / 100)
            price_variation = np.random.normal(0, 0.05)
            unit_price = round(base_price * (1 + price_variation), 2)
            
            # Discounts based on tier
            discount_pct = {'Platinum': 5, 'Gold': 3, 'Silver': 1, 'Standard': 0}[customer['customer_tier']]
            discount_amount = round(unit_price * quantity * discount_pct / 100, 2)
            
            # Freight cost
            freight_cost = round(REGIONS[customer['primary_region']]['freight_base'] * quantity / 20, 2)
            
            total_amount = round(unit_price * quantity, 2)
            net_revenue = round(total_amount - discount_amount - freight_cost, 2)
            
            orders.append({
                'order_id': f'ORD-{order_id:08d}',
                'order_line_id': 1,
                'order_date': current_date.strftime('%Y-%m-%d'),
                'ship_date': (current_date + timedelta(days=random.randint(3, 14))).strftime('%Y-%m-%d'),
                'customer_id': customer['customer_id'],
                'customer_name': customer['customer_name'],
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'quantity_mt': quantity,
                'unit_price': unit_price,
                'total_amount': total_amount,
                'currency': 'USD',
                'sales_region': customer['primary_region'],
                'sales_channel': random.choice(SALES_CHANNELS),
                'customer_segment': customer['customer_segment'],
                'contract_id': f'CTR-{customer["customer_id"][-5:]}' if random.random() > 0.3 else None,
                'discount_pct': discount_pct,
                'rebate_amount': round(total_amount * random.uniform(0, 0.02), 2),
                'freight_cost': freight_cost,
                'ship_from_site': site['site_id'],
                'ship_to_location': f'{customer["primary_region"]} Distribution',
                'incoterms': random.choice(['FOB', 'CIF', 'DDP', 'DAP']),
                'net_revenue': net_revenue
            })
            order_id += 1
    
    df = pd.DataFrame(orders)
    print(f"Generated {len(df)} sales orders")
    return df


def generate_production_runs(products_df: pd.DataFrame, sites_df: pd.DataFrame, days: int = 365) -> pd.DataFrame:
    """Generate production run/batch data."""
    print("Generating production run data...")
    
    runs = []
    run_id = 1
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        
        # Each site produces 2-4 batches per day
        for _, site in sites_df.iterrows():
            num_runs = random.randint(2, 4)
            
            for _ in range(num_runs):
                product = products_df.sample(1).iloc[0]
                
                planned_qty = random.randint(100, 500)
                yield_pct = random.uniform(0.92, 0.99)
                actual_yield = round(planned_qty * yield_pct, 1)
                
                feedstock_consumed = round(actual_yield * random.uniform(1.05, 1.15), 1)
                feedstock_cost = round(feedstock_consumed * product['standard_cost_per_mt'] * 0.6, 2)
                energy_cost = round(actual_yield * random.uniform(30, 60), 2)
                labor_cost = round(actual_yield * random.uniform(20, 40), 2)
                overhead_cost = round(actual_yield * random.uniform(40, 80), 2)
                
                total_variable = feedstock_cost + energy_cost
                total_fixed = labor_cost + overhead_cost
                cost_per_mt = round((total_variable + total_fixed) / actual_yield, 2)
                
                start_time = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=random.randint(0, 20))
                end_time = start_time + timedelta(hours=random.randint(4, 12))
                
                runs.append({
                    'run_id': f'RUN-{run_id:08d}',
                    'site_id': site['site_id'],
                    'product_id': product['product_id'],
                    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'planned_quantity_mt': planned_qty,
                    'actual_yield_mt': actual_yield,
                    'yield_pct': round(yield_pct * 100, 2),
                    'feedstock_consumed_mt': feedstock_consumed,
                    'feedstock_cost': feedstock_cost,
                    'energy_cost': energy_cost,
                    'labor_cost': labor_cost,
                    'overhead_cost': overhead_cost,
                    'total_variable_cost': total_variable,
                    'total_fixed_cost': total_fixed,
                    'cost_per_mt': cost_per_mt,
                    'quality_grade': random.choice(['A', 'A', 'A', 'B']),
                    'batch_status': 'COMPLETED',
                    'carbon_footprint_kg': round(actual_yield * random.uniform(500, 1500), 0)
                })
                run_id += 1
    
    df = pd.DataFrame(runs)
    print(f"Generated {len(df)} production runs")
    return df


def generate_inventory_balances(products_df: pd.DataFrame, sites_df: pd.DataFrame) -> pd.DataFrame:
    """Generate current inventory balances."""
    print("Generating inventory balance data...")
    
    balances = []
    balance_id = 1
    snapshot_date = datetime.now().strftime('%Y-%m-%d')
    
    for _, product in products_df.iterrows():
        for _, site in sites_df.iterrows():
            quantity = round(random.uniform(100, 2000), 1)
            allocated = round(quantity * random.uniform(0.1, 0.4), 1)
            available = round(quantity - allocated, 1)
            
            avg_daily_demand = random.uniform(10, 50)
            days_of_supply = round(available / avg_daily_demand, 1)
            
            balances.append({
                'inventory_balance_id': balance_id,
                'snapshot_date': snapshot_date,
                'product_id': product['product_id'],
                'site_id': site['site_id'],
                'quantity_mt': quantity,
                'quantity_allocated_mt': allocated,
                'quantity_available_mt': available,
                'inventory_value': round(quantity * product['standard_cost_per_mt'], 2),
                'days_of_supply': days_of_supply,
                'reorder_point_mt': round(avg_daily_demand * 14, 1),
                'safety_stock_mt': round(avg_daily_demand * 7, 1),
                'storage_cost_daily': round(quantity * 0.5, 2),
                'inventory_age_days': random.randint(5, 90),
                'quality_status': random.choice(['RELEASED', 'RELEASED', 'RELEASED', 'QC_HOLD'])
            })
            balance_id += 1
    
    df = pd.DataFrame(balances)
    print(f"Generated {len(df)} inventory balances")
    return df


def generate_contract_terms(products_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """Generate customer contract terms."""
    print("Generating contract terms...")
    
    terms = []
    term_id = 1
    
    # Generate contracts for top-tier customers
    top_customers = customers_df[customers_df['customer_tier'].isin(['Platinum', 'Gold'])]
    
    for _, customer in top_customers.iterrows():
        # Each customer has contracts for 2-5 products
        num_products = random.randint(2, 5)
        contracted_products = products_df.sample(num_products)
        
        for _, product in contracted_products.iterrows():
            effective_date = datetime.now() - timedelta(days=random.randint(30, 365))
            expiration_date = effective_date + timedelta(days=365)
            
            # Pricing formula
            index_code = random.choice(['ETH-DEL-USG', 'PP-DEL-USG', 'BEN-FOB-USG'])
            adjustment = random.choice([30, 50, 75, 100, -25])
            
            min_volume = round(customer['annual_volume_mt'] * 0.1, 0)
            
            terms.append({
                'term_id': term_id,
                'contract_id': f'CTR-{customer["customer_id"][-5:]}',
                'customer_id': customer['customer_id'],
                'product_id': product['product_id'],
                'effective_date': effective_date.strftime('%Y-%m-%d'),
                'expiration_date': expiration_date.strftime('%Y-%m-%d'),
                'price_formula': f'{index_code} + ${adjustment}/MT',
                'base_index_code': index_code,
                'index_adjustment': adjustment,
                'fixed_price': None,
                'floor_price': round(product['standard_cost_per_mt'] * 1.05, 2),
                'ceiling_price': round(product['standard_cost_per_mt'] * 1.50, 2),
                'min_volume_mt': min_volume,
                'max_volume_mt': min_volume * 3,
                'volume_tier_1_mt': min_volume * 1.5,
                'volume_tier_1_discount': 2,
                'volume_tier_2_mt': min_volume * 2.5,
                'volume_tier_2_discount': 4,
                'payment_terms_days': customer['payment_terms_days'],
                'early_pay_discount_pct': 1 if customer['customer_tier'] == 'Platinum' else 0,
                'contract_status': 'ACTIVE',
                'contract_type': random.choice(['ANNUAL', 'MULTI_YEAR'])
            })
            term_id += 1
    
    df = pd.DataFrame(terms)
    print(f"Generated {len(df)} contract terms")
    return df


def generate_market_reports(products_df: pd.DataFrame, days: int = 180) -> pd.DataFrame:
    """
    Generate market intelligence reports for Cortex Search.
    DRD Requirement: Search by Chemical_Name and Region
    """
    print("Generating market reports for Cortex Search...")
    
    reports = []
    report_id = 1
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Templates for report content
    supply_templates = [
        "Supply constraints for {chemical} in {region} are expected to persist through Q{quarter}. Key factors include planned maintenance at {n} major facilities and unexpected outages in the upstream naphtha supply chain. Prices have risen {pct}% week-over-week.",
        "The {region} {chemical} market is experiencing tightness due to reduced imports from Asia. Local producers are operating at {util}% capacity utilization. Spot availability is limited with premiums of ${premium}/MT over contract prices.",
        "Force majeure declared at {producer} {chemical} unit in {region} has removed approximately {capacity}kt/year of capacity from the market. Recovery is expected in {weeks} weeks. Buyers are seeking alternative sources.",
    ]
    
    demand_templates = [
        "Demand for {chemical} in {region} has {direction} by {pct}% compared to the same period last year. Key end-use sectors driving this trend include {sector1} and {sector2}. Inventory levels at converters remain {inv_level}.",
        "The {region} {chemical} market outlook remains {outlook} for the next quarter. Automotive and construction sectors are showing {auto_trend} demand signals. Export opportunities to {export_region} are {export_trend}.",
        "Consumer demand in {region} for {chemical}-based products has {direction} following seasonal patterns. Packaging demand is {pkg_trend} while industrial applications show {ind_trend} growth.",
    ]
    
    price_templates = [
        "Price assessment: {chemical} CFR {region} is trading at ${price}/MT, {direction} ${change}/MT from last week. Market participants expect prices to {forecast} in the coming weeks based on feedstock movements.",
        "The spread between {chemical} and feedstock naphtha has {spread_direction} to ${spread}/MT. Producers report margins are {margin_status}. Contract negotiations for next quarter are {nego_status}.",
    ]
    
    for day_offset in range(0, days, random.randint(3, 7)):
        current_date = start_date + timedelta(days=day_offset)
        
        # Generate 2-5 reports per period
        for _ in range(random.randint(2, 5)):
            product = products_df.sample(1).iloc[0]
            region = random.choice(list(REGIONS.keys()))
            report_type = random.choice(REPORT_TYPES)
            source = random.choice(REPORT_SOURCES)
            
            # Generate content based on type
            if 'SUPPLY' in report_type:
                template = random.choice(supply_templates)
            elif 'OUTLOOK' in report_type or 'ANALYSIS' in report_type:
                template = random.choice(demand_templates)
            else:
                template = random.choice(price_templates)
            
            content = template.format(
                chemical=product['product_name'],
                region=region,
                quarter=((current_date.month - 1) // 3) + 1,
                n=random.randint(2, 5),
                pct=round(random.uniform(-10, 15), 1),
                util=random.randint(75, 95),
                premium=random.randint(20, 100),
                producer=random.choice(['Major Producer', 'Regional Supplier', 'Global Chemical Co']),
                capacity=random.randint(100, 500),
                weeks=random.randint(2, 8),
                direction=random.choice(['increased', 'decreased', 'remained stable']),
                sector1=random.choice(['packaging', 'automotive', 'construction', 'electronics']),
                sector2=random.choice(['agriculture', 'consumer goods', 'textiles', 'medical']),
                inv_level=random.choice(['high', 'moderate', 'low']),
                outlook=random.choice(['positive', 'cautious', 'challenging', 'stable']),
                auto_trend=random.choice(['strong', 'weak', 'mixed']),
                export_region=random.choice(['Europe', 'Asia', 'Latin America']),
                export_trend=random.choice(['expanding', 'limited', 'competitive']),
                pkg_trend=random.choice(['strong', 'moderate', 'subdued']),
                ind_trend=random.choice(['steady', 'improving', 'declining']),
                price=random.randint(800, 2000),
                change=random.randint(-50, 80),
                forecast=random.choice(['strengthen', 'soften', 'remain stable']),
                spread_direction=random.choice(['widened', 'narrowed', 'stabilized']),
                spread=random.randint(150, 400),
                margin_status=random.choice(['healthy', 'under pressure', 'improving']),
                nego_status=random.choice(['ongoing', 'concluded', 'contentious'])
            )
            
            title = f"{product['product_family']} Market Update: {region} - {current_date.strftime('%B %Y')}"
            
            reports.append({
                'report_id': report_id,
                'report_date': current_date.strftime('%Y-%m-%d'),
                'report_type': report_type,
                'report_title': title,
                'report_source': source,
                'chemical_name': product['product_name'],
                'region': region,
                'report_summary': content[:200] + '...',
                'report_content': content,
                'file_path': f'@MARKET_DOCUMENTS/{product["product_family"]}/{current_date.strftime("%Y-%m")}.pdf',
                'author': f'{random.choice(["John", "Sarah", "Michael", "Emily"])} {random.choice(["Smith", "Johnson", "Williams", "Chen"])}'
            })
            report_id += 1
    
    df = pd.DataFrame(reports)
    print(f"Generated {len(df)} market reports")
    return df


def generate_feedstock_mapping(products_df: pd.DataFrame) -> pd.DataFrame:
    """Generate feedstock to product mappings."""
    print("Generating feedstock mappings...")
    
    mappings = []
    mapping_id = 1
    
    # Mapping rules
    feedstock_rules = {
        'Olefins': [('ETH-DEL-USG', 1.1), ('NAPHTHA-CIF-NWE', 1.3)],
        'Polymers': [('ETH-DEL-USG', 1.05), ('PP-DEL-USG', 1.08)],
        'Aromatics': [('BEN-FOB-USG', 1.0), ('CRUDE-BRENT', 0.1)],
        'Intermediates': [('ETH-DEL-USG', 0.8), ('NAPHTHA-CIF-NWE', 0.5)],
    }
    
    for _, product in products_df.iterrows():
        rules = feedstock_rules.get(product['product_family'], [('CRUDE-BRENT', 0.05)])
        
        for feedstock_code, factor in rules:
            mappings.append({
                'mapping_id': mapping_id,
                'product_id': product['product_id'],
                'feedstock_index_code': feedstock_code,
                'conversion_factor': factor,
                'cost_weight_pct': round(100 / len(rules), 1),
                'effective_date': '2024-01-01'
            })
            mapping_id += 1
    
    df = pd.DataFrame(mappings)
    print(f"Generated {len(df)} feedstock mappings")
    return df


def generate_logistics_costs(sites_df: pd.DataFrame) -> pd.DataFrame:
    """Generate logistics cost matrix."""
    print("Generating logistics costs...")
    
    costs = []
    cost_id = 1
    
    for _, site in sites_df.iterrows():
        for region in REGIONS.keys():
            # Different transport modes
            modes = [('TRUCK', 1.0, 3), ('RAIL', 0.7, 7), ('VESSEL', 0.5, 21)]
            
            for mode, cost_mult, transit in modes:
                base_cost = REGIONS[region]['freight_base'] * cost_mult
                # Distance adjustment
                if site['region'] == region:
                    base_cost *= 0.5
                    transit = int(transit * 0.5)
                
                costs.append({
                    'cost_id': cost_id,
                    'origin_site_id': site['site_id'],
                    'destination_region': region,
                    'transport_mode': mode,
                    'cost_per_mt': round(base_cost, 2),
                    'transit_days': transit,
                    'effective_date': '2024-01-01',
                    'expiration_date': '2025-12-31',
                    'carrier': f'{mode} Carrier {random.randint(1, 5)}'
                })
                cost_id += 1
    
    df = pd.DataFrame(costs)
    print(f"Generated {len(df)} logistics cost records")
    return df


def generate_demand_history(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Generate aggregated demand history for ML training."""
    print("Generating demand history...")
    
    # Aggregate sales by week/product/region
    sales_df['order_date'] = pd.to_datetime(sales_df['order_date'])
    sales_df['week_start'] = sales_df['order_date'].dt.to_period('W').dt.start_time
    
    agg = sales_df.groupby(['week_start', 'product_id', 'sales_region', 'customer_segment']).agg({
        'quantity_mt': 'sum',
        'order_id': 'count',
        'customer_id': 'nunique',
        'unit_price': ['mean', 'min', 'max']
    }).reset_index()
    
    agg.columns = ['demand_date', 'product_id', 'region', 'customer_segment', 
                   'volume_mt', 'order_count', 'customer_count', 
                   'avg_price_per_mt', 'min_price_per_mt', 'max_price_per_mt']
    
    agg['demand_id'] = range(1, len(agg) + 1)
    agg['demand_date'] = agg['demand_date'].dt.strftime('%Y-%m-%d')
    
    # Add external factors (simulated)
    agg['crude_oil_price'] = agg.apply(lambda x: round(70 + random.uniform(-10, 20), 2), axis=1)
    agg['gdp_index'] = agg.apply(lambda x: round(100 + random.uniform(-5, 5), 1), axis=1)
    agg['manufacturing_pmi'] = agg.apply(lambda x: round(50 + random.uniform(-5, 10), 1), axis=1)
    
    print(f"Generated {len(agg)} demand history records")
    return agg


def main():
    parser = argparse.ArgumentParser(description='Generate synthetic data for Chemicals Pricing demo')
    parser.add_argument('--output-dir', default='../data/synthetic',
                        help='Output directory for generated CSV files')
    parser.add_argument('--days', type=int, default=365,
                        help='Number of days of historical data to generate')
    args = parser.parse_args()
    
    # Resolve paths
    script_dir = Path(__file__).parent
    output_dir = (script_dir / args.output_dir).resolve()
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Output directory: {output_dir}")
    print("=" * 60)
    
    # Generate all datasets
    products_df = generate_products()
    customers_df = generate_customers(200)
    sites_df = generate_sites()
    market_indices_df = generate_market_indices(args.days * 2)
    sales_df = generate_sales_orders(products_df, customers_df, sites_df, args.days)
    production_df = generate_production_runs(products_df, sites_df, args.days)
    inventory_df = generate_inventory_balances(products_df, sites_df)
    contracts_df = generate_contract_terms(products_df, customers_df)
    reports_df = generate_market_reports(products_df, 180)
    feedstock_df = generate_feedstock_mapping(products_df)
    logistics_df = generate_logistics_costs(sites_df)
    demand_df = generate_demand_history(sales_df)
    
    # Save all files
    print("\n" + "=" * 60)
    print("Saving generated data...")
    
    products_df.to_csv(output_dir / 'products.csv', index=False)
    customers_df.to_csv(output_dir / 'customers.csv', index=False)
    sites_df.to_csv(output_dir / 'sites.csv', index=False)
    market_indices_df.to_csv(output_dir / 'market_indices.csv', index=False)
    sales_df.to_csv(output_dir / 'sales_orders.csv', index=False)
    production_df.to_csv(output_dir / 'production_runs.csv', index=False)
    inventory_df.to_csv(output_dir / 'inventory_balances.csv', index=False)
    contracts_df.to_csv(output_dir / 'contract_terms.csv', index=False)
    reports_df.to_csv(output_dir / 'market_reports.csv', index=False)
    feedstock_df.to_csv(output_dir / 'feedstock_mapping.csv', index=False)
    logistics_df.to_csv(output_dir / 'logistics_costs.csv', index=False)
    demand_df.to_csv(output_dir / 'demand_history.csv', index=False)
    
    print(f"\nGenerated files in {output_dir}:")
    print(f"  - products.csv: {len(products_df)} records")
    print(f"  - customers.csv: {len(customers_df)} records")
    print(f"  - sites.csv: {len(sites_df)} records")
    print(f"  - market_indices.csv: {len(market_indices_df)} records")
    print(f"  - sales_orders.csv: {len(sales_df)} records")
    print(f"  - production_runs.csv: {len(production_df)} records")
    print(f"  - inventory_balances.csv: {len(inventory_df)} records")
    print(f"  - contract_terms.csv: {len(contracts_df)} records")
    print(f"  - market_reports.csv: {len(reports_df)} records")
    print(f"  - feedstock_mapping.csv: {len(feedstock_df)} records")
    print(f"  - logistics_costs.csv: {len(logistics_df)} records")
    print(f"  - demand_history.csv: {len(demand_df)} records")
    print("\nDone! Load these files into Snowflake using the data loading scripts.")


if __name__ == '__main__':
    main()
