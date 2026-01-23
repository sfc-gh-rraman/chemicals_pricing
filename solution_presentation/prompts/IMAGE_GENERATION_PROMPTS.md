# Image Generation Prompts for Chemicals Pricing Solution
## For Use with Gemini 3 / Imagen 3

---

## Style Guide (Apply to All Images)

**Visual Style:**
- Dark mode aesthetic with deep navy/black backgrounds (#0a0f1a to #1a1f2e)
- Glowing cyan/teal accents (#00d4ff, #22c55e)
- Subtle hexagonal grid patterns in background
- Futuristic, data-driven corporate feel
- High contrast text (white on dark)
- Subtle particle effects and light trails
- No photorealistic humans—use abstract icons or silhouettes

**Color Palette:**
- Background: Deep navy (#0a0f1a), dark blue-gray (#1a1f2e)
- Primary accent: Cyan/teal (#00d4ff, #0ea5e9)
- Secondary accent: Green (#22c55e)
- Warning/negative: Red (#ef4444)
- Text: White (#ffffff), light gray (#94a3b8)
- Charts: Viridis or cyan-to-green gradients

---

## Image 1: Problem Impact (problem-impact.png)

**Filename:** `problem-impact.png`
**Dimensions:** 1920x1080 (16:9)

**Prompt:**
```
Create a dramatic dark-themed infographic showing the financial impact of poor pricing decisions in the chemicals industry.

Central element: A massive glowing number "$3.2B" in cyan/teal with a soft glow effect, labeled "Annual Margin Leakage" below it.

Background: Deep navy with subtle hexagonal grid pattern and faint chemical molecule structures.

Below the main number, show three supporting statistics in glowing bordered boxes arranged horizontally:
- Left box: "5-15%" with text "Margin left on table from underpricing"
- Center box: "$75M" with text "Average manufacturer's annual loss"
- Right box: "85%" with text "Companies without elasticity analytics"

Include subtle imagery of a declining chart line in red fading into the background, suggesting money being lost.

Add a small icon of chemical flasks or industrial pipes in the corner to indicate the chemicals industry context.

Style: Futuristic data dashboard aesthetic, dark mode, glowing accents, corporate presentation quality.
```

---

## Image 2: Before-After Transformation (before-after.png)

**Filename:** `before-after.png`
**Dimensions:** 1920x1080 (16:9)

**Prompt:**
```
Create a split-screen before/after comparison infographic for chemicals pricing transformation.

LEFT SIDE (45% of image) - "BEFORE: Cost-Plus"
- Red/orange tint overlay
- Background shows a person silhouette looking at a basic spreadsheet on screen
- Show outdated dashboard with simple bar charts
- Warning icons and red alert boxes
- Text overlay: "Cost + 25% Margin"
- Bottom callout box in red: "$75M/YEAR MARGIN LEAKAGE" with X icon

RIGHT SIDE (45% of image) - "AFTER: Optimization-Driven"  
- Cyan/teal/green tint overlay
- Background shows futuristic dashboard with 3D charts, heatmaps, optimization curves
- Show elasticity matrix visualization and profit surface
- Checkmark icons and green success indicators
- Text overlay: "ML-Optimized Pricing"
- Bottom callout box in green: "$12M/YEAR MARGIN CAPTURE" with checkmark

CENTER (10% divider):
- Glowing arrow pointing from left to right
- Gradient transition from red to green

Add subtle chemical industry iconography (molecular structures, industrial silhouettes) in corners.

Style: High contrast, dramatic transformation visual, dark mode, futuristic feel.
```

---

## Image 3: ROI Value Breakdown (roi-value.png)

**Filename:** `roi-value.png`
**Dimensions:** 1920x1080 (16:9)

**Prompt:**
```
Create a ROI value breakdown infographic for a chemicals pricing solution.

Top section:
- Large glowing header text: "Annual Value: $12M" in cyan
- Subtitle: "For a $500M revenue chemicals manufacturer"

Center section - Stacked value bars (horizontal):
- Largest bar (cyan): "$9M - Margin Optimization" with target icon
- Medium bar (teal): "$2.5M - Eliminated Underpricing" with dollar-sign icon
- Small bar (green): "$500K - Demand Forecast Savings" with truck/logistics icon

Right side:
- Large circular gauge or meter showing "10,186% ROI" with glowing ring
- Below: "3-Year Return" text
- Upward arrow indicating growth

Bottom timeline:
- Horizontal arrow from left to right
- Marker at start: "Investment: $350K"
- Marker at 2 weeks: "Payback: <2 weeks"
- Marker at end: "Year 1 Net: $11.65M"

Background: Dark navy with subtle grid pattern, chemical molecular structures faintly visible.

Add Snowflake logo watermark in corner (or generic snowflake icon).

Style: Executive presentation quality, clean data visualization, dark mode, glowing accents.
```

---

## Image 4: Solution Architecture (architecture.png)

**Filename:** `architecture.png`
**Dimensions:** 1920x1080 (16:9)

**Prompt:**
```
Create a solution architecture diagram for a chemicals pricing analytics platform built on Snowflake.

Layout: Three-zone horizontal flow (left to right)

ZONE 1 - DATA SOURCES (left 25%):
Header: "DATA SOURCES"
Show 4 source boxes stacked vertically with icons:
- "ERP Systems" (SAP icon, database) - "Sales orders, pricing history"
- "Market Data Feeds" (chart icon) - "Feedstock indices, commodity prices"  
- "Cost Systems" (calculator icon) - "Variable costs, logistics"
- "Market Reports" (document icon) - "Analyst reports, competitor intel"
Arrows flowing right from each source

ZONE 2 - SNOWFLAKE PLATFORM (center 50%):
Large bordered box with Snowflake logo
Header: "SNOWFLAKE DATA CLOUD"
Three horizontal layers inside:

Top layer - INGESTION:
- "Snowpipe" box with "Real-time" badge
- "External Tables" box
- "Cortex Search Indexing" box

Middle layer - PROCESSING:
- Three connected boxes: "RAW Schema" → "ATOMIC Schema" → "Data Mart"
- Above the flow: "Snowpark ML" box with "Elasticity Models" label
- Show "Streams" and "Tasks" labels on arrows

Bottom layer - AI SERVICES:
- "Cortex Analyst" box
- "Cortex Search" box  
- "Cortex Agent" box

ZONE 3 - APPLICATIONS (right 25%):
Header: "APPLICATIONS"
Show 4 output boxes with icons:
- "Streamlit Dashboard" - "6 Pages, 3 Personas"
- "Cortex Analyst" - "Natural Language Queries"
- "Cortex Search" - "RAG on Market Reports"
- "Notebooks" - "ML Development"
Arrows flowing from center platform to each

Connecting lines: Glowing cyan/teal lines showing data flow with small animated dots (suggest motion)

Background: Dark navy, subtle hexagonal grid, Snowflake branding colors

Style: Technical architecture diagram, clean boxes and arrows, dark mode, professional.
```

---

## Image 5: Data ERD (data-erd.png)

**Filename:** `data-erd.png`
**Dimensions:** 1920x1080 (16:9)

**Prompt:**
```
Create a data entity relationship diagram showing three schemas for a chemicals pricing solution.

Layout: Three vertical columns representing schemas, with entities as boxes and relationships as connecting lines.

LEFT COLUMN - "RAW SCHEMA" (header in orange/amber):
Subtitle: "Landing Zone"
Show 3 entity boxes:
- MARKET_FEEDSTOCK (price_date, index_code, index_value)
- ERP_SALES_ORDERS (order_id, product_id, quantity, price)
- MARKET_REPORTS (report_id, title, content) - with document icon

CENTER COLUMN - "ATOMIC SCHEMA" (header in cyan):
Subtitle: "Normalized Entities"
Show 5 entity boxes:
- PRODUCT (product_id PK, product_name, product_family) - with "VIEW" badge
- CUSTOMER (customer_id PK, customer_name, tier)
- SALES_ORDER (order_id PK, customer_id FK, product_id FK)
- MARKET_INDEX (price_date, index_code, index_value)
- INVENTORY_BALANCE (product_id, site_id, quantity)
Show key icons (🔑) for primary keys

RIGHT COLUMN - "CHEMICAL_OPS SCHEMA" (header in green):
Subtitle: "Data Mart"
Show 5 entity boxes:
- MARGIN_ANALYZER - "VIEW" badge
- COST_TO_SERVE - "VIEW" badge
- ML_TRAINING_DATA - "VIEW" badge - with ML icon
- ELASTICITY_MATRIX - with matrix grid icon
- OPTIMAL_PRICING - with target icon

Connecting lines:
- Curved teal lines showing transformations from RAW → ATOMIC
- Curved green lines showing joins from ATOMIC → CHEMICAL_OPS
- Label one arrow "Transforms"
- Label one arrow "Joins"
- Label one arrow "ML Features"

Bottom annotation: "Data flows left to right →" with arrow

Background: Dark navy, subtle grid
Add small Snowflake logo in corner

Style: ERD diagram style, dark mode, glowing schema headers, clean typography.
```

---

## Image 6: Main Dashboard (dashboard-optimizer.png)

**Filename:** `dashboard-optimizer.png`
**Dimensions:** 1920x1080 (16:9)

**Prompt:**
```
Create a screenshot-style mockup of a chemicals pricing optimizer dashboard.

Layout: Dark themed analytics dashboard with sidebar navigation

LEFT SIDEBAR (dark, narrow):
- Logo area at top (abstract chemical/snowflake icon)
- Navigation items with icons:
  - "Margin Command" (chart icon)
  - "Pricing Desk" (dollar icon)
  - "Data Science" (brain icon)
  - "Market Intelligence" (search icon)
  - "Ask Cortex" (chat icon)
  - "Pricing Optimizer" (target icon) - HIGHLIGHTED/SELECTED

MAIN CONTENT AREA:

Top section - KPIs in 4 cards:
- "Current Profit" - "$52.5M" in blue
- "Optimal Profit" - "$54.8M" in green with "+4.4%" badge
- "Profit Gain" - "+$2.3M" in green
- "Revenue Change" - "+1.2%" in blue

Center-left - ELASTICITY MATRIX:
- 4x4 heatmap grid
- Row/column labels: "Polymers", "Aromatics", "Intermediates", "Olefins"
- Diagonal cells in dark red (negative values: -1.25, -1.52, -1.38, -1.85)
- Off-diagonal cells in blue shades (positive substitution values: 0.15, 0.28, 0.32)
- Colorbar showing scale from -2 to +1
- Title: "Cross-Price Elasticity Matrix"

Center-right - 3D PROFIT SURFACE:
- Wireframe/surface plot showing profit as function of two prices
- Cyan/green gradient coloring
- Red dot marker showing "Current State"
- Axes labeled "Polymers Price" and "Aromatics Price"
- Title: "Profit Response Surface"

Bottom - WATERFALL CHART:
- Horizontal waterfall showing margin contribution by product
- Green bars for gains, red for losses
- Blue total bar at end showing "+$2.3M"
- Title: "Margin Impact by Product"

Right sidebar - ALERT PANEL:
- Orange/yellow bordered box
- "⚠️ PRICING OPPORTUNITY" header
- "Polymers: ε = -0.8 (Inelastic)"
- "Recommendation: Consider 8% price increase"
- "Expected margin gain: +$1.2M"
- "Acknowledge" button in cyan

Footer: "Last updated: 2 minutes ago | Powered by Snowflake Cortex"

Style: Modern analytics dashboard, dark mode, Streamlit-like UI, professional data visualization.
```

---

## Image 7: Elasticity Matrix Visualization (elasticity-matrix.png)

**Filename:** `elasticity-matrix.png`
**Dimensions:** 1200x1000 (square-ish)

**Prompt:**
```
Create a detailed cross-price elasticity matrix heatmap visualization.

Main element: 4x4 matrix heatmap

Row labels (left side, demand products):
- Polymers
- Aromatics  
- Intermediates
- Olefins

Column labels (bottom, price products):
- Polymers
- Aromatics
- Intermediates
- Olefins

Cell values and colors:
Diagonal (own-price, dark red):
- Polymers/Polymers: -1.25
- Aromatics/Aromatics: -1.52
- Intermediates/Intermediates: -1.38
- Olefins/Olefins: -1.85

Off-diagonal (cross-price):
- Blue cells (substitutes, positive): 0.15, 0.28, 0.32, 0.21, 0.18, 0.14
- Light red cells (complements, small negative): -0.08, -0.05

Show values as white text overlaid on colored cells.

Color bar on right: 
- Gradient from dark red (-2) through white (0) to blue (+1)
- Label: "ε (Elasticity)"

Title above: "Cross-Price Elasticity Matrix"
Subtitle: "εᵢⱼ = ∂ln(Qᵢ)/∂ln(Pⱼ) | How demand for row product responds to column price"

Annotations:
- Arrow pointing to diagonal: "Own-Price (Negative)"
- Arrow pointing to blue off-diagonal: "Substitutes (Positive)"

Legend box in corner:
- "Diagonal: Own-price elasticity"
- "Off-diagonal positive: Substitute products"
- "Off-diagonal negative: Complement products"

Background: Dark navy with subtle grid
Style: Clean data visualization, dark mode, publication-quality chart.
```

---

## Image 8: Problem Statement Slide (margin-leakage.png)

**Filename:** `margin-leakage.png`
**Dimensions:** 1920x1080 (16:9)

**Prompt:**
```
Create a dramatic infographic showing how margin leaks occur in chemicals pricing.

Visual concept: A "leaky bucket" metaphor with money/margin flowing out

Center: Large stylized bucket or container in cyan outline, filled with glowing cyan "liquid" representing margin

Multiple leaks/holes in the bucket with streams flowing out, each labeled:
- "Underpriced Inelastic Products" - largest leak stream
- "Ignored Substitution Effects" - medium stream
- "Delayed Feedstock Pass-Through" - medium stream  
- "Gut-Feel Pricing Decisions" - smaller stream

The leaked streams flow down into a "LOST MARGIN" pool at bottom showing "$75M/year"

Top of image: Large header "Where Does Margin Go?"

Right side statistics panel:
- "62% of pricing decisions lack elasticity data"
- "3-8% volume shifts from cross-price effects"
- "15-30 day lag in feedstock response"

Add subtle industrial chemical plant silhouette in background

Color scheme:
- Bucket outline: Cyan
- Margin liquid: Glowing cyan/teal
- Leak streams: Red/orange gradient
- Lost pool: Dark red
- Text: White

Style: Infographic, metaphorical visualization, dark mode, executive presentation.
```

---

## Usage Notes

1. **Resolution:** Generate at 2x the listed dimensions for best quality, then downscale
2. **Format:** Save as PNG with transparency where appropriate
3. **Consistency:** Maintain the dark navy background (#0a0f1a) across all images
4. **Branding:** Include subtle Snowflake references but avoid explicit logos unless licensed
5. **Accessibility:** Ensure text contrast meets WCAG guidelines (white on dark)

---

## Quick Reference - All Images

| # | Filename | Description | Priority |
|---|----------|-------------|----------|
| 1 | problem-impact.png | $3.2B industry losses headline | HIGH |
| 2 | before-after.png | Cost-plus vs Optimization split | HIGH |
| 3 | roi-value.png | $12M annual value breakdown | HIGH |
| 4 | architecture.png | Solution architecture diagram | HIGH |
| 5 | data-erd.png | Three-schema data model | MEDIUM |
| 6 | dashboard-optimizer.png | Main optimizer dashboard mockup | HIGH |
| 7 | elasticity-matrix.png | Cross-price elasticity heatmap | MEDIUM |
| 8 | margin-leakage.png | Leaky bucket margin metaphor | MEDIUM |
