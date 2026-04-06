# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "streamlit",
#   "pandas",
#   "plotly",
#   "snowflake-connector-python",
# ]
# ///
"""
ETF EDA Streamlit App — SQL-first (all aggregations pushed to Snowflake)
Run: uv run streamlit run ETF_DB/etf_eda_app.py
"""
import os
import pathlib
import tomllib

import pandas as pd
import plotly.express as px
import streamlit as st
import snowflake.connector

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(page_title="ETF EDA", layout="wide", page_icon="📊")
st.title("📊 ETF Data — Exploratory Data Analysis")
st.caption("Source: `ETF_DB.LOCAL_COPY` · CONSTITUENTS (1.17M rows) + INDUSTRY (4,152 ETFs)")

# ── Connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    config_path = pathlib.Path.home() / ".snowflake" / "config.toml"
    with open(config_path, "rb") as f:
        toml = tomllib.load(f)
    conn_name = toml.get("default_connection_name") or os.environ.get("SNOWFLAKE_CONNECTION", "snowconn")
    cfg = toml["connections"][conn_name]
    return snowflake.connector.connect(
        account=cfg["account"], user=cfg["user"], password=cfg["password"],
        role=cfg.get("role", "ACCOUNTADMIN"), warehouse=cfg.get("warehouse", "cortex_analyst_wh"),
    )

@st.cache_data(ttl=3600)
def q(sql):
    df = pd.read_sql(sql, get_conn())
    df.columns = df.columns.str.lower()
    return df

# ── Preload INDUSTRY + find best CONSTITUENTS snapshot date ───────────────
with st.spinner("Loading INDUSTRY…"):
    ind = q("SELECT * FROM ETF_DB.LOCAL_COPY.INDUSTRY")
    ind["aum_bn"] = ind["aum"] / 1e9
    ind["net_expenses_bps"] = ind["net_expenses"] * 100

# Best date = snapshot with the most ETFs (2025-04-01 has 3,748 vs 2025-04-02 has 11)
_best = q("""
    SELECT as_of_date FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
    GROUP BY as_of_date ORDER BY COUNT(*) DESC LIMIT 1
""")
BEST_DATE = str(_best["as_of_date"].iloc[0])[:10]
st.sidebar.caption(f"CONSTITUENTS snapshot: **{BEST_DATE}**")

# ── Preload SECURITIES master ──────────────────────────────────────────────
with st.spinner("Loading SECURITIES…"):
    sec = q("SELECT * FROM ETF_DB.LOCAL_COPY.SECURITIES")

# ── Sidebar nav ────────────────────────────────────────────────────────────
section = st.sidebar.radio(
    "Section",
    ["🏦 Universe Overview",
     "💰 AUM & Issuers",
     "💸 Fee Analysis",
     "🌍 Exposure",
     "📋 Holdings Analysis",
     "🔗 Combined Analysis",
     "⏱ Time-Series",
     "🔍 Ticker Deep Dive",
     "📌 Security Lookup"],
)

# ══════════════════════════════════════════════════════════════════════════
if section == "🏦 Universe Overview":
    st.header("ETF Universe Overview")

    totals = q(f"""
        SELECT
          (SELECT COUNT(DISTINCT composite_ticker) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
           WHERE as_of_date = '{BEST_DATE}') AS etf_count,
          (SELECT COUNT(DISTINCT constituent_ticker) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
           WHERE as_of_date = '{BEST_DATE}') AS stock_count,
          (SELECT COUNT(*) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
           WHERE as_of_date = '{BEST_DATE}') AS holding_rows,
          '{BEST_DATE}'::date AS latest_date
    """)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total ETFs (INDUSTRY)", f"{len(ind):,}")
    c2.metric("ETFs (latest snapshot)", f"{totals['etf_count'].iloc[0]:,}")
    c3.metric("Unique Securities", f"{totals['stock_count'].iloc[0]:,}")
    c4.metric("Holdings (latest)", f"{totals['holding_rows'].iloc[0]:,}")
    c5.metric("Total AUM", f"${ind['aum_bn'].sum():,.0f}B")

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        ac = ind["asset_class"].fillna("Unknown").value_counts().reset_index()
        ac.columns = ["asset_class", "count"]
        fig = px.bar(ac, x="count", y="asset_class", orientation="h", color="asset_class",
                     title="ETF Count by Asset Class",
                     labels={"count": "Number of ETFs", "asset_class": ""})
        fig.update_layout(showlegend=False, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.pie(ac, values="count", names="asset_class", title="ETF Universe Composition")
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Missing Values — INDUSTRY")
    null_pct = (ind.isnull().mean() * 100).sort_values(ascending=False)
    null_pct = null_pct[null_pct > 0].reset_index()
    null_pct.columns = ["column", "pct_missing"]
    fig = px.bar(null_pct, x="pct_missing", y="column", orientation="h",
                 labels={"pct_missing": "% Missing", "column": ""},
                 title="Missing Values by Column (INDUSTRY)")
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=420)
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════
elif section == "💰 AUM & Issuers":
    st.header("AUM & Issuer Analysis")

    aum = ind[ind["aum_bn"].notna() & (ind["aum_bn"] > 0)].copy()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ETFs with AUM data", f"{len(aum):,}")
    c2.metric("Total AUM", f"${aum['aum_bn'].sum():,.0f}B")
    c3.metric("Median AUM", f"${aum['aum_bn'].median():.3f}B")
    c4.metric("Largest ETF", f"${aum['aum_bn'].max():,.1f}B")

    col1, col2 = st.columns(2)
    with col1:
        fig = px.histogram(aum, x="aum_bn", nbins=60, log_y=True,
                           title="AUM Distribution (log y-axis)",
                           labels={"aum_bn": "AUM ($B)"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        import numpy as np
        aum["log_aum"] = np.log10(aum["aum_bn"].clip(lower=1e-6))
        fig = px.histogram(aum, x="log_aum", nbins=60,
                           title="AUM Distribution (log₁₀ scale)",
                           labels={"log_aum": "log₁₀(AUM $B)"})
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top 15 ETFs by AUM")
    top15 = (aum.nlargest(15, "aum_bn")
             [["composite_ticker","description","issuer","asset_class","aum_bn"]]
             .rename(columns={"composite_ticker":"Ticker","description":"Name",
                              "issuer":"Issuer","asset_class":"Asset Class","aum_bn":"AUM ($B)"}))
    top15["AUM ($B)"] = top15["AUM ($B)"].round(1)
    top15["Name"] = top15["Name"].str[:50]
    st.dataframe(top15, use_container_width=True, hide_index=True)

    st.subheader("Issuer Landscape")
    issuer = (aum.groupby("issuer")
              .agg(etf_count=("composite_ticker","count"), total_aum_bn=("aum_bn","sum"))
              .sort_values("total_aum_bn", ascending=False).head(12).reset_index())

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(issuer, x="total_aum_bn", y="issuer", orientation="h",
                     color="total_aum_bn", color_continuous_scale="Blues",
                     title="Top 12 Issuers by AUM ($B)",
                     labels={"total_aum_bn":"AUM ($B)","issuer":""})
        fig.update_layout(yaxis={"categoryorder":"total ascending"}, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(issuer, x="etf_count", y="issuer", orientation="h",
                     color="etf_count", color_continuous_scale="Oranges",
                     title="Top 12 Issuers by ETF Count",
                     labels={"etf_count":"ETF Count","issuer":""})
        fig.update_layout(yaxis={"categoryorder":"total ascending"}, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("AUM Concentration Curve")
    sorted_aum = aum.sort_values("aum_bn", ascending=False)["aum_bn"]
    cum_pct = (sorted_aum.cumsum() / sorted_aum.sum() * 100).reset_index(drop=True)
    fig = px.line(x=range(1, len(cum_pct)+1), y=cum_pct,
                  labels={"x":"ETFs ranked by AUM","y":"Cumulative % of Total AUM"},
                  title="AUM Concentration Curve")
    for n in [10, 50, 100, 200]:
        if n <= len(cum_pct):
            fig.add_vline(x=n, line_dash="dash", line_color="gray",
                          annotation_text=f"Top {n}: {cum_pct.iloc[n-1]:.1f}%",
                          annotation_position="bottom right")
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════
elif section == "💸 Fee Analysis":
    st.header("Fee Structure Analysis")

    fees = ind[ind["net_expenses_bps"].notna() & (ind["net_expenses_bps"] > 0)].copy()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ETFs with Fee Data", f"{len(fees):,}")
    c2.metric("Min Expense Ratio", f"{fees['net_expenses_bps'].min():.1f} bps")
    c3.metric("Median Expense Ratio", f"{fees['net_expenses_bps'].median():.0f} bps")
    c4.metric("Max Expense Ratio", f"{fees['net_expenses_bps'].max():.0f} bps")

    col1, col2 = st.columns(2)
    with col1:
        fig = px.histogram(fees, x=fees["net_expenses_bps"].clip(upper=200), nbins=60,
                           title="Expense Ratio Distribution (clipped 200 bps)",
                           labels={"x":"Net Expense Ratio (bps)"})
        fig.add_vline(x=fees["net_expenses_bps"].median(), line_dash="dash", line_color="red",
                      annotation_text=f"Median: {fees['net_expenses_bps'].median():.0f} bps")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fee_ac = fees.groupby("asset_class")["net_expenses_bps"].median().sort_values().reset_index()
        fig = px.bar(fee_ac, x="net_expenses_bps", y="asset_class", orientation="h",
                     color="net_expenses_bps", color_continuous_scale="Reds",
                     title="Median Expense Ratio by Asset Class (bps)",
                     labels={"net_expenses_bps":"Median (bps)","asset_class":""})
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top 15 Categories by Median Fee (min 5 ETFs)")
    fee_cat = (fees.groupby("category")["net_expenses_bps"]
               .agg(["median","count"]).query("count >= 5")
               .sort_values("median", ascending=False).head(15).reset_index())
    fig = px.bar(fee_cat, x="median", y="category", orientation="h",
                 color="median", color_continuous_scale="YlOrRd",
                 labels={"median":"Median (bps)","category":""},
                 title="Highest-Fee ETF Categories")
    fig.update_layout(yaxis={"categoryorder":"total ascending"}, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("AUM vs Expense Ratio")
    fee_aum = fees[fees["aum_bn"].notna() & (fees["aum_bn"] > 0)].copy()
    fig = px.scatter(fee_aum, x="aum_bn", y="net_expenses_bps",
                     color="asset_class", opacity=0.5, log_x=True,
                     hover_data=["composite_ticker","description","issuer"],
                     labels={"aum_bn":"AUM ($B, log)","net_expenses_bps":"Expense Ratio (bps)"},
                     title="AUM vs Expense Ratio")
    fig.update_traces(marker_size=5)
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════
elif section == "🌍 Exposure":
    st.header("Geographic & Sector Exposure")

    col1, col2 = st.columns(2)
    with col1:
        region = ind["region"].fillna("Unknown").value_counts().head(15).reset_index()
        region.columns = ["region","count"]
        fig = px.bar(region, x="count", y="region", orientation="h",
                     color="count", color_continuous_scale="Blues",
                     title="ETF Count by Region", labels={"count":"ETFs","region":""})
        fig.update_layout(yaxis={"categoryorder":"total ascending"}, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        dev = ind["development_class"].fillna("Unknown").value_counts().reset_index()
        dev.columns = ["class","count"]
        fig = px.pie(dev, values="count", names="class", title="Market Development Class")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Sector Exposure — Equity ETFs")
    sector = (ind[ind["asset_class"]=="Equity"]["sector_exposure"]
              .fillna("Broad").value_counts().head(20).reset_index())
    sector.columns = ["sector","count"]
    fig = px.bar(sector, x="count", y="sector", orientation="h",
                 color="count", color_continuous_scale="Teal",
                 title="Top 20 Sector Exposures (Equity ETFs)",
                 labels={"count":"ETFs","sector":""})
    fig.update_layout(yaxis={"categoryorder":"total ascending"}, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        lev = ind["is_levered"].value_counts().reset_index()
        lev.columns = ["lev","count"]
        lev["label"] = lev["lev"].map({True:"Leveraged", False:"Non-Leveraged"})
        fig = px.pie(lev, values="count", names="label", title="Leveraged ETFs",
                     color_discrete_sequence=["tomato","steelblue"])
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        etn = ind["is_etn"].value_counts().reset_index()
        etn.columns = ["etn","count"]
        etn["label"] = etn["etn"].map({True:"ETN", False:"ETF"})
        fig = px.pie(etn, values="count", names="label", title="ETF vs ETN",
                     color_discrete_sequence=["darkorange","steelblue"])
        st.plotly_chart(fig, use_container_width=True)
    with col3:
        dist = ind["distribution_frequency"].fillna("Unknown").value_counts().head(6).reset_index()
        dist.columns = ["freq","count"]
        fig = px.bar(dist, x="freq", y="count", title="Distribution Frequency",
                     labels={"freq":"","count":"ETF Count"})
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════
elif section == "📋 Holdings Analysis":
    st.header("Constituent Holdings Analysis")
    st.caption("All computations pushed to Snowflake — only aggregated results returned")

    # All aggregations done in SQL
    overview = q(f"""
        SELECT
          '{BEST_DATE}'::date AS latest_date,
          COUNT(DISTINCT composite_ticker) AS etf_count,
          COUNT(DISTINCT constituent_ticker) AS stock_count,
          COUNT(*) AS total_rows,
          ROUND(AVG(weight),4) AS avg_weight,
          ROUND(MEDIAN(weight),4) AS median_weight
        FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
        WHERE as_of_date = '{BEST_DATE}'
    """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ETFs", f"{overview['etf_count'].iloc[0]:,}")
    c2.metric("Unique Securities", f"{overview['stock_count'].iloc[0]:,}")
    c3.metric("Avg Weight", f"{overview['avg_weight'].iloc[0]:.4f}%")
    c4.metric("Median Weight", f"{overview['median_weight'].iloc[0]:.4f}%")

    with st.spinner("Loading holdings distributions…"):
        holdings_dist = q(f"""
            WITH h AS (
              SELECT composite_ticker, COUNT(*) AS n
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              WHERE as_of_date = '{BEST_DATE}'
              GROUP BY composite_ticker
            )
            SELECT
              WIDTH_BUCKET(LEAST(n, 500), 0, 500, 50) AS bucket,
              MIN(n) AS bucket_min,
              MAX(n) AS bucket_max,
              COUNT(*) AS etf_count
            FROM h GROUP BY 1 ORDER BY 1
        """)

        weight_dist = q(f"""
            SELECT
              WIDTH_BUCKET(LEAST(weight, 5), 0, 5, 60) AS bucket,
              MIN(weight) AS bucket_min,
              MAX(weight) AS bucket_max,
              COUNT(*) AS cnt
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
            WHERE as_of_date = '{BEST_DATE}'
              AND weight > 0
            GROUP BY 1 ORDER BY 1
        """)

        top10_conc = q(f"""
            WITH ranked AS (
              SELECT composite_ticker, weight,
                ROW_NUMBER() OVER (PARTITION BY composite_ticker ORDER BY weight DESC) AS rn
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              WHERE as_of_date = '{BEST_DATE}'
            ),
            top10 AS (
              SELECT composite_ticker, SUM(weight) AS top10_wt
              FROM ranked WHERE rn <= 10
              GROUP BY composite_ticker
            )
            SELECT
              WIDTH_BUCKET(top10_wt, 0, 105, 21) AS bucket,
              MIN(top10_wt) AS wt_min,
              MAX(top10_wt) AS wt_max,
              COUNT(*) AS etf_count
            FROM top10 GROUP BY 1 ORDER BY 1
        """)

    col1, col2 = st.columns(2)
    with col1:
        holdings_dist["label"] = holdings_dist["bucket_min"].round(0).astype(int).astype(str)
        fig = px.bar(holdings_dist, x="bucket_min", y="etf_count",
                     title="Holdings per ETF Distribution (clipped 500)",
                     labels={"bucket_min":"Holdings (lower bound)","etf_count":"ETF Count"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(weight_dist, x="bucket_min", y="cnt",
                     title="Constituent Weight Distribution (clipped 5%)",
                     labels={"bucket_min":"Weight % (lower bound)","cnt":"Count"})
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Portfolio Concentration — Top-10 Holdings Weight")
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(top10_conc, x="wt_min", y="etf_count",
                     title="Top-10 Holdings Weight Sum per ETF",
                     labels={"wt_min":"Top-10 Weight % (lower bound)","etf_count":"ETF Count"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        bins_df = pd.DataFrame({
            "range": ["0-20%","20-40%","40-60%","60-80%","80-100%",">100%"],
            "count": [
                top10_conc[top10_conc["wt_min"] <= 20]["etf_count"].sum(),
                top10_conc[(top10_conc["wt_min"] > 20) & (top10_conc["wt_min"] <= 40)]["etf_count"].sum(),
                top10_conc[(top10_conc["wt_min"] > 40) & (top10_conc["wt_min"] <= 60)]["etf_count"].sum(),
                top10_conc[(top10_conc["wt_min"] > 60) & (top10_conc["wt_min"] <= 80)]["etf_count"].sum(),
                top10_conc[(top10_conc["wt_min"] > 80) & (top10_conc["wt_min"] <= 100)]["etf_count"].sum(),
                top10_conc[top10_conc["wt_min"] > 100]["etf_count"].sum(),
            ]
        })
        fig = px.bar(bins_df, x="range", y="count", title="ETFs by Concentration Bucket",
                     labels={"range":"Top-10 Weight","count":"ETF Count"})
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Most Widely Held Securities")
    with st.spinner("Loading top securities…"):
        ubiquity = q(f"""
            SELECT constituent_ticker, constituent_name,
                   COUNT(DISTINCT composite_ticker) AS etf_count,
                   ROUND(AVG(weight),4) AS avg_weight,
                   SUM(market_value) AS total_market_value
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
            WHERE as_of_date = '{BEST_DATE}'
            GROUP BY 1, 2
            ORDER BY etf_count DESC
            LIMIT 25
        """)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(ubiquity, x="etf_count", y="constituent_ticker", orientation="h",
                     hover_data=["constituent_name"],
                     title="Top 25 Stocks by ETF Appearance Count",
                     labels={"etf_count":"Number of ETFs","constituent_ticker":""})
        fig.update_layout(yaxis={"categoryorder":"total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        mv = ubiquity[ubiquity["total_market_value"] > 0].copy()
        mv["name_short"] = mv["constituent_name"].str[:25]
        fig = px.treemap(mv, path=["name_short"], values="total_market_value",
                         hover_data=["constituent_ticker"],
                         title="Top 25 by Aggregate Market Value")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Country & Exchange Breakdown")
    with st.spinner("Loading country/exchange data…"):
        country = q(f"""
            SELECT COALESCE(country_of_exchange,'Unknown') AS country,
                   COUNT(*) AS cnt
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
            WHERE as_of_date = '{BEST_DATE}'
            GROUP BY 1 ORDER BY cnt DESC LIMIT 15
        """)
        currency = q(f"""
            SELECT COALESCE(currency_traded,'Unknown') AS currency,
                   COUNT(*) AS cnt
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
            WHERE as_of_date = '{BEST_DATE}'
            GROUP BY 1 ORDER BY cnt DESC LIMIT 10
        """)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(country, x="cnt", y="country", orientation="h",
                     title="Top 15 Countries of Exchange",
                     labels={"cnt":"Holdings","country":""})
        fig.update_layout(yaxis={"categoryorder":"total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(currency, x="currency", y="cnt",
                     title="Holdings by Trading Currency",
                     labels={"currency":"","cnt":"Holdings"})
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════
elif section == "🔗 Combined Analysis":
    st.header("Combined: INDUSTRY ⋈ CONSTITUENTS")
    st.caption("Joined on `composite_ticker` (latest CONSTITUENTS snapshot)")

    with st.spinner("Running join analysis…"):
        # Use ROW_NUMBER to avoid correlated subqueries (not supported by Snowflake)
        combined_sql = q(f"""
            WITH ranked AS (
              SELECT composite_ticker, weight,
                     ROW_NUMBER() OVER (PARTITION BY composite_ticker ORDER BY weight DESC) AS rn
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              WHERE as_of_date = '{BEST_DATE}'
            ),
            top10 AS (
              SELECT composite_ticker, SUM(weight) AS top10_weight
              FROM ranked WHERE rn <= 10
              GROUP BY composite_ticker
            ),
            actual AS (
              SELECT composite_ticker,
                     COUNT(*) AS actual_holdings,
                     SUM(weight) AS total_weight
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              WHERE as_of_date = '{BEST_DATE}'
              GROUP BY composite_ticker
            ),
            joined AS (
              SELECT i.composite_ticker, i.asset_class, i.issuer,
                     i.aum / 1e9 AS aum_bn,
                     i.net_expenses * 100 AS net_expenses_bps,
                     i.num_holdings AS reported_holdings,
                     a.actual_holdings, t.top10_weight
              FROM ETF_DB.LOCAL_COPY.INDUSTRY i
              JOIN actual a ON i.composite_ticker = a.composite_ticker
              JOIN top10 t ON i.composite_ticker = t.composite_ticker
            )
            SELECT asset_class,
                   COUNT(*) AS etf_count,
                   ROUND(SUM(aum_bn),1) AS total_aum_bn,
                   ROUND(MEDIAN(aum_bn),3) AS median_aum_bn,
                   ROUND(MEDIAN(actual_holdings),0) AS median_holdings,
                   ROUND(MEDIAN(net_expenses_bps),1) AS median_expense_bps,
                   ROUND(MEDIAN(top10_weight),1) AS median_top10_weight
            FROM joined
            WHERE aum_bn > 0
            GROUP BY 1
            ORDER BY total_aum_bn DESC
        """)

        scatter_data = q(f"""
            WITH ranked AS (
              SELECT composite_ticker, weight,
                     ROW_NUMBER() OVER (PARTITION BY composite_ticker ORDER BY weight DESC) AS rn
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              WHERE as_of_date = '{BEST_DATE}'
            ),
            top10 AS (
              SELECT composite_ticker, SUM(weight) AS top10_weight
              FROM ranked WHERE rn <= 10
              GROUP BY composite_ticker
            ),
            actual AS (
              SELECT composite_ticker, COUNT(*) AS actual_holdings
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              WHERE as_of_date = '{BEST_DATE}'
              GROUP BY composite_ticker
            )
            SELECT i.composite_ticker, i.asset_class, i.description,
                   i.issuer, i.aum / 1e9 AS aum_bn,
                   i.net_expenses * 100 AS net_expenses_bps,
                   i.num_holdings AS reported_holdings,
                   a.actual_holdings, t.top10_weight
            FROM ETF_DB.LOCAL_COPY.INDUSTRY i
            JOIN actual a ON i.composite_ticker = a.composite_ticker
            JOIN top10 t ON i.composite_ticker = t.composite_ticker
            WHERE i.aum > 0
        """)

    st.subheader("Summary by Asset Class")
    st.dataframe(combined_sql, use_container_width=True, hide_index=True)

    st.subheader("Reported vs Actual Holdings")
    match = scatter_data[scatter_data["reported_holdings"].notna()].copy()
    col1, col2 = st.columns(2)
    with col1:
        fig = px.scatter(match, x="reported_holdings", y="actual_holdings",
                         color="asset_class", opacity=0.4,
                         hover_data=["composite_ticker"],
                         title="Reported vs Actual Holdings",
                         labels={"reported_holdings":"Reported (INDUSTRY)",
                                 "actual_holdings":"Actual (CONSTITUENTS)"})
        mx = max(match["reported_holdings"].max(), match["actual_holdings"].max())
        fig.add_shape(type="line", x0=0, y0=0, x1=mx, y1=mx,
                      line=dict(color="red", dash="dash"))
        fig.update_traces(marker_size=4)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        scatter_data["delta"] = scatter_data["actual_holdings"] - scatter_data["reported_holdings"]
        fig = px.histogram(scatter_data.dropna(subset=["delta"]),
                           x=scatter_data["delta"].clip(-100, 100),
                           nbins=60, title="Discrepancy: Actual − Reported (clipped ±100)",
                           labels={"x":"Difference"})
        fig.add_vline(x=0, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.scatter(scatter_data, x="actual_holdings", y="aum_bn",
                         color="asset_class", opacity=0.4, log_x=True, log_y=True,
                         hover_data=["composite_ticker","description"],
                         title="AUM vs Holdings Count",
                         labels={"actual_holdings":"Holdings (log)","aum_bn":"AUM $B (log)"})
        fig.update_traces(marker_size=4)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.box(scatter_data[scatter_data["net_expenses_bps"].notna() &
                                   (scatter_data["net_expenses_bps"] > 0) &
                                   scatter_data["asset_class"].notna()],
                     x="asset_class", y="net_expenses_bps",
                     color="asset_class", points=False,
                     title="Expense Ratio by Asset Class",
                     labels={"net_expenses_bps":"Expense Ratio (bps)","asset_class":""})
        fig.update_yaxes(range=[0, 200])
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════
elif section == "⏱ Time-Series":
    st.header("Time-Series Analysis — 4 Snapshots")

    with st.spinner("Loading time-series data…"):
        snapshots = q("""
            SELECT as_of_date,
                   COUNT(DISTINCT composite_ticker) AS etf_count,
                   COUNT(DISTINCT constituent_ticker) AS stock_count,
                   COUNT(*) AS total_holdings,
                   ROUND(AVG(weight),4) AS avg_weight,
                   ROUND(MEDIAN(weight),4) AS median_weight
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
            GROUP BY as_of_date ORDER BY as_of_date
        """)
        ts_agg = q("""
            WITH stable AS (
              SELECT composite_ticker
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              GROUP BY composite_ticker
              HAVING COUNT(DISTINCT as_of_date) = (
                SELECT COUNT(DISTINCT as_of_date) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS)
            )
            SELECT c.as_of_date,
                   MEDIAN(sub.n) AS median_holdings,
                   MEDIAN(sub.tw) AS median_total_weight
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS c
            JOIN stable s ON c.composite_ticker = s.composite_ticker
            JOIN (
              SELECT as_of_date, composite_ticker,
                     COUNT(*) AS n, SUM(weight) AS tw
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              GROUP BY 1, 2
            ) sub ON sub.as_of_date = c.as_of_date AND sub.composite_ticker = c.composite_ticker
            GROUP BY c.as_of_date
            ORDER BY c.as_of_date
        """)
        changes = q(f"""
            WITH stable AS (
              SELECT composite_ticker
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
              GROUP BY composite_ticker
              HAVING COUNT(DISTINCT as_of_date) = (
                SELECT COUNT(DISTINCT as_of_date) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS)
            ),
            dated AS (
              SELECT c.composite_ticker, c.as_of_date, COUNT(*) AS n
              FROM ETF_DB.LOCAL_COPY.CONSTITUENTS c
              JOIN stable s ON c.composite_ticker = s.composite_ticker
              GROUP BY 1, 2
            ),
            pivot AS (
              SELECT composite_ticker,
                MAX(CASE WHEN as_of_date = (SELECT MIN(as_of_date) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS) THEN n END) AS first_n,
                MAX(CASE WHEN as_of_date = '{BEST_DATE}' THEN n END) AS last_n
              FROM dated GROUP BY composite_ticker
            )
            SELECT
              WIDTH_BUCKET(LEAST(GREATEST(last_n - first_n, -50), 50), -50, 50, 50) AS bucket,
              MIN(last_n - first_n) AS delta_min,
              COUNT(*) AS etf_count
            FROM pivot
            WHERE first_n IS NOT NULL AND last_n IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """)

    st.subheader("Snapshot Overview")
    st.dataframe(snapshots, use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(ts_agg, x="as_of_date", y="median_holdings", markers=True,
                      title="Median Holdings per ETF Across Snapshots",
                      labels={"as_of_date":"Date","median_holdings":"Median Holdings"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.line(ts_agg, x="as_of_date", y="median_total_weight", markers=True,
                      title="Median Total Weight per ETF",
                      labels={"as_of_date":"Date","median_total_weight":"Median Total Weight (%)"})
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Holdings Count Change — First → Last Snapshot")
    fig = px.bar(changes, x="delta_min", y="etf_count",
                 title="Holdings Count Change Distribution (clipped ±50)",
                 labels={"delta_min":"Δ Holdings (lower bound)","etf_count":"ETF Count"})
    fig.add_vline(x=0, line_dash="dash", line_color="red")
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════
elif section == "🔍 Ticker Deep Dive":
    st.header("Ticker Deep Dive")

    # Build sorted list of tickers that have holdings on BEST_DATE
    ticker_list = sorted(
        ind["composite_ticker"].dropna().unique().tolist()
    )
    default_idx = ticker_list.index("SPY") if "SPY" in ticker_list else 0
    ticker = st.selectbox("Select ETF ticker", ticker_list, index=default_idx)

    # ── Fund metadata from INDUSTRY ───────────────────────────────────────
    meta = ind[ind["composite_ticker"] == ticker]
    if meta.empty:
        st.warning(f"No INDUSTRY record found for {ticker}.")
        st.stop()
    meta = meta.iloc[0]

    st.subheader(f"{ticker} — {meta.get('description', '') or ''}")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Issuer", meta.get("issuer") or "—")
    c2.metric("Asset Class", meta.get("asset_class") or "—")
    c3.metric("AUM", f"${meta['aum_bn']:.2f}B" if pd.notna(meta.get("aum_bn")) else "—")
    c4.metric("Expense Ratio",
              f"{meta['net_expenses_bps']:.0f} bps"
              if pd.notna(meta.get("net_expenses_bps")) else "—")
    c5.metric("# Holdings (reported)",
              f"{int(meta['num_holdings']):,}"
              if pd.notna(meta.get("num_holdings")) else "—")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Fund details**")
        details = {
            "Category": meta.get("category"),
            "Region": meta.get("region"),
            "Sector Exposure": meta.get("sector_exposure"),
            "Development Class": meta.get("development_class"),
            "Benchmark": meta.get("primary_benchmark"),
            "Listing Exchange": meta.get("listing_exchange"),
            "Inception Date": meta.get("inception_date"),
            "Distribution Freq.": meta.get("distribution_frequency"),
            "Leveraged": meta.get("is_levered"),
            "ETN": meta.get("is_etn"),
        }
        st.table(
            pd.DataFrame({"Value": {k: ("—" if v is None or (isinstance(v, float) and pd.isna(v)) else v)
                                    for k, v in details.items()}})
        )
    with col2:
        st.markdown("**Liquidity & trading**")
        trading = {
            "Bid-Ask Spread": f"{meta['bid_ask_spread']:.4f}" if pd.notna(meta.get("bid_ask_spread")) else "—",
            "Discount/Premium": f"{meta['discount_premium']:.4f}" if pd.notna(meta.get("discount_premium")) else "—",
            "Short Interest": f"{int(meta['short_interest']):,}" if pd.notna(meta.get("short_interest")) else "—",
        }
        st.table(pd.DataFrame({"Value": trading}))

    st.divider()

    # ── Holdings from CONSTITUENTS ────────────────────────────────────────
    with st.spinner(f"Loading holdings for {ticker}…"):
        holdings = q(f"""
            SELECT constituent_ticker, constituent_name, asset_class,
                   security_type, country_of_exchange, currency_traded,
                   ROUND(weight, 4) AS weight,
                   market_value, shares_held
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
            WHERE composite_ticker = '{ticker}'
              AND as_of_date = '{BEST_DATE}'
            ORDER BY weight DESC
        """)

    if holdings.empty:
        st.info(f"No constituent data for {ticker} on {BEST_DATE}.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Actual Holdings", f"{len(holdings):,}")
        c2.metric("Total Weight", f"{holdings['weight'].sum():.2f}%")
        c3.metric("Top-10 Weight", f"{holdings.head(10)['weight'].sum():.2f}%")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Top 25 Holdings")
            top25 = holdings.head(25)[
                ["constituent_ticker", "constituent_name", "weight", "market_value"]
            ].copy()
            top25["constituent_name"] = top25["constituent_name"].str[:35]
            top25 = top25.rename(columns={
                "constituent_ticker": "Ticker",
                "constituent_name": "Name",
                "weight": "Weight (%)",
                "market_value": "Market Value ($)",
            })
            top25["Market Value ($)"] = top25["Market Value ($)"].apply(
                lambda v: f"${v:,.0f}" if pd.notna(v) else "—"
            )
            st.dataframe(top25, use_container_width=True, hide_index=True)

        with col2:
            fig = px.bar(
                holdings.head(25),
                x="weight", y="constituent_ticker",
                orientation="h",
                hover_data=["constituent_name"],
                title="Top 25 Holdings by Weight (%)",
                labels={"weight": "Weight (%)", "constituent_ticker": ""},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Portfolio Breakdown")
        col1, col2 = st.columns(2)
        with col1:
            ac_w = (
                holdings.groupby("asset_class")["weight"].sum()
                .sort_values(ascending=False).reset_index()
            )
            fig = px.pie(ac_w, values="weight", names="asset_class",
                         title="Weight by Asset Class")
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            country_w = (
                holdings.groupby("country_of_exchange")["weight"].sum()
                .sort_values(ascending=False).head(10).reset_index()
            )
            fig = px.bar(country_w, x="weight", y="country_of_exchange",
                         orientation="h",
                         title="Top 10 Countries by Weight (%)",
                         labels={"weight": "Weight (%)", "country_of_exchange": ""})
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Weight Distribution")
        fig = px.histogram(
            holdings[holdings["weight"] > 0],
            x="weight", nbins=50,
            title="Constituent Weight Distribution (%)",
            labels={"weight": "Weight (%)"},
        )
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════
elif section == "📌 Security Lookup":
    st.header("Security (Constituent Ticker) Lookup")
    st.caption(f"Which ETFs hold a given security — snapshot {BEST_DATE}")

    # ── Ticker search (selectbox backed by preloaded SECURITIES) ─────────
    ticker_options = sorted(sec["constituent_ticker"].dropna().unique().tolist())
    default_idx = ticker_options.index("AAPL") if "AAPL" in ticker_options else 0
    cticker = st.selectbox("Select or type a constituent ticker", ticker_options, index=default_idx)

    # ── Security metadata from in-memory SECURITIES ───────────────────────
    sec_row = sec[sec["constituent_ticker"] == cticker]
    if sec_row.empty:
        st.warning(f"**{cticker}** not found in SECURITIES master.")
        st.stop()
    sec_meta = sec_row.iloc[0]

    # ── ETF holdings from CONSTITUENTS (position/weight data only) ────────
    with st.spinner(f"Querying holdings for {cticker}…"):
        etf_rows = q(f"""
            SELECT
                c.composite_ticker   AS etf_ticker,
                i.description        AS etf_name,
                i.issuer,
                i.asset_class,
                i.category,
                i.aum / 1e9          AS aum_bn,
                i.net_expenses * 100 AS net_expenses_bps,
                ROUND(c.weight, 4)   AS weight,
                c.market_value,
                c.shares_held
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS c
            LEFT JOIN ETF_DB.LOCAL_COPY.INDUSTRY i
                ON c.composite_ticker = i.composite_ticker
            WHERE c.constituent_ticker = '{cticker}'
              AND c.as_of_date = '{BEST_DATE}'
            ORDER BY c.weight DESC
        """)

    if etf_rows.empty:
        st.info(f"**{cticker}** exists in SECURITIES but has no holdings on {BEST_DATE}.")
        st.stop()

    # ── Security summary ──────────────────────────────────────────────────
    sec_name = sec_meta.get("constituent_name") or cticker
    active_flag = sec_meta.get("active_flag")
    inactive_reason = sec_meta.get("inactive_reason")

    # Header with active badge
    badge_col, title_col = st.columns([1, 6])
    with title_col:
        st.subheader(f"{cticker} — {sec_name}")
    with badge_col:
        if active_flag is True:
            st.success("ACTIVE")
        elif active_flag is False:
            reason_label = {
                "not_in_edgar_tickers": "Not in EDGAR",
                "no_recent_10k_10q": "Stale Filer",
            }.get(inactive_reason or "", inactive_reason or "Inactive")
            if (inactive_reason or "").startswith("deregistration"):
                reason_label = "Deregistered"
            st.error(f"INACTIVE\n{reason_label}")
        else:
            st.warning("STATUS\nUnknown")

    # ── Top KPIs ──────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ETFs Holding", f"{len(etf_rows):,}")
    c2.metric("Avg Weight", f"{etf_rows['weight'].mean():.4f}%")
    c3.metric("Max Weight", f"{etf_rows['weight'].max():.4f}%")
    c4.metric("Total AUM Exposed", f"${etf_rows['aum_bn'].sum():,.1f}B")

    st.divider()

    # ── Two-column metadata layout ─────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("##### Identity")
        def _v(key): return sec_meta.get(key) or "—"

        identity_data = {
            "Security Type":       _v("security_type"),
            "Asset Class":         _v("asset_class"),
            "Country of Exchange": _v("country_of_exchange"),
            "Exchange":            _v("exchange"),
            "Currency":            _v("currency_traded"),
            "ISIN":                _v("isin"),
            "CUSIP":               _v("cusip"),
        }
        st.table(pd.DataFrame({"Value": identity_data}))

    with col_right:
        st.markdown("##### SEC EDGAR")
        cik_val = sec_meta.get("edgar_cik")
        cik_display = (
            f"[{cik_val}](https://www.sec.gov/cgi-bin/browse-edgar"
            f"?action=getcompany&CIK={cik_val})"
            if cik_val else "—"
        )
        sic = sec_meta.get("sic_code")
        sic_desc = sec_meta.get("sic_description")
        sic_display = f"{sic} — {sic_desc}" if sic and sic_desc else (sic or "—")

        shares = sec_meta.get("shares_outstanding")
        shares_date = sec_meta.get("shares_as_of_date")
        if shares:
            if shares >= 1e9:
                shares_display = f"{shares/1e9:.2f}B"
            elif shares >= 1e6:
                shares_display = f"{shares/1e6:.1f}M"
            else:
                shares_display = f"{shares:,.0f}"
            if shares_date:
                shares_display += f" (as of {str(shares_date)[:10]})"
        else:
            shares_display = "—"

        edgar_data = {
            "CIK":               cik_display,
            "EDGAR Name":        _v("edgar_name"),
            "SIC":               sic_display,
            "State of Inc.":     _v("state_of_inc"),
            "Entity Type":       _v("entity_type"),
            "EIN":               _v("ein"),
            "Listed Exchanges":  _v("listed_exchanges"),
            "Filer Category":    _v("filer_category"),
            "Shares Outstanding":shares_display,
        }
        # Render as markdown table to support hyperlinks
        st.markdown("| Field | Value |")
        st.markdown("|-------|-------|")
        for field, val in edgar_data.items():
            st.markdown(f"| {field} | {val} |")

        enriched_at = sec_meta.get("edgar_enriched_at")
        if enriched_at:
            st.caption(f"EDGAR data as of: {str(enriched_at)[:19]}")

    # ── ETF exposure pie ───────────────────────────────────────────────────
    asset_counts = etf_rows["asset_class"].fillna("Unknown").value_counts().reset_index()
    asset_counts.columns = ["asset_class", "count"]
    fig = px.pie(asset_counts, values="count", names="asset_class",
                 title="ETFs Holding This Security — by Asset Class")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── ETFs holding this security ────────────────────────────────────────
    st.subheader(f"ETFs holding {cticker} (sorted by weight)")
    display = etf_rows[
        ["etf_ticker", "etf_name", "issuer", "asset_class", "category",
         "aum_bn", "net_expenses_bps", "weight", "market_value", "shares_held"]
    ].copy()
    display["etf_name"] = display["etf_name"].str[:45]
    display["market_value"] = display["market_value"].apply(
        lambda v: f"${v:,.0f}" if pd.notna(v) else "—"
    )
    display["shares_held"] = display["shares_held"].apply(
        lambda v: f"{v:,.0f}" if pd.notna(v) else "—"
    )
    display = display.rename(columns={
        "etf_ticker": "ETF", "etf_name": "Name", "issuer": "Issuer",
        "asset_class": "Asset Class", "category": "Category",
        "aum_bn": "AUM ($B)", "net_expenses_bps": "Exp. Ratio (bps)",
        "weight": "Weight (%)", "market_value": "Mkt Value ($)",
        "shares_held": "Shares Held",
    })
    st.dataframe(display, use_container_width=True, hide_index=True)

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        top_n = min(30, len(etf_rows))
        fig = px.bar(
            etf_rows.head(top_n),
            x="weight", y="etf_ticker", orientation="h",
            color="asset_class",
            hover_data=["etf_name", "aum_bn"],
            title=f"Top {top_n} ETF Weights for {cticker} (%)",
            labels={"weight": "Weight (%)", "etf_ticker": "ETF",
                    "asset_class": "Asset Class"},
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.histogram(
            etf_rows, x="weight", nbins=40,
            title=f"Weight Distribution of {cticker} Across ETFs",
            labels={"weight": "Weight (%)"},
        )
        st.plotly_chart(fig, use_container_width=True)

    # AUM-weighted exposure by issuer
    issuer_exp = (
        etf_rows[etf_rows["aum_bn"].notna()]
        .groupby("issuer")
        .agg(etf_count=("etf_ticker", "count"), total_aum_bn=("aum_bn", "sum"))
        .sort_values("total_aum_bn", ascending=False)
        .head(12).reset_index()
    )
    if not issuer_exp.empty:
        st.subheader(f"Issuer Exposure — ETFs holding {cticker}")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(issuer_exp, x="total_aum_bn", y="issuer", orientation="h",
                         color="total_aum_bn", color_continuous_scale="Blues",
                         title="Total AUM of ETFs holding security (by issuer)",
                         labels={"total_aum_bn": "AUM ($B)", "issuer": ""})
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(issuer_exp, x="etf_count", y="issuer", orientation="h",
                         color="etf_count", color_continuous_scale="Oranges",
                         title="Number of ETFs holding security (by issuer)",
                         labels={"etf_count": "ETF Count", "issuer": ""})
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
