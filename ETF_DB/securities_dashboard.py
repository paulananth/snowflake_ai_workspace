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
Securities Master Dashboard
Insights on the SECURITIES table including EDGAR enrichment results.
Run: uv run streamlit run ETF_DB/securities_dashboard.py
"""
import os
import pathlib
import tomllib

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector

st.set_page_config(
    page_title="Securities Master",
    layout="wide",
    page_icon="🔐",
)
st.title("🔐 Securities Master — Insights Dashboard")
st.caption("Source: `ETF_DB.LOCAL_COPY.SECURITIES` — 113,855 unique tickers enriched with SEC EDGAR data")

# ── Connection ──────────────────────────────────────────────────────────────
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

# ── Sidebar nav ─────────────────────────────────────────────────────────────
section = st.sidebar.radio("Section", [
    "📊 Overview",
    "🟢 Active vs Inactive",
    "🏭 Industry & SIC",
    "🌍 Geography & Exchange",
    "📈 Shares Outstanding",
    "🔎 Security Detail",
])

# ── Load data (cache whole table for in-memory filters) ─────────────────────
@st.cache_data(ttl=3600)
def load_securities():
    return q("SELECT * FROM ETF_DB.LOCAL_COPY.SECURITIES")

with st.spinner("Loading SECURITIES…"):
    sec = load_securities()

# Derived columns
sec["is_enriched"] = sec["edgar_enriched_at"].notna()
sec["has_cik"] = sec["edgar_cik"].notna()
sec["has_shares"] = sec["shares_outstanding"].notna()
sec["active_label"] = sec["active_flag"].map(
    {True: "Active", False: "Inactive"}
).fillna("Not enriched")

# ══════════════════════════════════════════════════════════════════════════════
if section == "📊 Overview":
    st.header("Universe Overview")

    total      = len(sec)
    enriched   = sec["is_enriched"].sum()
    with_cik   = sec["has_cik"].sum()
    with_shares= sec["has_shares"].sum()
    with_isin  = sec["isin"].notna().sum()
    with_cusip = sec["cusip"].notna().sum()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Securities", f"{total:,}")
    c2.metric("EDGAR Enriched", f"{enriched:,}", f"{enriched/total*100:.1f}%")
    c3.metric("With CIK", f"{with_cik:,}", f"{with_cik/total*100:.1f}%")
    c4.metric("With ISIN", f"{with_isin:,}", f"{with_isin/total*100:.1f}%")
    c5.metric("With CUSIP", f"{with_cusip:,}", f"{with_cusip/total*100:.1f}%")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Asset Class Breakdown")
        ac = sec["asset_class"].fillna("Unknown").value_counts().reset_index()
        ac.columns = ["Asset Class", "Count"]
        fig = px.bar(ac, x="Count", y="Asset Class", orientation="h",
                     color="Count", color_continuous_scale="Blues",
                     text="Count")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(showlegend=False, coloraxis_showscale=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Security Type Breakdown")
        st_df = sec["security_type"].fillna("Unknown").value_counts().head(12).reset_index()
        st_df.columns = ["Security Type", "Count"]
        fig = px.bar(st_df, x="Count", y="Security Type", orientation="h",
                     color="Count", color_continuous_scale="Purples", text="Count")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(showlegend=False, coloraxis_showscale=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("EDGAR Enrichment Status")
        enrich_df = pd.DataFrame({
            "Status": ["Enriched", "Not Enriched"],
            "Count": [enriched, total - enriched],
        })
        fig = px.pie(enrich_df, values="Count", names="Status",
                     color_discrete_map={"Enriched": "#2ecc71", "Not Enriched": "#e0e0e0"})
        fig.update_traces(textinfo="percent+label")
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Only US equities were targeted for enrichment.")

    with col2:
        st.subheader("Identifier Coverage")
        cov_data = pd.DataFrame({
            "Identifier": ["ISIN", "CUSIP", "EDGAR CIK", "Shares Outstanding"],
            "Have": [with_isin, with_cusip, with_cik, with_shares],
            "Missing": [total - with_isin, total - with_cusip,
                        total - with_cik, total - with_shares],
        })
        fig = px.bar(cov_data, x="Identifier", y=["Have", "Missing"],
                     barmode="stack",
                     color_discrete_map={"Have": "#3498db", "Missing": "#ecf0f1"})
        fig.update_layout(height=300, legend_title="")
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        st.subheader("Active Flag Status")
        af = sec["active_label"].value_counts().reset_index()
        af.columns = ["Status", "Count"]
        color_map = {"Active": "#2ecc71", "Inactive": "#e74c3c", "Not enriched": "#bdc3c7"}
        fig = px.pie(af, values="Count", names="Status",
                     color="Status", color_discrete_map=color_map)
        fig.update_traces(textinfo="percent+label")
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Missing Values Heatmap")
    cols_to_check = [
        "constituent_name", "asset_class", "security_type", "country_of_exchange",
        "exchange", "currency_traded", "isin", "cusip",
        "edgar_cik", "edgar_name", "sic_code", "state_of_inc",
        "entity_type", "filer_category", "active_flag", "shares_outstanding",
    ]
    missing_pct = (sec[cols_to_check].isna().mean() * 100).reset_index()
    missing_pct.columns = ["Column", "Missing %"]
    missing_pct = missing_pct.sort_values("Missing %", ascending=False)
    fig = px.bar(missing_pct, x="Missing %", y="Column", orientation="h",
                 color="Missing %", color_continuous_scale="RdYlGn_r",
                 text=missing_pct["Missing %"].map("{:.1f}%".format))
    fig.update_traces(textposition="outside")
    fig.update_layout(height=500, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
elif section == "🟢 Active vs Inactive":
    st.header("Active vs Inactive Securities")
    st.caption("Based on SEC EDGAR enrichment — US equities only")

    enriched_sec = sec[sec["is_enriched"]]
    active   = enriched_sec[enriched_sec["active_flag"] == True]
    inactive = enriched_sec[enriched_sec["active_flag"] == False]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Enriched (US Equity)", f"{len(enriched_sec):,}")
    c2.metric("Active", f"{len(active):,}", f"{len(active)/len(enriched_sec)*100:.1f}%")
    c3.metric("Inactive", f"{len(inactive):,}", f"{len(inactive)/len(enriched_sec)*100:.1f}%")
    c4.metric("With Shares Outstanding", f"{enriched_sec['has_shares'].sum():,}",
              f"{enriched_sec['has_shares'].mean()*100:.1f}%")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Inactive Reason Breakdown")
        reason_map = {
            "not_in_edgar_tickers": "Not in SEC EDGAR",
            "no_recent_10k_10q":    "Stale Filer (no 10-K/10-Q)",
        }
        def map_reason(r):
            if r is None:
                return "Unknown"
            if str(r).startswith("deregistration"):
                return "Formally Deregistered"
            return reason_map.get(r, r)

        inactive_copy = inactive.copy()
        inactive_copy["reason_label"] = inactive_copy["inactive_reason"].apply(map_reason)
        reason_df = inactive_copy["reason_label"].value_counts().reset_index()
        reason_df.columns = ["Reason", "Count"]
        fig = px.pie(reason_df, values="Count", names="Reason",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_traces(textinfo="percent+label+value")
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Active vs Inactive by Security Type")
        pivot = (
            enriched_sec[enriched_sec["security_type"].notna()]
            .groupby(["security_type", "active_label"])
            .size().reset_index(name="count")
        )
        top_types = pivot.groupby("security_type")["count"].sum().nlargest(8).index
        pivot = pivot[pivot["security_type"].isin(top_types)]
        fig = px.bar(pivot, x="count", y="security_type", color="active_label",
                     orientation="h", barmode="stack",
                     color_discrete_map={"Active": "#2ecc71", "Inactive": "#e74c3c"},
                     labels={"count": "Securities", "security_type": "Security Type",
                              "active_label": "Status"})
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Filer Category Distribution (Active Securities)")
    fc = active["filer_category"].fillna("Unknown").value_counts().reset_index()
    fc.columns = ["Filer Category", "Count"]
    fig = px.bar(fc, x="Filer Category", y="Count",
                 color="Count", color_continuous_scale="Blues", text="Count")
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=350)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Top 20 Inactive Securities (Deregistered)")
    deregistered = inactive[
        inactive["inactive_reason"].str.startswith("deregistration", na=False)
    ][["constituent_ticker", "constituent_name", "inactive_reason",
       "edgar_name", "sic_description", "state_of_inc"]].head(20)
    st.dataframe(deregistered.rename(columns={
        "constituent_ticker": "Ticker",
        "constituent_name": "Name",
        "inactive_reason": "Reason",
        "edgar_name": "EDGAR Name",
        "sic_description": "Industry",
        "state_of_inc": "State",
    }), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
elif section == "🏭 Industry & SIC":
    st.header("Industry & SIC Code Analysis")
    st.caption("SIC codes from SEC EDGAR — US equities only")

    enriched_sec = sec[sec["is_enriched"] & sec["sic_code"].notna()]

    c1, c2, c3 = st.columns(3)
    c1.metric("Securities with SIC Code", f"{len(enriched_sec):,}")
    c2.metric("Unique SIC Codes", f"{enriched_sec['sic_code'].nunique():,}")
    c3.metric("Unique SIC Descriptions", f"{enriched_sec['sic_description'].nunique():,}")

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 20 Industries by Security Count")
        sic_df = enriched_sec["sic_description"].value_counts().head(20).reset_index()
        sic_df.columns = ["Industry", "Securities"]
        fig = px.bar(sic_df, x="Securities", y="Industry", orientation="h",
                     color="Securities", color_continuous_scale="Teal", text="Securities")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=550, coloraxis_showscale=False,
                          yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top 20 Industries — Active Securities Only")
        active_sic = enriched_sec[enriched_sec["active_flag"] == True]
        sic_active = active_sic["sic_description"].value_counts().head(20).reset_index()
        sic_active.columns = ["Industry", "Securities"]
        fig = px.bar(sic_active, x="Securities", y="Industry", orientation="h",
                     color="Securities", color_continuous_scale="Mint", text="Securities")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=550, coloraxis_showscale=False,
                          yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("State of Incorporation Distribution")
    state_df = enriched_sec["state_of_inc"].fillna("Unknown").value_counts().head(15).reset_index()
    state_df.columns = ["State", "Count"]
    fig = px.bar(state_df, x="State", y="Count",
                 color="Count", color_continuous_scale="Blues", text="Count")
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=350)
    st.plotly_chart(fig, use_container_width=True)
    st.caption("DE (Delaware) dominates as the most common state of incorporation for US public companies.")

    st.divider()
    st.subheader("Entity Type Breakdown")
    et_df = enriched_sec["entity_type"].fillna("Unknown").value_counts().reset_index()
    et_df.columns = ["Entity Type", "Count"]
    fig = px.pie(et_df, values="Count", names="Entity Type",
                 color_discrete_sequence=px.colors.qualitative.Set3)
    fig.update_traces(textinfo="percent+label")
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
elif section == "🌍 Geography & Exchange":
    st.header("Geography & Exchange")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 20 Countries of Exchange")
        ctry = sec["country_of_exchange"].fillna("Unknown").value_counts().head(20).reset_index()
        ctry.columns = ["Country", "Securities"]
        fig = px.bar(ctry, x="Securities", y="Country", orientation="h",
                     color="Securities", color_continuous_scale="Blues", text="Securities")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=500, coloraxis_showscale=False,
                          yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Currency Distribution")
        curr = sec["currency_traded"].fillna("Unknown").value_counts().head(15).reset_index()
        curr.columns = ["Currency", "Count"]
        fig = px.bar(curr, x="Count", y="Currency", orientation="h",
                     color="Count", color_continuous_scale="Purples", text="Count")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=500, coloraxis_showscale=False,
                          yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Listed Exchanges (EDGAR — US Equities)")
    enriched_sec = sec[sec["is_enriched"] & sec["listed_exchanges"].notna()]
    # Flatten pipe-separated exchanges
    exchanges = (
        enriched_sec["listed_exchanges"]
        .str.split("|")
        .explode()
        .str.strip()
        .str.upper()
        .value_counts()
        .head(15)
        .reset_index()
    )
    exchanges.columns = ["Exchange", "Securities"]
    fig = px.bar(exchanges, x="Exchange", y="Securities",
                 color="Securities", color_continuous_scale="Teal", text="Securities")
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=350)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Non-US Securities by Country (top 25)")
        non_us = (
            sec[sec["country_of_exchange"].notna() & (sec["country_of_exchange"] != "US")]
            ["country_of_exchange"].value_counts().head(25).reset_index()
        )
        non_us.columns = ["Country", "Count"]
        fig = px.pie(non_us, values="Count", names="Country",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_traces(textinfo="label+percent")
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Securities per Asset Class × Country (top 5 countries)")
        top5 = sec["country_of_exchange"].value_counts().head(5).index.tolist()
        cross = (
            sec[sec["country_of_exchange"].isin(top5)]
            .groupby(["country_of_exchange", "asset_class"])
            .size().reset_index(name="count")
        )
        fig = px.bar(cross, x="country_of_exchange", y="count",
                     color="asset_class", barmode="stack",
                     labels={"country_of_exchange": "Country",
                              "count": "Securities", "asset_class": "Asset Class"})
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
elif section == "📈 Shares Outstanding":
    st.header("Shares Outstanding Analysis")
    st.caption("From SEC EDGAR XBRL filings — US equities only")

    shares_df = sec[sec["has_shares"] & sec["shares_outstanding"] > 0].copy()
    shares_df["shares_bn"] = shares_df["shares_outstanding"] / 1e9
    shares_df["shares_m"]  = shares_df["shares_outstanding"] / 1e6

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Securities with Shares", f"{len(shares_df):,}")
    c2.metric("Median Shares", f"{shares_df['shares_outstanding'].median()/1e6:.1f}M")
    c3.metric("Largest (shares)", f"{shares_df['shares_bn'].max():.1f}B")
    c4.metric("Smallest (shares)",
              f"{shares_df['shares_outstanding'].min():,.0f}")

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Shares Distribution (log scale)")
        fig = px.histogram(
            shares_df, x="shares_outstanding", nbins=60, log_x=True,
            labels={"shares_outstanding": "Shares Outstanding"},
            color_discrete_sequence=["#3498db"],
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("X-axis is log scale. Most companies cluster between 10M-500M shares.")

    with col2:
        st.subheader("Top 25 by Shares Outstanding")
        top25 = (
            shares_df[["constituent_ticker", "constituent_name",
                        "sic_description", "filer_category", "shares_outstanding",
                        "shares_as_of_date"]]
            .sort_values("shares_outstanding", ascending=False)
            .head(25)
        )
        top25["shares_outstanding"] = top25["shares_outstanding"].apply(
            lambda v: f"{v/1e9:.2f}B" if v >= 1e9 else f"{v/1e6:.0f}M"
        )
        top25["shares_as_of_date"] = top25["shares_as_of_date"].astype(str).str[:10]
        st.dataframe(top25.rename(columns={
            "constituent_ticker": "Ticker",
            "constituent_name": "Name",
            "sic_description": "Industry",
            "filer_category": "Filer",
            "shares_outstanding": "Shares",
            "shares_as_of_date": "As Of",
        }), use_container_width=True, hide_index=True, height=350)

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Shares Outstanding by Filer Category")
        fc_shares = (
            shares_df[shares_df["filer_category"].notna()]
            .groupby("filer_category")["shares_outstanding"]
            .agg(["count", "median"])
            .reset_index()
        )
        fc_shares.columns = ["Filer Category", "Count", "Median Shares"]
        fc_shares["Median (M)"] = fc_shares["Median Shares"] / 1e6
        fig = px.bar(fc_shares, x="Filer Category", y="Median (M)",
                     color="Count", text=fc_shares["Median (M)"].map("{:.0f}M".format),
                     color_continuous_scale="Blues",
                     labels={"Median (M)": "Median Shares (M)"})
        fig.update_layout(coloraxis_showscale=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Shares Data Recency")
        shares_df["shares_as_of_date"] = pd.to_datetime(shares_df["shares_as_of_date"])
        shares_df["year_month"] = shares_df["shares_as_of_date"].dt.to_period("M").astype(str)
        recency = shares_df["year_month"].value_counts().sort_index().reset_index()
        recency.columns = ["Month", "Count"]
        fig = px.bar(recency.tail(24), x="Month", y="Count",
                     color_discrete_sequence=["#2ecc71"],
                     labels={"Month": "Filing Month", "Count": "Securities"})
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Month the shares figure was reported in SEC filings.")


# ══════════════════════════════════════════════════════════════════════════════
elif section == "🔎 Security Detail":
    st.header("Security Detail Lookup")

    # ── Search ────────────────────────────────────────────────────────────
    ticker_options = sorted(sec["constituent_ticker"].dropna().unique().tolist())
    default_idx = ticker_options.index("AAPL") if "AAPL" in ticker_options else 0
    cticker = st.selectbox("Select or type a ticker", ticker_options, index=default_idx)

    row = sec[sec["constituent_ticker"] == cticker]
    if row.empty:
        st.warning(f"Ticker **{cticker}** not found.")
        st.stop()
    r = row.iloc[0]

    def _v(key): return r.get(key) if pd.notna(r.get(key)) else None
    def _d(key): return _v(key) or "—"

    # Header
    active_flag = _v("active_flag")
    col_title, col_badge = st.columns([5, 1])
    with col_title:
        st.subheader(f"{cticker} — {_d('constituent_name')}")
        tags = " · ".join(filter(None, [_d("asset_class"), _d("security_type"),
                                        _d("country_of_exchange")]))
        st.caption(tags)
    with col_badge:
        if active_flag is True:
            st.success("ACTIVE")
        elif active_flag is False:
            st.error("INACTIVE")
        else:
            st.info("NOT ENRICHED")

    st.divider()

    # Two-column detail layout
    left, right = st.columns(2)

    with left:
        st.markdown("#### Identity")
        identity = {
            "Security Type":       _d("security_type"),
            "Asset Class":         _d("asset_class"),
            "Country of Exchange": _d("country_of_exchange"),
            "Exchange":            _d("exchange"),
            "Currency":            _d("currency_traded"),
            "ISIN":                _d("isin"),
            "CUSIP":               _d("cusip"),
        }
        for k, v in identity.items():
            col_k, col_v = st.columns([2, 3])
            col_k.markdown(f"**{k}**")
            col_v.markdown(v)

    with right:
        st.markdown("#### SEC EDGAR")
        cik = _v("edgar_cik")
        cik_link = (
            f"[{cik}](https://www.sec.gov/cgi-bin/browse-edgar"
            f"?action=getcompany&CIK={cik})" if cik else "—"
        )
        sic = _v("sic_code")
        sic_desc = _v("sic_description")
        sic_str = f"{sic} — {sic_desc}" if sic and sic_desc else _d("sic_code")

        shares = _v("shares_outstanding")
        shares_date = _v("shares_as_of_date")
        if shares:
            if shares >= 1e9:
                shares_str = f"{shares/1e9:.3f}B shares"
            elif shares >= 1e6:
                shares_str = f"{shares/1e6:.2f}M shares"
            else:
                shares_str = f"{shares:,.0f} shares"
            if shares_date:
                shares_str += f" (as of {str(shares_date)[:10]})"
        else:
            shares_str = "—"

        edgar_fields = {
            "CIK":                cik_link,
            "EDGAR Name":         _d("edgar_name"),
            "SIC":                sic_str,
            "State of Inc.":      _d("state_of_inc"),
            "Entity Type":        _d("entity_type"),
            "EIN":                _d("ein"),
            "Listed Exchanges":   _d("listed_exchanges"),
            "Filer Category":     _d("filer_category"),
            "Shares Outstanding": shares_str,
            "Inactive Reason":    _d("inactive_reason"),
        }
        for k, v in edgar_fields.items():
            col_k, col_v = st.columns([2, 3])
            col_k.markdown(f"**{k}**")
            col_v.markdown(v)

        enriched_at = _v("edgar_enriched_at")
        if enriched_at:
            st.caption(f"EDGAR data fetched: {str(enriched_at)[:19]}")

    # ── ETFs holding this ticker ──────────────────────────────────────────
    st.divider()
    st.subheader(f"ETFs Holding {cticker}")
    with st.spinner("Querying holdings…"):
        etf_rows = q(f"""
            SELECT
                c.composite_ticker AS etf,
                i.description      AS etf_name,
                i.issuer,
                i.aum / 1e9        AS aum_bn,
                ROUND(c.weight, 4) AS weight,
                c.market_value,
                c.shares_held
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS c
            LEFT JOIN ETF_DB.LOCAL_COPY.INDUSTRY i
                ON c.composite_ticker = i.composite_ticker
                AND i.as_of_date = (
                    SELECT MAX(as_of_date) FROM ETF_DB.LOCAL_COPY.INDUSTRY
                )
            WHERE c.constituent_ticker = '{cticker}'
              AND c.as_of_date = (
                  SELECT as_of_date FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
                  GROUP BY as_of_date ORDER BY COUNT(*) DESC LIMIT 1
              )
            ORDER BY c.weight DESC
        """)

    if etf_rows.empty:
        st.info(f"No ETF holdings found for **{cticker}** in the latest snapshot.")
    else:
        m1, m2, m3 = st.columns(3)
        m1.metric("ETFs Holding", f"{len(etf_rows):,}")
        m2.metric("Max Weight", f"{etf_rows['weight'].max():.4f}%")
        m3.metric("Total AUM Exposed", f"${etf_rows['aum_bn'].sum():,.1f}B")

        etf_rows["market_value"] = etf_rows["market_value"].apply(
            lambda v: f"${v:,.0f}" if pd.notna(v) else "—"
        )
        etf_rows["shares_held"] = etf_rows["shares_held"].apply(
            lambda v: f"{v:,.0f}" if pd.notna(v) else "—"
        )
        etf_rows["aum_bn"] = etf_rows["aum_bn"].apply(
            lambda v: f"${v:.2f}B" if pd.notna(v) else "—"
        )
        st.dataframe(etf_rows.rename(columns={
            "etf": "ETF", "etf_name": "ETF Name", "issuer": "Issuer",
            "aum_bn": "AUM", "weight": "Weight (%)",
            "market_value": "Mkt Value", "shares_held": "Shares Held",
        }), use_container_width=True, hide_index=True)
