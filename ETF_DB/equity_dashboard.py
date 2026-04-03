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
Equity Securities Dashboard
Analysis of ASSET_CLASS = 'Equity' securities only.
Run: uv run --with streamlit --with pandas --with plotly --with snowflake-connector-python python -m streamlit run ETF_DB/equity_dashboard.py
"""
import pathlib
import tomllib

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector

# ── ISO-2 to ISO-3 country code mapping ────────────────────────────────────
ISO2_ISO3 = {
    "AF":"AFG","AL":"ALB","DZ":"DZA","AO":"AGO","AR":"ARG","AM":"ARM",
    "AU":"AUS","AT":"AUT","AZ":"AZE","BH":"BHR","BD":"BGD","BY":"BLR",
    "BE":"BEL","BZ":"BLZ","BJ":"BEN","BM":"BMU","BT":"BTN","BO":"BOL",
    "BA":"BIH","BW":"BWA","BR":"BRA","BN":"BRN","BG":"BGR","BF":"BFA",
    "KH":"KHM","CM":"CMR","CA":"CAN","CV":"CPV","CL":"CHL","CN":"CHN",
    "CO":"COL","CR":"CRI","HR":"HRV","CY":"CYP","CZ":"CZE","DK":"DNK",
    "DO":"DOM","EC":"ECU","EG":"EGY","SV":"SLV","EE":"EST","ET":"ETH",
    "FI":"FIN","FR":"FRA","GE":"GEO","DE":"DEU","GH":"GHA","GR":"GRC",
    "GT":"GTM","HN":"HND","HK":"HKG","HU":"HUN","IS":"ISL","IN":"IND",
    "ID":"IDN","IR":"IRN","IQ":"IRQ","IE":"IRL","IL":"ISR","IT":"ITA",
    "JM":"JAM","JP":"JPN","JO":"JOR","KZ":"KAZ","KE":"KEN","KW":"KWT",
    "KG":"KGZ","LA":"LAO","LV":"LVA","LB":"LBN","LY":"LBY","LT":"LTU",
    "LU":"LUX","MO":"MAC","MK":"MKD","MG":"MDG","MW":"MWI","MY":"MYS",
    "MV":"MDV","ML":"MLI","MT":"MLT","MR":"MRT","MU":"MUS","MX":"MEX",
    "MD":"MDA","MN":"MNG","MA":"MAR","MZ":"MOZ","MM":"MMR","NA":"NAM",
    "NP":"NPL","NL":"NLD","NZ":"NZL","NI":"NIC","NG":"NGA","NO":"NOR",
    "OM":"OMN","PK":"PAK","PA":"PAN","PG":"PNG","PY":"PRY","PE":"PER",
    "PH":"PHL","PL":"POL","PT":"PRT","PR":"PRI","QA":"QAT","RO":"ROU",
    "RU":"RUS","RW":"RWA","SA":"SAU","SN":"SEN","RS":"SRB","SL":"SLE",
    "SG":"SGP","SK":"SVK","SI":"SVN","ZA":"ZAF","KR":"KOR","ES":"ESP",
    "LK":"LKA","SE":"SWE","CH":"CHE","TW":"TWN","TZ":"TZA","TH":"THA",
    "TT":"TTO","TN":"TUN","TR":"TUR","UG":"UGA","UA":"UKR","AE":"ARE",
    "GB":"GBR","US":"USA","UY":"URY","UZ":"UZB","VE":"VEN","VN":"VNM",
    "YE":"YEM","ZM":"ZMB","ZW":"ZWE","CW":"CUW","KY":"CYM","VG":"VGB",
    "TC":"TCA","BS":"BHS","BB":"BRB","GI":"GIB","JE":"JEY","GG":"GGY",
    "IM":"IMN","MC":"MCO","LI":"LIE","SM":"SMR","FO":"FRO","GL":"GRL",
    "MF":"MAF","CX":"CXR","CC":"CCK","NF":"NFK","BV":"BVT","HM":"HMD",
    "TF":"ATF","GS":"SGS","AQ":"ATA","CI":"CIV","SZ":"SWZ","PS":"PSE",
}

st.set_page_config(
    page_title="Equity Securities",
    layout="wide",
    page_icon="📈",
)
st.title("📈 Equity Securities Dashboard")
st.caption(
    "Source: `ETF_DB.LOCAL_COPY.SECURITIES` — filtered to `ASSET_CLASS = 'Equity'` only · "
    "20,279 equities across 60 countries"
)

# ── Connection ───────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    config_path = pathlib.Path.home() / ".snowflake" / "config.toml"
    with open(config_path, "rb") as f:
        cfg = tomllib.load(f)["connections"]["myfirstsnow"]
    return snowflake.connector.connect(
        account=cfg["account"], user=cfg["user"], password=cfg["password"],
        role=cfg.get("role", "ACCOUNTADMIN"), warehouse="cortex_analyst_wh",
    )

@st.cache_data(ttl=3600)
def q(sql):
    df = pd.read_sql(sql, get_conn())
    df.columns = df.columns.str.lower()
    return df

# ── Load equity securities ───────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_equities():
    df = q("SELECT * FROM ETF_DB.LOCAL_COPY.SECURITIES WHERE asset_class = 'Equity'")
    df["is_enriched"]  = df["edgar_enriched_at"].notna()
    df["has_cik"]      = df["edgar_cik"].notna()
    df["has_shares"]   = df["shares_outstanding"].notna()
    df["iso3"]         = df["country_of_exchange"].map(ISO2_ISO3)
    df["active_label"] = df["active_flag"].map(
        {True: "Active", False: "Inactive"}
    ).fillna("Not Enriched")
    return df

@st.cache_data(ttl=3600)
def best_snapshot_date():
    return str(
        q("SELECT as_of_date FROM ETF_DB.LOCAL_COPY.CONSTITUENTS "
          "GROUP BY as_of_date ORDER BY COUNT(*) DESC LIMIT 1")
        ["as_of_date"].iloc[0]
    )[:10]

with st.spinner("Loading equities…"):
    eq  = load_equities()
    SNAP = best_snapshot_date()

# ── Sidebar ──────────────────────────────────────────────────────────────────
section = st.sidebar.radio("Section", [
    "📊 Overview",
    "🗺️ Global Map",
    "🇺🇸 US Equities",
    "🌏 Non-US Equities",
    "📦 ETF Exposure",
    "🔎 Equity Lookup",
])

st.sidebar.divider()
st.sidebar.caption(f"Snapshot: **{SNAP}**")
st.sidebar.caption(f"Total equities: **{len(eq):,}**")

# ── Colour palette ───────────────────────────────────────────────────────────
ACTIVE_COLOR   = "#2ecc71"
INACTIVE_COLOR = "#e74c3c"
NEUTRAL_COLOR  = "#bdc3c7"
STATUS_MAP     = {"Active": ACTIVE_COLOR, "Inactive": INACTIVE_COLOR,
                  "Not Enriched": NEUTRAL_COLOR}

# ═══════════════════════════════════════════════════════════════════════════════
if section == "📊 Overview":
    st.header("Equity Universe — Overview")

    total     = len(eq)
    enriched  = eq["is_enriched"].sum()
    active    = (eq["active_flag"] == True).sum()
    inactive  = (eq["active_flag"] == False).sum()
    countries = eq["country_of_exchange"].nunique()
    w_shares  = eq["has_shares"].sum()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Equities",  f"{total:,}")
    c2.metric("Countries",       f"{countries}")
    c3.metric("EDGAR Enriched",  f"{enriched:,}", f"{enriched/total*100:.1f}%")
    c4.metric("Active",          f"{active:,}",   f"{active/total*100:.1f}%")
    c5.metric("Inactive",        f"{inactive:,}",  f"{inactive/total*100:.1f}%")
    c6.metric("With Shares",     f"{w_shares:,}",  f"{w_shares/total*100:.1f}%")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Security Type Breakdown")
        st_counts = (
            eq["security_type"]
            .fillna("Unknown")
            .replace("", "Unknown")
            .value_counts()
            .reset_index()
        )
        st_counts.columns = ["Type", "Count"]
        fig = px.bar(
            st_counts, x="Count", y="Type", orientation="h",
            color="Count", color_continuous_scale="Blues", text="Count",
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(
            coloraxis_showscale=False, height=500,
            yaxis={"categoryorder": "total ascending"},
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Active / Inactive / Not Enriched")
        status_df = eq["active_label"].value_counts().reset_index()
        status_df.columns = ["Status", "Count"]
        fig = px.pie(
            status_df, values="Count", names="Status",
            color="Status", color_discrete_map=STATUS_MAP,
        )
        fig.update_traces(textinfo="percent+label+value", pull=[0.03, 0.03, 0])
        fig.update_layout(height=320)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Identifier Coverage")
        cov = pd.DataFrame({
            "Identifier": ["ISIN", "CUSIP", "EDGAR CIK", "Shares"],
            "Have":    [eq["isin"].notna().sum(), eq["cusip"].notna().sum(),
                        eq["has_cik"].sum(),       w_shares],
            "Missing": [total - eq["isin"].notna().sum(),
                        total - eq["cusip"].notna().sum(),
                        total - eq["has_cik"].sum(),
                        total - w_shares],
        })
        fig = px.bar(
            cov, x="Identifier", y=["Have", "Missing"],
            barmode="stack",
            color_discrete_map={"Have": "#3498db", "Missing": "#ecf0f1"},
        )
        fig.update_layout(legend_title="", height=260)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Missing Values by Field")
    check_cols = [
        "constituent_name", "security_type", "country_of_exchange",
        "exchange", "currency_traded", "isin", "cusip",
        "edgar_cik", "sic_code", "state_of_inc",
        "filer_category", "active_flag", "shares_outstanding",
    ]
    miss = (eq[check_cols].isna().mean() * 100).reset_index()
    miss.columns = ["Field", "Missing %"]
    miss = miss.sort_values("Missing %", ascending=False)
    fig = px.bar(
        miss, x="Missing %", y="Field", orientation="h",
        color="Missing %", color_continuous_scale="RdYlGn_r",
        text=miss["Missing %"].map("{:.1f}%".format),
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(height=450, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
elif section == "🗺️ Global Map":
    st.header("Global Distribution of Equity Securities")

    # ── Sidebar filters ────────────────────────────────────────────────────
    st.sidebar.subheader("Filters")
    sec_types = ["All"] + sorted(
        eq["security_type"].dropna().replace("", "Unknown").unique().tolist()
    )
    sel_type = st.sidebar.selectbox("Security Type", sec_types)
    status_filter = st.sidebar.multiselect(
        "Status", ["Active", "Inactive", "Not Enriched"],
        default=["Active", "Inactive", "Not Enriched"],
    )

    filtered = eq.copy()
    if sel_type != "All":
        filtered = filtered[filtered["security_type"] == sel_type]
    if status_filter:
        filtered = filtered[filtered["active_label"].isin(status_filter)]

    # ── Country aggregation ────────────────────────────────────────────────
    country_agg = (
        filtered[filtered["iso3"].notna()]
        .groupby(["country_of_exchange", "iso3"])
        .agg(
            securities=("constituent_ticker", "count"),
            active=("active_flag", lambda x: (x == True).sum()),
            inactive=("active_flag", lambda x: (x == False).sum()),
        )
        .reset_index()
    )
    country_agg["active_pct"] = (
        country_agg["active"] / country_agg["securities"] * 100
    ).round(1)

    st.subheader("Equity Count by Country")

    map_metric = st.radio(
        "Colour map by", ["Securities Count", "Active Count", "Active %"],
        horizontal=True,
    )
    metric_col = {
        "Securities Count": "securities",
        "Active Count":     "active",
        "Active %":         "active_pct",
    }[map_metric]
    color_scale = {
        "securities": "Blues",
        "active":     "Greens",
        "active_pct": "RdYlGn",
    }[metric_col]

    fig_map = px.choropleth(
        country_agg,
        locations="iso3",
        locationmode="ISO-3",
        color=metric_col,
        hover_name="country_of_exchange",
        hover_data={
            "iso3": False,
            "securities": True,
            "active": True,
            "inactive": True,
            "active_pct": ":.1f",
        },
        color_continuous_scale=color_scale,
        projection="natural earth",
        labels={
            "securities": "Securities",
            "active":     "Active",
            "inactive":   "Inactive",
            "active_pct": "Active %",
            "country_of_exchange": "Country",
        },
    )
    fig_map.update_layout(
        geo=dict(showframe=False, showcoastlines=True,
                 coastlinecolor="lightgray", showland=True,
                 landcolor="whitesmoke", showocean=True,
                 oceancolor="aliceblue"),
        margin={"r": 0, "t": 10, "l": 0, "b": 0},
        height=480,
        coloraxis_colorbar=dict(title=map_metric),
    )
    st.plotly_chart(fig_map, use_container_width=True)

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 25 Countries by Equity Count")
        top25 = country_agg.nlargest(25, "securities")
        fig = px.bar(
            top25.sort_values("securities"),
            x="securities", y="country_of_exchange", orientation="h",
            color="active_pct",
            color_continuous_scale="RdYlGn",
            text="securities",
            labels={"securities": "Equities",
                    "country_of_exchange": "Country",
                    "active_pct": "Active %"},
            hover_data=["active", "inactive", "active_pct"],
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(
            height=620, coloraxis_colorbar=dict(title="Active %"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Security Type Mix by Country (top 12)")
        top12_countries = country_agg.nlargest(12, "securities")[
            "country_of_exchange"
        ].tolist()
        type_cross = (
            filtered[filtered["country_of_exchange"].isin(top12_countries)]
            .groupby(["country_of_exchange", "security_type"])
            .size()
            .reset_index(name="count")
        )
        type_cross["security_type"] = (
            type_cross["security_type"].fillna("Unknown").replace("", "Unknown")
        )
        fig = px.bar(
            type_cross,
            x="count", y="country_of_exchange", color="security_type",
            orientation="h", barmode="stack",
            labels={"count": "Securities",
                    "country_of_exchange": "Country",
                    "security_type": "Type"},
        )
        fig.update_layout(height=620, legend_title="Security Type")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Bubble map — active equities only ─────────────────────────────────
    st.subheader("Active Equities — Bubble Map")

    # Add lat/lon centroids for bubble map (approximate country centroids)
    CENTROIDS = {
        "US":(-95,38),"CN":(105,35),"JP":(138,36),"IN":(78,22),"KR":(128,37),
        "HK":(114,22),"TH":(101,15),"CA":(-95,56),"TW":(121,24),"GB":(-2,54),
        "AU":(133,-27),"DE":(10,51),"SE":(18,60),"MY":(112,3),"BR":(-52,-14),
        "TR":(35,39),"ID":(120,-2),"FR":(2,47),"CH":(8,47),"IL":(35,31),
        "NO":(10,62),"FI":(26,64),"DK":(10,56),"NL":(5,52),"BE":(4,51),
        "AT":(14,47),"ES":((-4),40),"IT":(12,42),"PT":(-8,39),"GR":(22,39),
        "PL":(20,52),"CZ":(15,50),"HU":(19,47),"RO":(25,46),"RU":(100,60),
        "ZA":(25,-29),"NG":(8,10),"EG":(30,27),"SG":(104,1),"PH":(122,13),
        "VN":(108,16),"PK":(70,30),"BD":(90,24),"LK":(81,8),"MX":(-102,24),
        "AR":(-65,-34),"CL":(-71,-30),"CO":(-74,4),"PE":(-76,-10),
        "NZ":(172,-42),"SA":(45,24),"AE":(54,24),"QA":(51,25),"KW":(48,29),
        "BH":(50,26),"OM":(57,22),"JO":(37,31),"KZ":(67,48),"MA":((-6),32),
        "TN":(9,34),"KE":(37,-1),"GH":(-2,8),"LU":(6,50),"IE":(-8,53),
        "PT":(-8,39),"MN":(105,46),"MM":(96,17),"KH":(105,12),"LK":(81,7),
    }

    active_eq = filtered[
        (filtered["active_flag"] == True) & (filtered["country_of_exchange"].notna())
    ]
    bubble_agg = active_eq.groupby("country_of_exchange").size().reset_index(name="count")
    bubble_agg["lon"] = bubble_agg["country_of_exchange"].map(
        lambda c: CENTROIDS.get(c, (None, None))[0]
    )
    bubble_agg["lat"] = bubble_agg["country_of_exchange"].map(
        lambda c: CENTROIDS.get(c, (None, None))[1]
    )
    bubble_agg = bubble_agg.dropna(subset=["lon", "lat"])

    fig_bubble = px.scatter_geo(
        bubble_agg,
        lon="lon", lat="lat",
        size="count",
        hover_name="country_of_exchange",
        hover_data={"count": True, "lon": False, "lat": False},
        size_max=60,
        color="count",
        color_continuous_scale="Teal",
        projection="natural earth",
        labels={"count": "Active Equities"},
    )
    fig_bubble.update_layout(
        geo=dict(showframe=False, showcoastlines=True,
                 coastlinecolor="lightgray", showland=True,
                 landcolor="whitesmoke", showocean=True,
                 oceancolor="aliceblue"),
        margin={"r": 0, "t": 10, "l": 0, "b": 0},
        height=430,
        coloraxis_colorbar=dict(title="Active Equities"),
    )
    st.plotly_chart(fig_bubble, use_container_width=True)

    # ── Country data table ─────────────────────────────────────────────────
    st.divider()
    st.subheader("Country Summary Table")
    display = country_agg.sort_values("securities", ascending=False).copy()
    display["active_pct"] = display["active_pct"].map("{:.1f}%".format)
    st.dataframe(
        display.rename(columns={
            "country_of_exchange": "Country (ISO-2)",
            "iso3": "ISO-3",
            "securities": "Total",
            "active": "Active",
            "inactive": "Inactive",
            "active_pct": "Active %",
        }).drop(columns=["iso3"]),
        use_container_width=True, hide_index=True, height=400,
    )


# ═══════════════════════════════════════════════════════════════════════════════
elif section == "🇺🇸 US Equities":
    st.header("US Equities — Deep Dive")
    st.caption("Only securities with `COUNTRY_OF_EXCHANGE = 'US'`")

    us = eq[eq["country_of_exchange"] == "US"].copy()
    enriched_us = us[us["is_enriched"]]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("US Equities",      f"{len(us):,}")
    c2.metric("EDGAR Enriched",   f"{len(enriched_us):,}",
              f"{len(enriched_us)/len(us)*100:.1f}%")
    c3.metric("Active",           f"{(us['active_flag']==True).sum():,}")
    c4.metric("Inactive",         f"{(us['active_flag']==False).sum():,}")
    c5.metric("With Shares",      f"{us['has_shares'].sum():,}",
              f"{us['has_shares'].mean()*100:.1f}%")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 20 Industries (SIC Description)")
        sic_df = (
            enriched_us["sic_description"]
            .fillna("Unknown")
            .value_counts()
            .head(20)
            .reset_index()
        )
        sic_df.columns = ["Industry", "Count"]
        fig = px.bar(
            sic_df, x="Count", y="Industry", orientation="h",
            color="Count", color_continuous_scale="Teal", text="Count",
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(
            height=560, coloraxis_showscale=False,
            yaxis={"categoryorder": "total ascending"},
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("State of Incorporation")
        state_df = (
            enriched_us["state_of_inc"]
            .fillna("Unknown")
            .value_counts()
            .head(15)
            .reset_index()
        )
        state_df.columns = ["State", "Count"]
        fig = px.bar(
            state_df, x="State", y="Count",
            color="Count", color_continuous_scale="Blues", text="Count",
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=300, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Filer Category")
        fc_df = (
            enriched_us["filer_category"]
            .fillna("Unknown")
            .value_counts()
            .reset_index()
        )
        fc_df.columns = ["Filer Category", "Count"]
        fig = px.pie(
            fc_df, values="Count", names="Filer Category",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_traces(textinfo="percent+label")
        fig.update_layout(height=260, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Inactive reason map (US geography by state of inc) ─────────────────
    st.subheader("Active vs Inactive by Industry")
    top15_sic = (
        enriched_us["sic_description"]
        .fillna("Unknown")
        .value_counts()
        .head(15)
        .index.tolist()
    )
    sic_status = (
        enriched_us[enriched_us["sic_description"].isin(top15_sic)]
        .groupby(["sic_description", "active_label"])
        .size()
        .reset_index(name="count")
    )
    fig = px.bar(
        sic_status, x="count", y="sic_description", color="active_label",
        orientation="h", barmode="stack",
        color_discrete_map=STATUS_MAP,
        labels={"count": "Securities", "sic_description": "Industry",
                "active_label": "Status"},
    )
    fig.update_layout(height=450, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Shares outstanding ─────────────────────────────────────────────────
    st.subheader("Shares Outstanding Distribution (US Active Equities)")
    shares_us = us[us["has_shares"] & (us["active_flag"] == True)].copy()
    shares_us["shares_bn"] = shares_us["shares_outstanding"] / 1e9

    col1, col2 = st.columns(2)
    with col1:
        fig = px.histogram(
            shares_us, x="shares_outstanding", nbins=60, log_x=True,
            color_discrete_sequence=["#3498db"],
            labels={"shares_outstanding": "Shares Outstanding"},
            title="Distribution (log scale)",
        )
        fig.update_layout(height=320)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        top_shares = (
            shares_us[["constituent_ticker", "constituent_name",
                        "sic_description", "shares_outstanding", "shares_as_of_date"]]
            .sort_values("shares_outstanding", ascending=False)
            .head(20)
        )
        top_shares["shares_outstanding"] = top_shares["shares_outstanding"].apply(
            lambda v: f"{v/1e9:.2f}B" if v >= 1e9 else f"{v/1e6:.0f}M"
        )
        top_shares["shares_as_of_date"] = (
            top_shares["shares_as_of_date"].astype(str).str[:10]
        )
        st.markdown("**Top 20 by Shares Outstanding**")
        st.dataframe(
            top_shares.rename(columns={
                "constituent_ticker": "Ticker",
                "constituent_name": "Name",
                "sic_description": "Industry",
                "shares_outstanding": "Shares",
                "shares_as_of_date": "As Of",
            }),
            use_container_width=True, hide_index=True, height=320,
        )


# ═══════════════════════════════════════════════════════════════════════════════
elif section == "🌏 Non-US Equities":
    st.header("Non-US Equities")

    non_us = eq[eq["country_of_exchange"] != "US"].copy()
    non_us = non_us[non_us["country_of_exchange"].notna()]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Non-US Equities",     f"{len(non_us):,}")
    c2.metric("Countries",           f"{non_us['country_of_exchange'].nunique()}")
    c3.metric("Active",              f"{(non_us['active_flag']==True).sum():,}")
    c4.metric("Most Common Country",
              non_us["country_of_exchange"].value_counts().index[0])

    st.divider()

    # ── Non-US choropleth ─────────────────────────────────────────────────
    st.subheader("Non-US Equity Count by Country")
    non_us_agg = (
        non_us[non_us["iso3"].notna()]
        .groupby(["country_of_exchange", "iso3"])
        .agg(
            securities=("constituent_ticker", "count"),
            active=("active_flag", lambda x: (x == True).sum()),
        )
        .reset_index()
    )
    non_us_agg["active_pct"] = (
        non_us_agg["active"] / non_us_agg["securities"] * 100
    ).round(1)

    map_col = st.radio(
        "Colour by", ["securities", "active", "active_pct"],
        format_func=lambda x: {"securities": "Total Count",
                                "active": "Active Count",
                                "active_pct": "Active %"}[x],
        horizontal=True,
    )

    fig_map = px.choropleth(
        non_us_agg,
        locations="iso3",
        locationmode="ISO-3",
        color=map_col,
        hover_name="country_of_exchange",
        hover_data={
            "iso3": False,
            "securities": True,
            "active": True,
            "active_pct": ":.1f",
        },
        color_continuous_scale="Viridis" if map_col != "active_pct" else "RdYlGn",
        projection="natural earth",
        labels={"securities": "Equities", "active": "Active",
                "active_pct": "Active %", "country_of_exchange": "Country"},
    )
    fig_map.update_layout(
        geo=dict(showframe=False, showcoastlines=True,
                 coastlinecolor="lightgray", showland=True,
                 landcolor="whitesmoke", showocean=True, oceancolor="aliceblue"),
        margin={"r": 0, "t": 10, "l": 0, "b": 0},
        height=460,
    )
    st.plotly_chart(fig_map, use_container_width=True)

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 20 Non-US Countries")
        top20 = non_us_agg.nlargest(20, "securities").sort_values("securities")
        fig = px.bar(
            top20, x="securities", y="country_of_exchange", orientation="h",
            color="active_pct", color_continuous_scale="RdYlGn",
            text="securities",
            labels={"securities": "Equities",
                    "country_of_exchange": "Country",
                    "active_pct": "Active %"},
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(
            height=520,
            coloraxis_colorbar=dict(title="Active %"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Security Type by Region")
        # Map countries to rough regions
        REGIONS = {
            "CN":"Asia","JP":"Asia","KR":"Asia","HK":"Asia","TW":"Asia",
            "TH":"Asia","MY":"Asia","IN":"Asia","ID":"Asia","PH":"Asia",
            "VN":"Asia","SG":"Asia","BD":"Asia","PK":"Asia","LK":"Asia",
            "MN":"Asia","KH":"Asia","MM":"Asia",
            "GB":"Europe","DE":"Europe","FR":"Europe","SE":"Europe",
            "CH":"Europe","NL":"Europe","NO":"Europe","DK":"Europe",
            "FI":"Europe","BE":"Europe","AT":"Europe","ES":"Europe",
            "IT":"Europe","PT":"Europe","GR":"Europe","PL":"Europe",
            "CZ":"Europe","HU":"Europe","RO":"Europe","LU":"Europe",
            "IE":"Europe","RU":"Europe",
            "CA":"Americas","BR":"Americas","MX":"Americas","AR":"Americas",
            "CL":"Americas","CO":"Americas","PE":"Americas",
            "AU":"Oceania","NZ":"Oceania",
            "ZA":"Africa","NG":"Africa","EG":"Africa","KE":"Africa",
            "GH":"Africa","MA":"Africa","TN":"Africa",
            "SA":"Middle East","AE":"Middle East","IL":"Middle East",
            "TR":"Middle East","QA":"Middle East","KW":"Middle East",
            "BH":"Middle East","OM":"Middle East","JO":"Middle East",
        }
        non_us["region"] = non_us["country_of_exchange"].map(REGIONS).fillna("Other")
        region_type = (
            non_us.groupby(["region", "security_type"])
            .size().reset_index(name="count")
        )
        region_type["security_type"] = (
            region_type["security_type"].fillna("Unknown").replace("", "Unknown")
        )
        fig = px.bar(
            region_type, x="region", y="count", color="security_type",
            barmode="stack",
            labels={"region": "Region", "count": "Securities",
                    "security_type": "Type"},
        )
        fig.update_layout(height=520, legend_title="Security Type")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("Currency Distribution (Non-US Equities)")
    curr_df = (
        non_us["currency_traded"]
        .fillna("Unknown")
        .value_counts()
        .head(15)
        .reset_index()
    )
    curr_df.columns = ["Currency", "Count"]
    fig = px.pie(
        curr_df, values="Count", names="Currency",
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_traces(textinfo="label+percent")
    fig.update_layout(height=350, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
elif section == "📦 ETF Exposure":
    st.header("Equity ETF Exposure")
    st.caption(f"Based on CONSTITUENTS snapshot — {SNAP}")

    with st.spinner("Loading ETF exposure data…"):
        etf_exp = q(f"""
            SELECT
                c.constituent_ticker,
                s.constituent_name,
                s.security_type,
                s.country_of_exchange,
                s.sic_description,
                s.active_flag,
                COUNT(DISTINCT c.composite_ticker) AS etf_count,
                ROUND(MAX(c.weight), 4)             AS max_weight,
                ROUND(AVG(c.weight), 4)             AS avg_weight,
                ROUND(SUM(c.weight), 4)             AS total_weight,
                SUM(c.market_value)                 AS total_market_value
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS c
            JOIN ETF_DB.LOCAL_COPY.SECURITIES s
                ON c.constituent_ticker = s.constituent_ticker
            WHERE s.asset_class = 'Equity'
              AND c.as_of_date = '{SNAP}'
            GROUP BY 1,2,3,4,5,6
            ORDER BY etf_count DESC
        """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique Equities in ETFs", f"{len(etf_exp):,}")
    c2.metric("Max ETF Membership",
              f"{etf_exp['etf_count'].max():,} ETFs",
              etf_exp.loc[etf_exp['etf_count'].idxmax(), 'constituent_ticker'])
    c3.metric("Avg ETF Count per Security", f"{etf_exp['etf_count'].mean():.1f}")
    c4.metric("Median ETF Count", f"{etf_exp['etf_count'].median():.0f}")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 30 by ETF Membership Count")
        top30 = etf_exp.head(30).sort_values("etf_count")
        top30["active_label"] = top30["active_flag"].map(
            {True: "Active", False: "Inactive"}
        ).fillna("Not Enriched")
        fig = px.bar(
            top30, x="etf_count", y="constituent_ticker",
            orientation="h", color="active_label",
            color_discrete_map=STATUS_MAP,
            text="etf_count",
            hover_data=["constituent_name", "max_weight", "avg_weight"],
            labels={"etf_count": "# ETFs", "constituent_ticker": "Ticker",
                    "active_label": "Status"},
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=680, legend_title="Status")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top 30 by Max Weight in Any ETF")
        top_wt = etf_exp.nlargest(30, "max_weight").sort_values("max_weight")
        fig = px.bar(
            top_wt, x="max_weight", y="constituent_ticker", orientation="h",
            color="max_weight", color_continuous_scale="Reds", text="max_weight",
            hover_data=["constituent_name", "etf_count"],
            labels={"max_weight": "Max Weight (%)",
                    "constituent_ticker": "Ticker"},
        )
        fig.update_traces(
            texttemplate="%{text:.3f}%", textposition="outside"
        )
        fig.update_layout(height=680, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("ETF Membership Distribution")
        fig = px.histogram(
            etf_exp, x="etf_count", nbins=50,
            color_discrete_sequence=["#9b59b6"],
            labels={"etf_count": "# ETFs Holding Security"},
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Most equities appear in fewer than 20 ETFs; mega-caps appear in 400+.")

    with col2:
        st.subheader("ETF Count by Country (top 15)")
        country_exp = (
            etf_exp[etf_exp["country_of_exchange"].notna()]
            .groupby("country_of_exchange")["etf_count"]
            .agg(["sum", "mean", "count"])
            .reset_index()
        )
        country_exp.columns = ["Country", "Total ETF Slots", "Avg ETFs/Security", "Securities"]
        top15_c = country_exp.nlargest(15, "Total ETF Slots")
        fig = px.bar(
            top15_c.sort_values("Total ETF Slots"),
            x="Total ETF Slots", y="Country", orientation="h",
            color="Avg ETFs/Security", color_continuous_scale="Blues",
            text="Securities",
            hover_data=["Avg ETFs/Security"],
        )
        fig.update_traces(texttemplate="(%{text:,} secs)", textposition="outside")
        fig.update_layout(height=300, coloraxis_colorbar=dict(title="Avg ETFs"))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("Full Equity Exposure Table")
    disp = etf_exp.copy()
    disp["total_market_value"] = disp["total_market_value"].apply(
        lambda v: f"${v/1e9:.2f}B" if pd.notna(v) and v >= 1e9
        else (f"${v/1e6:.0f}M" if pd.notna(v) and v >= 1e6 else "—")
    )
    disp["active_flag"] = disp["active_flag"].map(
        {True: "Active", False: "Inactive"}
    ).fillna("—")
    st.dataframe(
        disp.rename(columns={
            "constituent_ticker": "Ticker",
            "constituent_name": "Name",
            "security_type": "Type",
            "country_of_exchange": "Country",
            "sic_description": "Industry",
            "active_flag": "Status",
            "etf_count": "# ETFs",
            "max_weight": "Max Wt%",
            "avg_weight": "Avg Wt%",
            "total_weight": "Sum Wt%",
            "total_market_value": "Total Mkt Value",
        }),
        use_container_width=True, hide_index=True, height=450,
    )


# ═══════════════════════════════════════════════════════════════════════════════
elif section == "🔎 Equity Lookup":
    st.header("Equity Security Lookup")

    ticker_opts = sorted(eq["constituent_ticker"].dropna().unique().tolist())
    default_idx = ticker_opts.index("AAPL") if "AAPL" in ticker_opts else 0
    cticker = st.selectbox("Select or type an equity ticker", ticker_opts, index=default_idx)

    row = eq[eq["constituent_ticker"] == cticker]
    if row.empty:
        st.warning(f"**{cticker}** not found in equity securities.")
        st.stop()
    r = row.iloc[0]

    def _v(key):
        v = r.get(key)
        return v if pd.notna(v) else None
    def _d(key): return _v(key) or "—"

    # ── Header ────────────────────────────────────────────────────────────
    active_flag = _v("active_flag")
    hcol1, hcol2 = st.columns([5, 1])
    with hcol1:
        st.subheader(f"{cticker}  —  {_d('constituent_name')}")
        tags = "  ·  ".join(filter(None, [
            _d("security_type"), _d("country_of_exchange"),
            _d("currency_traded"), _d("exchange"),
        ]))
        st.caption(tags)
    with hcol2:
        if active_flag is True:
            st.success("ACTIVE")
        elif active_flag is False:
            reason = _v("inactive_reason") or "Inactive"
            if reason.startswith("deregistration"):
                reason = "Deregistered"
            elif reason == "not_in_edgar_tickers":
                reason = "Not in EDGAR"
            elif reason == "no_recent_10k_10q":
                reason = "Stale Filer"
            st.error(f"INACTIVE\n{reason}")
        else:
            st.info("NOT ENRICHED")

    st.divider()

    left, right = st.columns(2)

    with left:
        st.markdown("#### Identity")
        fields = {
            "Security Type":  _d("security_type"),
            "Asset Class":    _d("asset_class"),
            "Country":        _d("country_of_exchange"),
            "Exchange":       _d("exchange"),
            "Currency":       _d("currency_traded"),
            "ISIN":           _d("isin"),
            "CUSIP":          _d("cusip"),
        }
        for k, v in fields.items():
            a, b = st.columns([2, 3])
            a.markdown(f"**{k}**"); b.markdown(v)

    with right:
        st.markdown("#### SEC EDGAR")
        cik = _v("edgar_cik")
        cik_md = (
            f"[{cik}](https://www.sec.gov/cgi-bin/browse-edgar"
            f"?action=getcompany&CIK={cik})" if cik else "—"
        )
        sic = _v("sic_code"); sic_d = _v("sic_description")
        sic_str = f"{sic} — {sic_d}" if sic and sic_d else _d("sic_code")

        shares = _v("shares_outstanding")
        if shares:
            s_str = f"{shares/1e9:.3f}B" if shares >= 1e9 else f"{shares/1e6:.2f}M"
            sd = _v("shares_as_of_date")
            s_str += f" (as of {str(sd)[:10]})" if sd else ""
        else:
            s_str = "—"

        edgar_fields = {
            "CIK":            cik_md,
            "EDGAR Name":     _d("edgar_name"),
            "SIC":            sic_str,
            "State of Inc.":  _d("state_of_inc"),
            "Entity Type":    _d("entity_type"),
            "EIN":            _d("ein"),
            "Exchanges":      _d("listed_exchanges"),
            "Filer Category": _d("filer_category"),
            "Shares":         s_str,
        }
        for k, v in edgar_fields.items():
            a, b = st.columns([2, 3])
            a.markdown(f"**{k}**"); b.markdown(v)

        enriched_at = _v("edgar_enriched_at")
        if enriched_at:
            st.caption(f"EDGAR data fetched: {str(enriched_at)[:19]}")

    # ── ETF holdings ──────────────────────────────────────────────────────
    st.divider()
    st.subheader(f"ETFs Holding {cticker}")
    with st.spinner("Querying ETF holdings…"):
        etf_rows = q(f"""
            SELECT
                c.composite_ticker              AS etf,
                i.description                   AS etf_name,
                i.issuer,
                i.asset_class                   AS etf_asset_class,
                i.aum / 1e9                     AS aum_bn,
                ROUND(c.weight, 4)              AS weight,
                c.market_value,
                c.shares_held
            FROM ETF_DB.LOCAL_COPY.CONSTITUENTS c
            LEFT JOIN ETF_DB.LOCAL_COPY.INDUSTRY i
                ON  c.composite_ticker = i.composite_ticker
                AND i.as_of_date = (
                    SELECT MAX(as_of_date) FROM ETF_DB.LOCAL_COPY.INDUSTRY
                )
            WHERE c.constituent_ticker = '{cticker}'
              AND c.as_of_date = '{SNAP}'
            ORDER BY c.weight DESC
        """)

    if etf_rows.empty:
        st.info(f"**{cticker}** has no holdings in the latest snapshot.")
    else:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("ETFs Holding", f"{len(etf_rows):,}")
        m2.metric("Max Weight",   f"{etf_rows['weight'].max():.4f}%")
        m3.metric("Avg Weight",   f"{etf_rows['weight'].mean():.4f}%")
        m4.metric("Total AUM",    f"${etf_rows['aum_bn'].sum():,.1f}B")

        col1, col2 = st.columns([3, 2])
        with col1:
            top_n = min(25, len(etf_rows))
            fig = px.bar(
                etf_rows.head(top_n),
                x="weight", y="etf", orientation="h",
                color="etf_asset_class",
                hover_data=["etf_name", "aum_bn"],
                labels={"weight": "Weight (%)", "etf": "ETF",
                        "etf_asset_class": "Asset Class"},
                title=f"Top {top_n} ETF Weights (%)",
            )
            fig.update_layout(
                yaxis={"categoryorder": "total ascending"}, height=520,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.histogram(
                etf_rows, x="weight", nbins=35,
                title="Weight Distribution Across ETFs",
                labels={"weight": "Weight (%)"},
                color_discrete_sequence=["#8e44ad"],
            )
            fig.update_layout(height=280)
            st.plotly_chart(fig, use_container_width=True)

            fig = px.pie(
                etf_rows["etf_asset_class"].fillna("Unknown")
                .value_counts().reset_index()
                .rename(columns={"etf_asset_class": "Asset Class", "count": "Count"}),
                values="Count", names="Asset Class",
                title="ETFs by Asset Class",
            )
            fig.update_traces(textinfo="percent+label")
            fig.update_layout(height=240, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        etf_rows["market_value"] = etf_rows["market_value"].apply(
            lambda v: f"${v:,.0f}" if pd.notna(v) else "—"
        )
        etf_rows["shares_held"] = etf_rows["shares_held"].apply(
            lambda v: f"{v:,.0f}" if pd.notna(v) else "—"
        )
        etf_rows["aum_bn"] = etf_rows["aum_bn"].apply(
            lambda v: f"${v:.2f}B" if pd.notna(v) else "—"
        )
        st.dataframe(
            etf_rows.rename(columns={
                "etf": "ETF", "etf_name": "Name", "issuer": "Issuer",
                "etf_asset_class": "Asset Class", "aum_bn": "AUM",
                "weight": "Weight (%)", "market_value": "Mkt Value",
                "shares_held": "Shares Held",
            }),
            use_container_width=True, hide_index=True,
        )
