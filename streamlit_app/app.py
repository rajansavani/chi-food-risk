import os
import pandas as pd
import streamlit as st
import altair as alt

st.set_page_config(
    page_title="Chicago Food Safety Risk Intelligence",
    page_icon="🍽️",
    layout="wide",
)

# @st.cache_data tells Streamlit to cache the result of this function.
# allows dashboard to stay fast and responsive
@st.cache_data(ttl=3600)
def load_risk_scores() -> pd.DataFrame:
    db_url = os.getenv("DATABASE_URL", "")
    if db_url:
        try:
            from sqlalchemy import create_engine
            engine = create_engine(db_url)
            df = pd.read_sql("SELECT * FROM risk_scores", engine)
            df["last_inspection_date"] = pd.to_datetime(df["last_inspection_date"], errors="coerce")
            return df
        except Exception as e:
            st.warning(f"Could not connect to database, using local CSV. Error: {e}")

    path = os.path.join(os.path.dirname(__file__), "..", "data", "risk_scores.csv")
    if not os.path.exists(path):
        st.error("No data found. Run the pipeline first: python src/transform.py")
        st.stop()
    df = pd.read_csv(path)
    df["last_inspection_date"] = pd.to_datetime(df["last_inspection_date"], errors="coerce")
    return df


@st.cache_data(ttl=3600)
def load_inspections() -> pd.DataFrame:
    db_url = os.getenv("DATABASE_URL", "")
    if db_url:
        try:
            from sqlalchemy import create_engine
            engine = create_engine(db_url)
            df = pd.read_sql(
                "SELECT inspection_id, establishment_id, dba_name, "
                "inspection_date, inspection_type, results, result_score, "
                "violation_severity, n_critical, n_serious "
                "FROM inspections ORDER BY inspection_date DESC",
                engine,
            )
            df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
            return df
        except Exception:
            pass

    path = os.path.join(os.path.dirname(__file__), "..", "data", "transformed_inspections.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    return df


risk_df = load_risk_scores()
insp_df = load_inspections()

# sidebar filters
st.sidebar.title("Filters")

min_score, max_score = st.sidebar.slider(
    "Risk Score Range",
    min_value=0, max_value=100,
    value=(0, 100),
)

facility_types = sorted(risk_df["facility_type"].dropna().unique())
selected_types = st.sidebar.multiselect("Facility Type", options=facility_types)

zip_codes = sorted(risk_df["zip"].dropna().unique())
selected_zips = st.sidebar.multiselect("ZIP Code", options=zip_codes)

# apply filters
filtered = risk_df[
    (risk_df["dynamic_risk_score"] >= min_score)
    & (risk_df["dynamic_risk_score"] <= max_score)
]
if selected_types:
    filtered = filtered[filtered["facility_type"].isin(selected_types)]
if selected_zips:
    filtered = filtered[filtered["zip"].isin(selected_zips)]

# header
st.title("🍽️ Chicago Food Safety Risk Intelligence")
st.caption(
    "Dynamic risk scoring for 42,000+ Chicago food establishments. "
    "Scores are computed from inspection outcomes, NLP-based violation "
    "analysis, and performance trends. "
    "[Data source](https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5)"
)
st.divider()


# show some high-level KPIs about the filtered set of establishments
c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Establishments", f"{len(filtered):,}")
c2.metric("High Risk (≥70)", f"{(filtered['dynamic_risk_score'] >= 70).sum():,}")
c3.metric("Avg Risk Score", f"{filtered['dynamic_risk_score'].mean():.1f}")
c4.metric("Total Failures", f"{filtered['total_failures'].sum():,}")
c5.metric("Total Inspections", f"{filtered['total_inspections'].sum():,}")

st.divider()


# risk map with dots sized by risk score
st.subheader("📍 Risk Map")

map_data = filtered.dropna(subset=["latitude", "longitude"]).copy()

if len(map_data) > 0:
    # scale dot size by risk score
    map_data["size"] = (map_data["dynamic_risk_score"] / 100 * 60 + 10).clip(10, 70)

    st.map(map_data, latitude="latitude", longitude="longitude", size="size")
else:
    st.info("No establishments match the current filters.")

st.divider()



# list of highest-risk establishments with key details and inspection history drill-down
st.subheader("🚨 Highest Risk Establishments")

top_n = st.slider("Show top N", min_value=5, max_value=50, value=20, step=5)

display_cols = {
    "dba_name": "Name",
    "address": "Address",
    "zip": "ZIP",
    "facility_type": "Type",
    "dynamic_risk_score": "Risk Score",
    "failure_rate_score": "Failure Rate",
    "violation_severity_score": "Violation Severity",
    "recency_score": "Recency",
    "trend_score": "Trend",
    "total_inspections": "Inspections",
    "total_failures": "Failures",
    "last_result": "Last Result",
}

top_risky = filtered.nlargest(top_n, "dynamic_risk_score")
available_cols = {k: v for k, v in display_cols.items() if k in top_risky.columns}

st.dataframe(
    top_risky[list(available_cols.keys())].rename(columns=available_cols),
    use_container_width=True,
    hide_index=True,
)

st.divider()



# score distribution histogram and risk by facility type bar chart
st.subheader("📊 Score Distribution")

col_left, col_right = st.columns(2)

with col_left:
    hist = (
        alt.Chart(filtered[["dynamic_risk_score"]].dropna())
        .mark_bar(opacity=0.8, color="#4A90D9")
        .encode(
            alt.X("dynamic_risk_score:Q", bin=alt.Bin(maxbins=25), title="Risk Score"),
            alt.Y("count()", title="Establishments"),
        )
        .properties(height=350, title="Distribution of Dynamic Risk Scores")
    )
    st.altair_chart(hist, use_container_width=True)

with col_right:
    # avg risk by facility type (only types with 10+ establishments)
    type_risk = (
        filtered.groupby("facility_type")["dynamic_risk_score"]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"mean": "avg_risk", "count": "n"})
        .query("n >= 10")
        .sort_values("avg_risk", ascending=False)
        .head(15)
    )

    if len(type_risk) > 0:
        bars = (
            alt.Chart(type_risk)
            .mark_bar(opacity=0.8, color="#E74C3C")
            .encode(
                alt.X("avg_risk:Q", title="Average Risk Score"),
                alt.Y("facility_type:N", sort="-x", title=""),
                tooltip=["facility_type", "avg_risk", "n"],
            )
            .properties(height=350, title="Avg Risk by Facility Type (min 10)")
        )
        st.altair_chart(bars, use_container_width=True)

st.divider()


# search an establishment by name or address and see details + inspection history
st.subheader("🔎 Establishment Lookup")

search = st.text_input(
    "Search by name or address",
    placeholder="e.g. McDonald's, or 123 N State",
)

if search:
    term = search.lower()
    matches = risk_df[
        risk_df["dba_name"].astype(str).str.lower().str.contains(term, na=False)
        | risk_df["aka_name"].astype(str).str.lower().str.contains(term, na=False)
        | risk_df["address"].astype(str).str.lower().str.contains(term, na=False)
    ].nlargest(20, "dynamic_risk_score")

    if len(matches) == 0:
        st.warning("No establishments found.")
    else:
        st.write(f"Found {len(matches)} matches (top 20 by risk score):")

        for _, est in matches.iterrows():
            # color indicator by score
            if est["dynamic_risk_score"] >= 70:
                icon = "🔴"
            elif est["dynamic_risk_score"] >= 40:
                icon = "🟡"
            else:
                icon = "🟢"

            label = f"{icon} {est['dba_name']} — {est['address']} — Score: {est['dynamic_risk_score']}"

            with st.expander(label):
                # score breakdown
                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Failure Rate", f"{est.get('failure_rate_score', 0):.0f}/100")
                s2.metric("Recency", f"{est.get('recency_score', 0):.0f}/100")
                s3.metric("Violation Severity", f"{est.get('violation_severity_score', 0):.0f}/100")

                trend = est.get("trend_score", 50)
                trend_label = "Worsening" if trend > 55 else "Improving" if trend < 45 else "Stable"
                s4.metric("Trend", trend_label)

                st.caption(
                    f"Type: {est.get('facility_type', 'N/A')} · "
                    f"City Risk Category: {est.get('risk_category', 'N/A')} · "
                    f"Inspections: {est.get('total_inspections', 0)} · "
                    f"Last inspected: {str(est.get('last_inspection_date', 'N/A'))[:10]}"
                )

                # inspection history from the full inspections table
                if len(insp_df) > 0 and "establishment_id" in insp_df.columns:
                    history = insp_df[
                        insp_df["establishment_id"] == est["establishment_id"]
                    ].sort_values("inspection_date", ascending=False).head(10)

                    if len(history) > 0:
                        st.write("**Recent Inspection History**")
                        hist_cols = {
                            "inspection_date": "Date",
                            "inspection_type": "Type",
                            "results": "Result",
                            "violation_severity": "Severity",
                            "n_critical": "Critical",
                            "n_serious": "Serious",
                        }
                        available = {k: v for k, v in hist_cols.items() if k in history.columns}
                        st.dataframe(
                            history[list(available.keys())].rename(columns=available),
                            use_container_width=True,
                            hide_index=True,
                        )

st.divider()


# risk by ZIP code table and bar chart
st.subheader("🏘️ Risk by ZIP Code")

zip_risk = (
    filtered.groupby("zip")
    .agg(
        avg_risk=("dynamic_risk_score", "mean"),
        n_establishments=("establishment_id", "count"),
        high_risk=("dynamic_risk_score", lambda x: (x >= 70).sum()),
    )
    .reset_index()
    .query("n_establishments >= 5")
    .sort_values("avg_risk", ascending=False)
)

col_z1, col_z2 = st.columns(2)

with col_z1:
    st.write("**Riskiest ZIP Codes** (min 5 establishments)")
    st.dataframe(
        zip_risk.head(15).rename(columns={
            "zip": "ZIP",
            "avg_risk": "Avg Risk",
            "n_establishments": "Establishments",
            "high_risk": "High Risk (≥70)",
        }).round(1),
        use_container_width=True,
        hide_index=True,
    )

with col_z2:
    if len(zip_risk) > 0:
        zip_chart = (
            alt.Chart(zip_risk.head(20))
            .mark_bar()
            .encode(
                alt.X("avg_risk:Q", title="Average Risk Score"),
                alt.Y("zip:N", sort="-x", title="ZIP Code"),
                alt.Color(
                    "avg_risk:Q",
                    scale=alt.Scale(scheme="reds"),
                    legend=None,
                ),
                tooltip=["zip", "avg_risk", "n_establishments", "high_risk"],
            )
            .properties(height=400, title="Top 20 ZIPs by Avg Risk")
        )
        st.altair_chart(zip_chart, use_container_width=True)


# footer
st.divider()
st.caption(
    "Built by Rajan Savani · "
    "Data: City of Chicago Open Data Portal · "
    "Risk scores are computed from inspection history, NLP-based violation "
    "severity analysis, and performance trends."
)