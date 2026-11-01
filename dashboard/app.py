
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================
# 1. PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Real-Time Mobility Dashboard",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# 2. PROJECT PATHS
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

SNAPSHOT_ROOT = ROOT / "data" / "gold_snapshots"

MANIFEST_PATH = SNAPSHOT_ROOT / "current.json"

# ============================================================
# 3. LOAD CURRENT SNAPSHOT MANIFEST
# ============================================================

@st.cache_data(ttl=10)
def read_current_manifest():

    with open(MANIFEST_PATH, "r") as file:
        manifest = json.load(file)

    snapshot_id = manifest["snapshot_id"]

    # Prevent unexpected paths in the manifest.
    if (
        not isinstance(snapshot_id, str)
        or not snapshot_id
        or "/" in snapshot_id
        or "\\" in snapshot_id
        or snapshot_id in (".", "..")
    ):
        raise ValueError("Invalid snapshot ID")

    return manifest


# ============================================================
# 4. LOAD VERSIONED GOLD DATA
# ============================================================

@st.cache_data(ttl=300)
def load_snapshot(snapshot_id):

    snapshot_path = SNAPSHOT_ROOT / snapshot_id

    fleet = pd.read_parquet(
        snapshot_path / "fleet_summary"
    )

    routes = pd.read_parquet(
        snapshot_path / "route_summary"
    )

    vehicles = pd.read_parquet(
        snapshot_path / "latest_vehicle_positions"
    )

    return fleet, routes, vehicles


def load_data():

    manifest = read_current_manifest()

    snapshot_id = manifest["snapshot_id"]

    fleet, routes, vehicles = load_snapshot(snapshot_id)

    return fleet, routes, vehicles, manifest


# ============================================================
# 5. DASHBOARD RENDERING
# ============================================================

@st.fragment(run_every="30s")
def render_dashboard():

    st.title("🚌 Real-Time Mobility Dashboard")

    st.caption(
        "Phoenix Metropolitan Area | "
        "Automatically refreshed Gold analytics"
    )

    # ========================================================
    # 6. LOAD DATA
    # ========================================================

    try:
        fleet, routes, vehicles, manifest = load_data()

    except Exception as error:

        st.error(
            f"Unable to load Gold snapshot: {error}"
        )

        st.info(
            "First run spark/gold_analytics.py "
            "to publish a Gold snapshot."
        )

        return

    if fleet.empty:
        st.warning("Fleet summary is empty.")
        return

    summary = fleet.iloc[0]

    # ========================================================
    # 7. SNAPSHOT INFORMATION
    # ========================================================

    published_at = manifest.get(
        "published_at_utc", "Unknown"
    )

    snapshot_id = manifest.get(
        "snapshot_id", "Unknown"
    )

    st.caption(
        f"Last analytics refresh (UTC): {published_at}"
    )

    with st.expander("Snapshot Information"):

        st.write(f"Snapshot ID: `{snapshot_id}`")

        st.write(
            "Input observations:",
            manifest.get("input_observations", "Unknown")
        )

        st.write(
            "Dataset counts:",
            manifest.get("datasets", {})
        )

    # ========================================================
    # 8. FLEET KPI CARDS
    # ========================================================

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Total Observations",
        f"{int(summary['total_observations']):,}"
    )

    c2.metric(
        "Distinct Vehicle IDs",
        f"{int(summary['unique_vehicles']):,}"
    )

    average_speed = summary[
        "average_valid_speed_kmh"
    ]

    c3.metric(
        "Average Valid Speed",
        (
            f"{average_speed:.2f} km/h"
            if pd.notna(average_speed)
            else "N/A"
        )
    )

    c4.metric(
        "Suspicious Movements",
        f"{int(summary['suspicious_movements']):,}"
    )

    st.divider()

    # ========================================================
    # 9. DATA FRESHNESS
    # ========================================================

    st.subheader("Data Freshness")

    st.caption(
        "Freshness is calculated from the latest vehicle "
        "timestamp in the saved Gold snapshot."
    )

    vehicles = vehicles.copy()

    vehicles["vehicle_timestamp_unix"] = (
        pd.to_numeric(
            vehicles["vehicle_timestamp_unix"],
            errors="coerce"
        )
    )

    latest_timestamp = (
        vehicles["vehicle_timestamp_unix"].max()
        if not vehicles.empty
        else float("nan")
    )

    if pd.notna(latest_timestamp):

        latest_datetime = datetime.fromtimestamp(
            float(latest_timestamp),
            tz=timezone.utc
        )

        now_utc = datetime.now(timezone.utc)

        age_minutes = (
            now_utc - latest_datetime
        ).total_seconds() / 60

        if age_minutes < 0:
            freshness_status = "Future timestamp detected"
        elif age_minutes <= 5:
            freshness_status = "Recent"
        elif age_minutes <= 30:
            freshness_status = "Delayed"
        else:
            freshness_status = "Stale"

        f1, f2, f3 = st.columns(3)

        f1.metric(
            "Latest Vehicle Observation",
            latest_datetime.strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
        )

        f2.metric(
            "Data Age",
            (
                f"{age_minutes:.1f} minutes"
                if age_minutes >= 0
                else "Future timestamp"
            )
        )

        f3.metric(
            "Freshness Status",
            freshness_status
        )

    else:
        st.warning(
            "No valid vehicle timestamps are available."
        )

    st.divider()

    # ========================================================
    # 10. VEHICLE LOCATION MAP
    # ========================================================

    st.subheader("Latest Recorded Vehicle Locations")

    st.caption(
        "Shows the latest stored GPS position for each "
        "vehicle ID. These positions are not guaranteed "
        "to represent currently operating vehicles."
    )

    vehicles["route_label"] = (
        vehicles["route_id"]
        .fillna("UNKNOWN")
        .astype(str)
    )

    route_options = sorted(
        vehicles["route_label"].unique().tolist()
    )

    selected_routes = st.multiselect(
        "Filter vehicles by route",
        options=route_options,
        key="vehicle_route_filter"
    )

    map_df = vehicles.copy()

    if selected_routes:
        map_df = map_df[
            map_df["route_label"].isin(selected_routes)
        ]

    map_df["latitude"] = pd.to_numeric(
        map_df["latitude"], errors="coerce"
    )

    map_df["longitude"] = pd.to_numeric(
        map_df["longitude"], errors="coerce"
    )

    map_df = map_df.dropna(
        subset=["latitude", "longitude"]
    )

    map_df = map_df[
        map_df["latitude"].between(32.5, 34.5)
        & map_df["longitude"].between(-113.5, -110.5)
    ]

    st.write(
        f"Showing **{len(map_df):,}** vehicle positions"
    )

    if not map_df.empty:

        st.map(
            map_df,
            latitude="latitude",
            longitude="longitude",
            zoom=9
        )

    else:

        st.warning(
            "No vehicle positions match the selected routes."
        )

    st.divider()

    # ========================================================
    # 11. ROUTE-LEVEL ANALYTICS
    # ========================================================

    st.subheader("Route-Level Analytics")

    top_routes = (
        routes.sort_values(
            "total_observations",
            ascending=False
        )
        .head(15)
        .copy()
    )

    top_routes["route_label"] = (
        top_routes["route_label"].astype(str)
    )

    if not top_routes.empty:

        fig_routes = px.bar(
            top_routes,
            x="route_label",
            y="total_observations",
            color="total_observations",
            title="Top 15 Routes by Observation Count",
            labels={
                "route_label": "Route",
                "total_observations": "Observations"
            }
        )

        fig_routes.update_xaxes(
            type="category",
            categoryorder="array",
            categoryarray=top_routes[
                "route_label"
            ].tolist()
        )

        fig_routes.update_layout(
            coloraxis_showscale=False,
            xaxis_title="Route",
            yaxis_title="Observations"
        )

        st.plotly_chart(
            fig_routes,
            use_container_width=True
        )

    st.divider()

    # ========================================================
    # 12. MOVEMENT QUALITY
    # ========================================================

    st.subheader("Movement Quality")

    normal = int(
        summary["normal_movements"]
    )

    suspicious = int(
        summary["suspicious_movements"]
    )

    quality_df = pd.DataFrame({
        "Movement Status": [
            "Normal",
            "Suspicious Speed"
        ],
        "Observations": [
            normal,
            suspicious
        ]
    })

    fig_quality = px.pie(
        quality_df,
        names="Movement Status",
        values="Observations",
        hole=0.5,
        title="Normal vs Suspicious Movement Pairs"
    )

    st.plotly_chart(
        fig_quality,
        use_container_width=True
    )

    st.caption(
        "First observations are excluded because "
        "there is no previous position available "
        "for calculating movement speed."
    )

    # ========================================================
    # 13. ROUTE PERFORMANCE TABLE
    # ========================================================

    st.subheader("Route Performance Details")

    st.dataframe(
        routes.sort_values(
            "total_observations",
            ascending=False
        ),
        use_container_width=True,
        hide_index=True
    )

    # ========================================================
    # 14. VEHICLE POSITION TABLE
    # ========================================================

    st.subheader("Latest Vehicle Position Details")

    st.dataframe(
        vehicles.drop(
            columns=["route_label"]
        ),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # ========================================================
    # 15. FOOTER
    # ========================================================

    st.caption(
        "Source: Valley Metro GTFS-Realtime | "
        "Kafka → Spark → Bronze → Silver → Gold"
    )

    st.caption(
        "Dashboard checks for newly published "
        "Gold snapshots every 30 seconds."
    )


# ============================================================
# 16. RUN DASHBOARD
# ============================================================

render_dashboard()
