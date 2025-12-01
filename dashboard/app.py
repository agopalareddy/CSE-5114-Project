"""
MLB Statcast Live Dashboard
============================
A Streamlit dashboard that connects to Snowflake to display
MLB pitch-by-pitch analytics in real-time.

Run with:
    streamlit run dashboard/app.py

Requirements:
    pip install streamlit snowflake-connector-python plotly pandas
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Kafka simulation helpers
from kafka_simulation import (
    get_simulation_status,
    start_kafka_simulation,
    pause_kafka_simulation,
    stop_kafka_simulation,
    update_simulation_speed,
    get_simulation_pitches_query,
    is_kafka_available,
)

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="MLB Statcast Dashboard",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# SNOWFLAKE CONNECTION
# =============================================================================


@st.cache_resource
def get_snowflake_connection():
    """
    Create and cache Snowflake connection using key-pair authentication.
    """
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    import base64

    # Load environment variables
    load_dotenv()

    SF_ACCOUNT = os.getenv("SF_ACCOUNT")
    SF_USER = os.getenv("SF_USER")
    SF_DATABASE = os.getenv("SF_DATABASE", "BLUEJAY_DB")
    SF_SCHEMA = os.getenv("SF_SCHEMA", "BLUEJAY_SCHEMA")
    SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
    SF_PRIVATE_KEY_FILE = os.getenv("SF_PRIVATE_KEY_FILE")
    SF_PRIVATE_KEY_B64 = os.getenv("SF_PRIVATE_KEY_B64")

    private_key = None

    # Load private key
    if SF_PRIVATE_KEY_FILE and os.path.exists(SF_PRIVATE_KEY_FILE):
        with open(SF_PRIVATE_KEY_FILE, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
    elif SF_PRIVATE_KEY_B64:
        private_key_bytes = base64.b64decode(SF_PRIVATE_KEY_B64)
        private_key = serialization.load_der_private_key(
            private_key_bytes, password=None, backend=default_backend()
        )
    else:
        st.error("Snowflake credentials not found. Check your .env file.")
        return None

    try:
        conn = snowflake.connector.connect(
            account=SF_ACCOUNT,
            user=SF_USER,
            private_key=private_key,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
            warehouse=SF_WAREHOUSE,
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None


@st.cache_data(ttl=60)  # Cache for 60 seconds (for live updates)
def run_query(query):
    """Execute a query and return results as DataFrame."""
    conn = get_snowflake_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# =============================================================================
# DATA QUERIES
# =============================================================================


def get_data_summary():
    """Get overall data summary."""
    query = """
    SELECT 
        COUNT(*) as total_pitches,
        COUNT(DISTINCT "game_pk") as total_games,
        COUNT(DISTINCT "pitcher") as unique_pitchers,
        COUNT(DISTINCT "batter") as unique_batters,
        MIN("game_date") as earliest_date,
        MAX("game_date") as latest_date
    FROM STATCAST
    """
    return run_query(query)


def get_available_dates():
    """Get list of dates with data and date range."""
    query = """
    SELECT DISTINCT "game_date" as GAME_DATE
    FROM STATCAST 
    ORDER BY "game_date" DESC
    """
    df = run_query(query)
    if df.empty:
        return [], None, None
    dates = df["GAME_DATE"].tolist()
    # Convert to date objects if they aren't already
    date_objects = []
    for d in dates:
        if hasattr(d, "date"):
            date_objects.append(d.date())
        elif hasattr(d, "strftime"):
            date_objects.append(d)
        else:
            from datetime import datetime

            date_objects.append(datetime.strptime(str(d), "%Y-%m-%d").date())
    return date_objects, min(date_objects), max(date_objects)


def get_games_for_date(date_str):
    """Get games played on a specific date."""
    query = f"""
    SELECT DISTINCT 
        "game_pk" as GAME_PK,
        "home_team" as HOME_TEAM,
        "away_team" as AWAY_TEAM,
        MAX("home_score") as HOME_SCORE,
        MAX("away_score") as AWAY_SCORE,
        COUNT(*) as TOTAL_PITCHES
    FROM STATCAST
    WHERE "game_date" = '{date_str}'
    GROUP BY "game_pk", "home_team", "away_team"
    ORDER BY "game_pk"
    """
    return run_query(query)


def get_game_pitches(game_pk):
    """Get all pitches for a specific game."""
    query = f"""
    SELECT 
        CONCAT("game_pk", '_', "at_bat_number", '_', "pitch_number") as PITCH_UID,
        "inning" as INNING,
        "inning_topbot" as INNING_TOPBOT,
        "at_bat_number" as AT_BAT_NUMBER,
        "pitch_number" as PITCH_NUMBER,
        "pitcher" as PITCHER,
        "player_name" as PITCHER_NAME,
        "batter" as BATTER,
        "pitch_type" as PITCH_TYPE,
        "pitch_name" as PITCH_NAME,
        "release_speed" as RELEASE_SPEED,
        "release_spin_rate" as RELEASE_SPIN_RATE,
        "pfx_x" as PFX_X,
        "pfx_z" as PFX_Z,
        "plate_x" as PLATE_X,
        "plate_z" as PLATE_Z,
        "zone" as ZONE,
        "type" as TYPE,
        "events" as EVENTS,
        "description" as DESCRIPTION,
        "balls" as BALLS,
        "strikes" as STRIKES,
        "home_score" as HOME_SCORE,
        "away_score" as AWAY_SCORE
    FROM STATCAST
    WHERE "game_pk" = {game_pk}
    ORDER BY "at_bat_number", "pitch_number"
    """
    return run_query(query)


def get_pitcher_stats(game_pk=None, date_str=None):
    """Get pitcher statistics."""
    where_clause = ""
    if game_pk:
        where_clause = f'WHERE "game_pk" = {game_pk}'
    elif date_str:
        where_clause = f"WHERE \"game_date\" = '{date_str}'"

    query = f"""
    SELECT 
        "pitcher" as PITCHER,
        "player_name" as PITCHER_NAME,
        "pitch_type" as PITCH_TYPE,
        "pitch_name" as PITCH_NAME,
        COUNT(*) as PITCH_COUNT,
        ROUND(AVG("release_speed"), 1) as AVG_VELOCITY,
        ROUND(MAX("release_speed"), 1) as MAX_VELOCITY,
        ROUND(AVG("release_spin_rate"), 0) as AVG_SPIN_RATE,
        ROUND(AVG("pfx_x") * 12, 1) as AVG_H_BREAK_INCHES,
        ROUND(AVG("pfx_z") * 12, 1) as AVG_V_BREAK_INCHES,
        ROUND(100.0 * SUM(CASE WHEN "type" = 'S' THEN 1 ELSE 0 END) / COUNT(*), 1) as STRIKE_PCT
    FROM STATCAST
    {where_clause}
    GROUP BY "pitcher", "player_name", "pitch_type", "pitch_name"
    ORDER BY PITCH_COUNT DESC
    """
    return run_query(query)


def get_pitch_locations(game_pk=None, pitcher_id=None):
    """Get pitch location data for strike zone visualization."""
    where_clauses = []
    if game_pk:
        where_clauses.append(f'"game_pk" = {game_pk}')
    if pitcher_id:
        where_clauses.append(f'"pitcher" = {pitcher_id}')

    where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
    SELECT 
        "plate_x" as PLATE_X,
        "plate_z" as PLATE_Z,
        "pitch_type" as PITCH_TYPE,
        "pitch_name" as PITCH_NAME,
        "type" as TYPE,
        "description" as DESCRIPTION,
        "release_speed" as RELEASE_SPEED
    FROM STATCAST
    {where_str}
    AND "plate_x" IS NOT NULL 
    AND "plate_z" IS NOT NULL
    LIMIT 500
    """
    return run_query(query)


def get_velocity_over_game(game_pk, pitcher_id=None):
    """Get velocity progression through the game."""
    pitcher_filter = f'AND "pitcher" = {pitcher_id}' if pitcher_id else ""

    query = f"""
    SELECT 
        "at_bat_number" as AT_BAT_NUMBER,
        "pitch_number" as PITCH_NUMBER,
        "pitcher" as PITCHER,
        "player_name" as PITCHER_NAME,
        "pitch_type" as PITCH_TYPE,
        "release_speed" as RELEASE_SPEED,
        "inning" as INNING
    FROM STATCAST
    WHERE "game_pk" = {game_pk}
    {pitcher_filter}
    AND "release_speed" IS NOT NULL
    ORDER BY "at_bat_number", "pitch_number"
    """
    return run_query(query)


def get_team_pitch_usage(date_str=None, season_year=None):
    """Get pitch type usage by team."""
    where_clause = ""
    if date_str:
        where_clause = f"WHERE \"game_date\" = '{date_str}'"
    elif season_year:
        where_clause = f'WHERE YEAR("game_date") = {season_year}'

    query = f"""
    SELECT 
        "home_team" as team,
        "pitch_type",
        "pitch_name",
        COUNT(*) as pitch_count,
        ROUND(AVG("release_speed"), 1) as avg_velocity
    FROM STATCAST
    {where_clause}
    GROUP BY "home_team", "pitch_type", "pitch_name"
    ORDER BY team, pitch_count DESC
    """
    return run_query(query)


def get_all_teams():
    """Get list of all teams in the data."""
    query = """
    SELECT DISTINCT "home_team" as TEAM
    FROM STATCAST
    UNION
    SELECT DISTINCT "away_team" as TEAM
    FROM STATCAST
    ORDER BY TEAM
    """
    df = run_query(query)
    return df["TEAM"].tolist() if not df.empty else []


def get_team_matchup_games(team1, team2):
    """Get all games between two teams."""
    query = f"""
    SELECT 
        "game_pk" as GAME_PK,
        "game_date" as GAME_DATE,
        "home_team" as HOME_TEAM,
        "away_team" as AWAY_TEAM,
        MAX("home_score") as HOME_SCORE,
        MAX("away_score") as AWAY_SCORE,
        COUNT(*) as TOTAL_PITCHES,
        COUNT(DISTINCT "pitcher") as PITCHERS_USED,
        ROUND(AVG("release_speed"), 1) as AVG_VELOCITY
    FROM STATCAST
    WHERE ("home_team" = '{team1}' AND "away_team" = '{team2}')
       OR ("home_team" = '{team2}' AND "away_team" = '{team1}')
    GROUP BY "game_pk", "game_date", "home_team", "away_team"
    ORDER BY "game_date" DESC
    """
    return run_query(query)


def get_team_matchup_summary(team1, team2):
    """Get summary statistics for matchup between two teams."""
    query = f"""
    WITH game_results AS (
        SELECT 
            "game_pk" as GAME_PK,
            "game_date" as GAME_DATE,
            "home_team" as HOME_TEAM,
            "away_team" as AWAY_TEAM,
            MAX("home_score") as HOME_SCORE,
            MAX("away_score") as AWAY_SCORE
        FROM STATCAST
        WHERE ("home_team" = '{team1}' AND "away_team" = '{team2}')
           OR ("home_team" = '{team2}' AND "away_team" = '{team1}')
        GROUP BY "game_pk", "game_date", "home_team", "away_team"
    )
    SELECT
        COUNT(*) as TOTAL_GAMES,
        SUM(CASE WHEN (HOME_TEAM = '{team1}' AND HOME_SCORE > AWAY_SCORE) 
                  OR (AWAY_TEAM = '{team1}' AND AWAY_SCORE > HOME_SCORE) THEN 1 ELSE 0 END) as TEAM1_WINS,
        SUM(CASE WHEN (HOME_TEAM = '{team2}' AND HOME_SCORE > AWAY_SCORE) 
                  OR (AWAY_TEAM = '{team2}' AND AWAY_SCORE > HOME_SCORE) THEN 1 ELSE 0 END) as TEAM2_WINS,
        SUM(CASE WHEN HOME_TEAM = '{team1}' THEN HOME_SCORE ELSE AWAY_SCORE END) as TEAM1_RUNS,
        SUM(CASE WHEN HOME_TEAM = '{team2}' THEN HOME_SCORE ELSE AWAY_SCORE END) as TEAM2_RUNS,
        MIN(GAME_DATE) as FIRST_GAME,
        MAX(GAME_DATE) as LAST_GAME
    FROM game_results
    """
    return run_query(query)


def get_matchup_pitching_stats(team1, team2):
    """Get pitching statistics for games between two teams."""
    query = f"""
    SELECT 
        CASE WHEN "home_team" = '{team1}' OR "away_team" = '{team1}' THEN
            CASE WHEN ("home_team" = '{team1}' AND "inning_topbot" = 'Bot') 
                  OR ("away_team" = '{team1}' AND "inning_topbot" = 'Top') 
                 THEN '{team1}' ELSE '{team2}' END
        END as PITCHING_TEAM,
        "pitch_type" as PITCH_TYPE,
        "pitch_name" as PITCH_NAME,
        COUNT(*) as PITCH_COUNT,
        ROUND(AVG("release_speed"), 1) as AVG_VELOCITY,
        ROUND(AVG("release_spin_rate"), 0) as AVG_SPIN,
        ROUND(100.0 * SUM(CASE WHEN "type" = 'S' THEN 1 ELSE 0 END) / COUNT(*), 1) as STRIKE_PCT
    FROM STATCAST
    WHERE ("home_team" = '{team1}' AND "away_team" = '{team2}')
       OR ("home_team" = '{team2}' AND "away_team" = '{team1}')
    GROUP BY PITCHING_TEAM, "pitch_type", "pitch_name"
    HAVING PITCHING_TEAM IS NOT NULL
    ORDER BY PITCHING_TEAM, PITCH_COUNT DESC
    """
    return run_query(query)


def get_matchup_top_pitchers(team1, team2):
    """Get top pitchers in matchup between two teams."""
    query = f"""
    SELECT 
        "player_name" as PITCHER_NAME,
        CASE WHEN ("home_team" = '{team1}' AND "inning_topbot" = 'Bot') 
              OR ("away_team" = '{team1}' AND "inning_topbot" = 'Top') 
             THEN '{team1}' ELSE '{team2}' END as TEAM,
        COUNT(*) as TOTAL_PITCHES,
        COUNT(DISTINCT "game_pk") as GAMES,
        ROUND(AVG("release_speed"), 1) as AVG_VELOCITY,
        ROUND(MAX("release_speed"), 1) as MAX_VELOCITY,
        ROUND(100.0 * SUM(CASE WHEN "type" = 'S' THEN 1 ELSE 0 END) / COUNT(*), 1) as STRIKE_PCT
    FROM STATCAST
    WHERE (("home_team" = '{team1}' AND "away_team" = '{team2}')
       OR ("home_team" = '{team2}' AND "away_team" = '{team1}'))
    GROUP BY "player_name", TEAM
    ORDER BY TOTAL_PITCHES DESC
    LIMIT 10
    """
    return run_query(query)


# =============================================================================
# VISUALIZATION FUNCTIONS
# =============================================================================


def create_strike_zone_plot(df):
    """Create a strike zone scatter plot."""
    if df.empty:
        return go.Figure()

    # Strike zone coordinates (in feet from center of plate)
    sz_left = -0.83
    sz_right = 0.83
    sz_bottom = 1.5
    sz_top = 3.5

    # Color map for pitch results
    color_map = {"S": "red", "B": "blue", "X": "green"}  # Strike  # Ball  # In play

    fig = go.Figure()

    # Add strike zone rectangle
    fig.add_shape(
        type="rect",
        x0=sz_left,
        y0=sz_bottom,
        x1=sz_right,
        y1=sz_top,
        line=dict(color="black", width=2),
        fillcolor="rgba(0,0,0,0)",
    )

    # Add home plate
    fig.add_shape(
        type="path",
        path="M -0.83 0 L 0.83 0 L 0.83 0.25 L 0 0.5 L -0.83 0.25 Z",
        line=dict(color="black", width=1),
        fillcolor="white",
    )

    # Plot pitches
    for pitch_type in df["TYPE"].unique():
        pitch_df = df[df["TYPE"] == pitch_type]
        fig.add_trace(
            go.Scatter(
                x=pitch_df["PLATE_X"],
                y=pitch_df["PLATE_Z"],
                mode="markers",
                name=f"{'Strike' if pitch_type == 'S' else 'Ball' if pitch_type == 'B' else 'In Play'}",
                marker=dict(
                    color=color_map.get(pitch_type, "gray"), size=8, opacity=0.6
                ),
                hovertemplate=(
                    "Location: (%{x:.2f}, %{y:.2f})<br>"
                    "Velocity: %{customdata[0]:.1f} mph<br>"
                    "Pitch: %{customdata[1]}<br>"
                    "<extra></extra>"
                ),
                customdata=pitch_df[["RELEASE_SPEED", "PITCH_NAME"]].values,
            )
        )

    fig.update_layout(
        title="Strike Zone",
        xaxis_title="Horizontal (ft from center)",
        yaxis_title="Vertical (ft)",
        xaxis=dict(range=[-2.5, 2.5], scaleanchor="y"),
        yaxis=dict(range=[0, 5]),
        height=500,
        showlegend=True,
    )

    return fig


def create_velocity_chart(df):
    """Create velocity progression chart."""
    if df.empty:
        return go.Figure()

    fig = px.scatter(
        df,
        x=df.index,
        y="RELEASE_SPEED",
        color="PITCH_TYPE",
        hover_data=["PITCHER_NAME", "INNING"],
        title="Pitch Velocity Throughout Game",
    )

    fig.update_layout(
        xaxis_title="Pitch Number", yaxis_title="Velocity (mph)", height=400
    )

    return fig


def create_pitch_mix_chart(df):
    """Create pitch mix pie chart."""
    if df.empty:
        return go.Figure()

    pitch_counts = df.groupby("PITCH_NAME")["PITCH_COUNT"].sum().reset_index()

    fig = px.pie(
        pitch_counts, values="PITCH_COUNT", names="PITCH_NAME", title="Pitch Mix"
    )

    return fig


# =============================================================================
# PAGE FUNCTIONS
# =============================================================================


def show_data_overview(summary):
    """Display data overview metrics."""
    st.header("📊 Data Overview")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Pitches", f"{summary['TOTAL_PITCHES'].iloc[0]:,}")
    with col2:
        st.metric("Total Games", f"{summary['TOTAL_GAMES'].iloc[0]:,}")
    with col3:
        st.metric("Unique Pitchers", f"{summary['UNIQUE_PITCHERS'].iloc[0]:,}")
    with col4:
        st.metric("Unique Batters", f"{summary['UNIQUE_BATTERS'].iloc[0]:,}")

    st.caption(
        f"Data range: {summary['EARLIEST_DATE'].iloc[0]} to {summary['LATEST_DATE'].iloc[0]}"
    )


def page_game_explorer():
    """Game Explorer page - browse games by date."""
    st.title("🎯 Game Explorer")
    st.markdown("Browse and analyze individual games by date")

    # Get data summary for overview
    summary = get_data_summary()
    if summary.empty:
        st.warning("No data found in STATCAST table. Is the backfill running?")
        st.stop()

    show_data_overview(summary)
    st.divider()

    available_dates, min_date, max_date = get_available_dates()
    if not available_dates:
        st.warning("No games found in database.")
        st.stop()

    # Convert available_dates to a set for quick lookup
    available_dates_set = set(available_dates)

    # Use date_input with calendar picker
    selected_date = st.date_input(
        "Select Date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        help=f"Data available from {min_date} to {max_date}",
    )

    # Check if selected date has data
    if selected_date not in available_dates_set:
        st.warning(
            f"No games on {selected_date.strftime('%Y-%m-%d (%A)')}. Please select another date."
        )
        nearby = sorted(
            [d for d in available_dates if abs((d - selected_date).days) <= 7]
        )
        if nearby:
            st.info(
                f"Nearby dates with games: {', '.join(d.strftime('%m/%d') for d in nearby[:5])}"
            )
        st.stop()

    # Get games for selected date
    games_df = get_games_for_date(str(selected_date))

    if games_df.empty:
        st.info(f"No games found for {selected_date}")
        st.stop()

    # Display games
    st.subheader(f"Games on {selected_date}")

    game_options = []
    for _, game in games_df.iterrows():
        label = f"{game['AWAY_TEAM']} @ {game['HOME_TEAM']} ({game['AWAY_SCORE']}-{game['HOME_SCORE']}) - {game['TOTAL_PITCHES']} pitches"
        game_options.append((game["GAME_PK"], label))

    selected_game = st.selectbox(
        "Select Game", game_options, format_func=lambda x: x[1]
    )

    game_pk = selected_game[0]

    st.divider()

    # Game Analysis
    st.header("📈 Game Analysis")

    tab1, tab2, tab3 = st.tabs(["Pitcher Stats", "Strike Zone", "Velocity Tracker"])

    with tab1:
        st.subheader("Pitcher Performance")
        pitcher_stats = get_pitcher_stats(game_pk=game_pk)

        if not pitcher_stats.empty:
            pitchers = pitcher_stats["PITCHER_NAME"].unique()

            for pitcher_name in pitchers[:5]:
                pitcher_df = pitcher_stats[
                    pitcher_stats["PITCHER_NAME"] == pitcher_name
                ]

                with st.expander(f"🎯 {pitcher_name}", expanded=False):
                    total_pitches = pitcher_df["PITCH_COUNT"].sum()
                    avg_velo = pitcher_df["AVG_VELOCITY"].mean()
                    max_velo = pitcher_df["MAX_VELOCITY"].max()

                    m1, m2, m3 = st.columns(3)
                    m1.metric("Total Pitches", total_pitches)
                    m2.metric("Avg Velocity", f"{avg_velo:.1f} mph")
                    m3.metric("Max Velocity", f"{max_velo:.1f} mph")

                    st.dataframe(
                        pitcher_df[
                            [
                                "PITCH_NAME",
                                "PITCH_COUNT",
                                "AVG_VELOCITY",
                                "MAX_VELOCITY",
                                "AVG_SPIN_RATE",
                                "STRIKE_PCT",
                            ]
                        ],
                        hide_index=True,
                        use_container_width=True,
                    )

    with tab2:
        st.subheader("Strike Zone Visualization")
        pitcher_stats = get_pitcher_stats(game_pk=game_pk)

        if not pitcher_stats.empty:
            pitcher_ids = pitcher_stats[["PITCHER", "PITCHER_NAME"]].drop_duplicates()

            selected_pitcher = st.selectbox(
                "Select Pitcher",
                pitcher_ids.values.tolist(),
                format_func=lambda x: x[1],
            )

            pitch_locations = get_pitch_locations(
                game_pk=game_pk, pitcher_id=selected_pitcher[0]
            )

            if not pitch_locations.empty:
                fig = create_strike_zone_plot(pitch_locations)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No pitch location data available")

    with tab3:
        st.subheader("Velocity Throughout Game")
        velocity_data = get_velocity_over_game(game_pk)

        if not velocity_data.empty:
            fig = create_velocity_chart(velocity_data)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Velocity trend helps identify pitcher fatigue")
        else:
            st.info("No velocity data available")

    st.divider()

    # Raw Data View
    with st.expander("🔍 View Raw Pitch Data"):
        pitches_df = get_game_pitches(game_pk)
        if not pitches_df.empty:
            st.dataframe(pitches_df, use_container_width=True, height=400)
            csv = pitches_df.to_csv(index=False)
            st.download_button(
                "Download CSV", csv, f"game_{game_pk}_pitches.csv", "text/csv"
            )


def page_team_matchup():
    """Team Matchup page - compare two teams head-to-head."""
    st.title("🆚 Team Matchup")
    st.markdown("Compare head-to-head statistics between any two teams")

    # Get all teams
    all_teams = get_all_teams()

    if not all_teams:
        st.warning("No team data available.")
        st.stop()

    col_team1, col_team2 = st.columns(2)

    with col_team1:
        team1 = st.selectbox("Select Team 1", all_teams, index=0, key="team1")

    with col_team2:
        default_idx = min(1, len(all_teams) - 1)
        team2 = st.selectbox("Select Team 2", all_teams, index=default_idx, key="team2")

    if team1 == team2:
        st.warning("⚠️ Please select two different teams to compare.")
        st.stop()

    # Get matchup data
    matchup_games = get_team_matchup_games(team1, team2)

    if matchup_games.empty:
        st.info(
            f"📭 No games found between {team1} and {team2} in the current dataset."
        )
        st.caption(
            "This could mean these teams haven't played each other in the loaded data, or the data is still being backfilled."
        )
        st.stop()

    # Summary stats
    matchup_summary = get_team_matchup_summary(team1, team2)

    if not matchup_summary.empty:
        st.subheader(f"📊 {team1} vs {team2} - Head to Head")

        summary_row = matchup_summary.iloc[0]
        total_games = int(summary_row["TOTAL_GAMES"])
        team1_wins = int(summary_row["TEAM1_WINS"])
        team2_wins = int(summary_row["TEAM2_WINS"])
        team1_runs = (
            int(summary_row["TEAM1_RUNS"]) if pd.notna(summary_row["TEAM1_RUNS"]) else 0
        )
        team2_runs = (
            int(summary_row["TEAM2_RUNS"]) if pd.notna(summary_row["TEAM2_RUNS"]) else 0
        )

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Games", total_games)
        m2.metric(f"{team1} Wins", team1_wins)
        m3.metric(f"{team2} Wins", team2_wins)
        m4.metric(f"{team1} Runs", team1_runs)
        m5.metric(f"{team2} Runs", team2_runs)

        # Win percentage visualization
        if total_games > 0:
            team1_pct = (team1_wins / total_games) * 100
            team2_pct = (team2_wins / total_games) * 100

            fig_record = go.Figure()
            fig_record.add_trace(
                go.Bar(
                    x=[team1_pct],
                    y=[" "],
                    orientation="h",
                    name=f"{team1} ({team1_wins}W)",
                    marker_color="#1f77b4",
                    text=f"{team1}: {team1_wins}W ({team1_pct:.1f}%)",
                    textposition="inside",
                )
            )
            fig_record.add_trace(
                go.Bar(
                    x=[team2_pct],
                    y=[" "],
                    orientation="h",
                    name=f"{team2} ({team2_wins}W)",
                    marker_color="#ff7f0e",
                    text=f"{team2}: {team2_wins}W ({team2_pct:.1f}%)",
                    textposition="inside",
                )
            )
            fig_record.update_layout(
                barmode="stack",
                title="Win Distribution",
                xaxis_title="Win Percentage",
                showlegend=True,
                height=150,
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig_record, use_container_width=True)

    st.divider()

    # Games list
    st.subheader("🗓️ All Games")

    display_games = matchup_games.copy()
    display_games["RESULT"] = display_games.apply(
        lambda r: f"{r['AWAY_TEAM']} {int(r['AWAY_SCORE'])} @ {r['HOME_TEAM']} {int(r['HOME_SCORE'])}",
        axis=1,
    )
    display_games["WINNER"] = display_games.apply(
        lambda r: (
            r["HOME_TEAM"] if r["HOME_SCORE"] > r["AWAY_SCORE"] else r["AWAY_TEAM"]
        ),
        axis=1,
    )

    st.dataframe(
        display_games[
            [
                "GAME_DATE",
                "RESULT",
                "WINNER",
                "TOTAL_PITCHES",
                "PITCHERS_USED",
                "AVG_VELOCITY",
            ]
        ],
        hide_index=True,
        use_container_width=True,
        column_config={
            "GAME_DATE": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "RESULT": "Score",
            "WINNER": "Winner",
            "TOTAL_PITCHES": st.column_config.NumberColumn("Pitches", format="%d"),
            "PITCHERS_USED": st.column_config.NumberColumn("Pitchers", format="%d"),
            "AVG_VELOCITY": st.column_config.NumberColumn(
                "Avg Velo", format="%.1f mph"
            ),
        },
    )

    # Game selector for detailed view
    st.subheader("🔍 Game Details")

    game_options_matchup = []
    for _, game in matchup_games.iterrows():
        date_str = (
            game["GAME_DATE"].strftime("%Y-%m-%d")
            if hasattr(game["GAME_DATE"], "strftime")
            else str(game["GAME_DATE"])
        )
        label = f"{date_str}: {game['AWAY_TEAM']} {int(game['AWAY_SCORE'])} @ {game['HOME_TEAM']} {int(game['HOME_SCORE'])}"
        game_options_matchup.append((game["GAME_PK"], label))

    selected_matchup_game = st.selectbox(
        "Select a game to view details",
        game_options_matchup,
        format_func=lambda x: x[1],
        key="matchup_game_select",
    )

    matchup_game_pk = selected_matchup_game[0]

    # Show game details in tabs
    tab_pitchers, tab_strikezone, tab_pitches = st.tabs(
        ["Pitchers", "Strike Zone", "All Pitches"]
    )

    with tab_pitchers:
        game_pitcher_stats = get_pitcher_stats(game_pk=matchup_game_pk)

        if not game_pitcher_stats.empty:
            for pitcher_name in game_pitcher_stats["PITCHER_NAME"].unique()[:8]:
                pitcher_df = game_pitcher_stats[
                    game_pitcher_stats["PITCHER_NAME"] == pitcher_name
                ]

                with st.expander(f"⚾ {pitcher_name}", expanded=False):
                    total_p = pitcher_df["PITCH_COUNT"].sum()
                    avg_v = pitcher_df["AVG_VELOCITY"].mean()
                    max_v = pitcher_df["MAX_VELOCITY"].max()

                    c1, c2, c3 = st.columns(3)
                    c1.metric("Pitches", total_p)
                    c2.metric("Avg Velo", f"{avg_v:.1f}")
                    c3.metric("Max Velo", f"{max_v:.1f}")

                    st.dataframe(
                        pitcher_df[
                            ["PITCH_NAME", "PITCH_COUNT", "AVG_VELOCITY", "STRIKE_PCT"]
                        ],
                        hide_index=True,
                        use_container_width=True,
                    )
        else:
            st.info("No pitcher stats available for this game.")

    with tab_strikezone:
        game_pitcher_stats = get_pitcher_stats(game_pk=matchup_game_pk)
        if not game_pitcher_stats.empty:
            pitcher_list = (
                game_pitcher_stats[["PITCHER", "PITCHER_NAME"]]
                .drop_duplicates()
                .values.tolist()
            )

            selected_p = st.selectbox(
                "Select Pitcher",
                pitcher_list,
                format_func=lambda x: x[1],
                key="matchup_pitcher_select",
            )

            locations = get_pitch_locations(
                game_pk=matchup_game_pk, pitcher_id=selected_p[0]
            )

            if not locations.empty:
                fig = create_strike_zone_plot(locations)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No pitch location data available.")
        else:
            st.info("No pitcher data available.")

    with tab_pitches:
        game_pitches = get_game_pitches(matchup_game_pk)
        if not game_pitches.empty:
            st.dataframe(game_pitches, use_container_width=True, height=400)

            csv = game_pitches.to_csv(index=False)
            st.download_button(
                "Download CSV",
                csv,
                f"matchup_{team1}_{team2}_game_{matchup_game_pk}.csv",
                "text/csv",
                key="matchup_download",
            )
        else:
            st.info("No pitch data available.")

    # Top pitchers in the matchup
    st.divider()
    st.subheader("🏆 Top Pitchers in This Matchup")

    top_pitchers = get_matchup_top_pitchers(team1, team2)
    if not top_pitchers.empty:
        col_t1, col_t2 = st.columns(2)

        with col_t1:
            st.markdown(f"**{team1} Pitchers**")
            t1_pitchers = top_pitchers[top_pitchers["TEAM"] == team1]
            if not t1_pitchers.empty:
                st.dataframe(
                    t1_pitchers[
                        [
                            "PITCHER_NAME",
                            "GAMES",
                            "TOTAL_PITCHES",
                            "AVG_VELOCITY",
                            "STRIKE_PCT",
                        ]
                    ],
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.caption("No data")

        with col_t2:
            st.markdown(f"**{team2} Pitchers**")
            t2_pitchers = top_pitchers[top_pitchers["TEAM"] == team2]
            if not t2_pitchers.empty:
                st.dataframe(
                    t2_pitchers[
                        [
                            "PITCHER_NAME",
                            "GAMES",
                            "TOTAL_PITCHES",
                            "AVG_VELOCITY",
                            "STRIKE_PCT",
                        ]
                    ],
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.caption("No data")


def page_home():
    """Home page with dashboard overview."""
    st.title("⚾ MLB Statcast Dashboard")
    st.markdown("Real-time pitch-by-pitch analytics powered by Snowflake")

    # Get data summary
    summary = get_data_summary()

    if summary.empty:
        st.warning("No data found in STATCAST table. Is the backfill running?")
        st.stop()

    show_data_overview(summary)

    st.divider()

    st.markdown(
        """
    ### 📖 How to Use This Dashboard
    
    Use the **sidebar navigation** to explore different features:
    
    - **🏠 Home** - Overview of all data in the system
    - **🎯 Game Explorer** - Browse games by date, view pitcher stats, strike zones, and velocity trends
    - **🆚 Team Matchup** - Compare head-to-head records between any two teams
    - **🎬 Live Game Simulation** - Watch any game unfold pitch-by-pitch
    
    ### 📊 Quick Stats
    """
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Recent Games**")
        available_dates, min_date, max_date = get_available_dates()
        if available_dates:
            recent_date = max_date
            recent_games = get_games_for_date(str(recent_date))
            if not recent_games.empty:
                st.caption(f"Games on {recent_date}")
                for _, game in recent_games.head(5).iterrows():
                    st.write(
                        f"• {game['AWAY_TEAM']} @ {game['HOME_TEAM']}: {int(game['AWAY_SCORE'])}-{int(game['HOME_SCORE'])}"
                    )

    with col2:
        st.markdown("**Available Teams**")
        teams = get_all_teams()
        if teams:
            st.caption(f"{len(teams)} teams in database")
            st.write(", ".join(teams[:10]) + ("..." if len(teams) > 10 else ""))


# =============================================================================
# LIVE GAME SIMULATION PAGE
# =============================================================================


def get_game_pitches_ordered(game_pk):
    """Get all pitches for a game in chronological order with full details."""
    query = f"""
    SELECT 
        "game_pk" as GAME_PK,
        "game_date" as GAME_DATE,
        "inning" as INNING,
        "inning_topbot" as INNING_TOPBOT,
        "at_bat_number" as AT_BAT_NUMBER,
        "pitch_number" as PITCH_NUMBER,
        "pitcher" as PITCHER_ID,
        "player_name" as PITCHER_NAME,
        "batter" as BATTER_ID,
        "pitch_type" as PITCH_TYPE,
        "pitch_name" as PITCH_NAME,
        "release_speed" as RELEASE_SPEED,
        "release_spin_rate" as SPIN_RATE,
        "pfx_x" as PFX_X,
        "pfx_z" as PFX_Z,
        "plate_x" as PLATE_X,
        "plate_z" as PLATE_Z,
        "zone" as ZONE,
        "type" as PITCH_RESULT,
        "events" as EVENTS,
        "description" as DESCRIPTION,
        "balls" as BALLS,
        "strikes" as STRIKES,
        "outs_when_up" as OUTS,
        "home_score" as HOME_SCORE,
        "away_score" as AWAY_SCORE,
        "home_team" as HOME_TEAM,
        "away_team" as AWAY_TEAM,
        "sz_top" as SZ_TOP,
        "sz_bot" as SZ_BOT
    FROM STATCAST
    WHERE "game_pk" = {game_pk}
    ORDER BY "inning", 
             CASE WHEN "inning_topbot" = 'Top' THEN 0 ELSE 1 END,
             "at_bat_number", 
             "pitch_number"
    """
    return run_query(query)


def create_live_strike_zone(pitches_df, current_idx):
    """Create animated strike zone showing pitches up to current index."""
    if pitches_df.empty or current_idx < 0:
        fig = go.Figure()
        # Add empty strike zone
        fig.add_shape(
            type="rect",
            x0=-0.83,
            y0=1.5,
            x1=0.83,
            y1=3.5,
            line=dict(color="black", width=2),
        )
        fig.update_layout(
            xaxis=dict(range=[-2.5, 2.5], scaleanchor="y"),
            yaxis=dict(range=[0, 5]),
            height=400,
            title="Strike Zone",
        )
        return fig

    # Get pitches up to current index
    display_df = pitches_df.iloc[: current_idx + 1].copy()
    display_df = display_df[
        display_df["PLATE_X"].notna() & display_df["PLATE_Z"].notna()
    ]

    if display_df.empty:
        fig = go.Figure()
        fig.add_shape(
            type="rect",
            x0=-0.83,
            y0=1.5,
            x1=0.83,
            y1=3.5,
            line=dict(color="black", width=2),
        )
        fig.update_layout(
            xaxis=dict(range=[-2.5, 2.5], scaleanchor="y"),
            yaxis=dict(range=[0, 5]),
            height=400,
            title="Strike Zone",
        )
        return fig

    # Color map
    color_map = {"S": "red", "B": "blue", "X": "green"}

    fig = go.Figure()

    # Strike zone rectangle
    fig.add_shape(
        type="rect",
        x0=-0.83,
        y0=1.5,
        x1=0.83,
        y1=3.5,
        line=dict(color="black", width=2),
        fillcolor="rgba(0,0,0,0)",
    )

    # Home plate
    fig.add_shape(
        type="path",
        path="M -0.83 0 L 0.83 0 L 0.83 0.25 L 0 0.5 L -0.83 0.25 Z",
        line=dict(color="black", width=1),
        fillcolor="white",
    )

    # Previous pitches (smaller, faded)
    if len(display_df) > 1:
        prev_df = display_df.iloc[:-1]
        for result_type in prev_df["PITCH_RESULT"].unique():
            type_df = prev_df[prev_df["PITCH_RESULT"] == result_type]
            fig.add_trace(
                go.Scatter(
                    x=type_df["PLATE_X"],
                    y=type_df["PLATE_Z"],
                    mode="markers",
                    marker=dict(
                        color=color_map.get(result_type, "gray"), size=6, opacity=0.3
                    ),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

    # Current pitch (larger, highlighted)
    current_pitch = display_df.iloc[-1]
    result_type = current_pitch["PITCH_RESULT"]
    fig.add_trace(
        go.Scatter(
            x=[current_pitch["PLATE_X"]],
            y=[current_pitch["PLATE_Z"]],
            mode="markers",
            marker=dict(
                color=color_map.get(result_type, "gray"),
                size=18,
                opacity=1,
                line=dict(color="white", width=2),
            ),
            name="Current Pitch",
            hovertemplate=(
                f"<b>{current_pitch['PITCH_NAME']}</b><br>"
                f"Velocity: {current_pitch['RELEASE_SPEED']:.1f} mph<br>"
                f"Result: {current_pitch['DESCRIPTION']}<br>"
                "<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        xaxis=dict(range=[-2.5, 2.5], scaleanchor="y", title=""),
        yaxis=dict(range=[0, 5], title=""),
        height=350,
        margin=dict(l=20, r=20, t=30, b=20),
        showlegend=False,
    )

    return fig


def create_velocity_tracker(pitches_df, current_idx):
    """Create velocity chart showing progression up to current pitch."""
    if pitches_df.empty or current_idx < 0:
        return go.Figure()

    display_df = pitches_df.iloc[: current_idx + 1].copy()
    display_df = display_df[display_df["RELEASE_SPEED"].notna()]

    if display_df.empty:
        return go.Figure()

    display_df["PITCH_IDX"] = range(1, len(display_df) + 1)

    fig = px.scatter(
        display_df,
        x="PITCH_IDX",
        y="RELEASE_SPEED",
        color="PITCH_NAME",
        hover_data=["PITCHER_NAME", "DESCRIPTION"],
    )

    fig.update_layout(
        xaxis_title="Pitch #",
        yaxis_title="Velocity (mph)",
        height=250,
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )

    return fig


def create_pitch_count_display(balls, strikes, outs):
    """Create HTML for pitch count display."""

    def ball_indicator(filled):
        color = "#2ecc71" if filled else "#bdc3c7"
        return f'<span style="display:inline-block;width:20px;height:20px;border-radius:50%;background:{color};margin:2px;"></span>'

    def strike_indicator(filled):
        color = "#e74c3c" if filled else "#bdc3c7"
        return f'<span style="display:inline-block;width:20px;height:20px;border-radius:50%;background:{color};margin:2px;"></span>'

    def out_indicator(filled):
        color = "#f39c12" if filled else "#bdc3c7"
        return f'<span style="display:inline-block;width:20px;height:20px;border-radius:50%;background:{color};margin:2px;"></span>'

    balls_html = "".join([ball_indicator(i < balls) for i in range(4)])
    strikes_html = "".join([strike_indicator(i < strikes) for i in range(3)])
    outs_html = "".join([out_indicator(i < outs) for i in range(3)])

    return f"""
    <div style="font-family: monospace; font-size: 14px;">
        <div><b>B</b> {balls_html}</div>
        <div><b>S</b> {strikes_html}</div>
        <div><b>O</b> {outs_html}</div>
    </div>
    """


def page_live_simulation():
    """
    Live Game Simulation page - powered by Kafka + Spark Streaming.

    Architecture:
    1. User selects a game and clicks Play
    2. Dashboard calls FastAPI to start simulation_producer
    3. simulation_producer reads from Snowflake, publishes to Kafka
    4. Spark Streaming consumes from Kafka, writes to SIMULATION_PITCHES
    5. Dashboard polls SIMULATION_PITCHES table to display live updates

    Fallback: If Kafka/API not available, uses local simulation mode.
    """
    st.title("🎬 Live Game Simulation")

    # Check if Kafka simulation API is available
    kafka_available, api_status = is_kafka_available()

    # Mode selection
    if kafka_available:
        st.success("🔌 Connected to Kafka Streaming Pipeline")
        mode = "kafka"
    else:
        st.warning("⚠️ Kafka API not available. Using local simulation mode.")
        st.caption(
            "To enable Kafka streaming: `uvicorn streaming.simulation_producer:app --port 8000`"
        )
        mode = "local"

    st.markdown("Watch any historical game unfold pitch-by-pitch")

    # Initialize session state
    if "sim_running" not in st.session_state:
        st.session_state.sim_running = False
    if "sim_pitch_idx" not in st.session_state:
        st.session_state.sim_pitch_idx = -1
    if "sim_game_pk" not in st.session_state:
        st.session_state.sim_game_pk = None
    if "sim_pitches" not in st.session_state:
        st.session_state.sim_pitches = None
    if "sim_mode" not in st.session_state:
        st.session_state.sim_mode = mode
    if "sim_speed" not in st.session_state:
        st.session_state.sim_speed = 1.0

    # Sidebar controls
    st.sidebar.markdown("### ⚙️ Simulation Settings")

    if mode == "kafka":
        speed = st.sidebar.slider(
            "Pitches per Second",
            min_value=0.5,
            max_value=5.0,
            value=st.session_state.sim_speed,
            step=0.5,
            help="How many pitches to show per second (e.g., 1 = one pitch every second, 2 = two pitches per second)",
        )
        if speed != st.session_state.sim_speed:
            st.session_state.sim_speed = speed
            if st.session_state.sim_running:
                update_simulation_speed(speed)
    else:
        # Local mode - use same pitches per second metric
        speed = st.sidebar.slider(
            "Pitches per Second",
            min_value=0.5,
            max_value=5.0,
            value=st.session_state.sim_speed,
            step=0.5,
            help="How many pitches to show per second",
        )
        st.session_state.sim_speed = speed

    show_velocity = st.sidebar.checkbox("Show Velocity Chart", value=True)
    show_pitch_log = st.sidebar.checkbox("Show Pitch Log", value=True)

    st.sidebar.markdown("---")

    # Show Kafka status in sidebar if connected
    if kafka_available and api_status:
        st.sidebar.markdown("### 📊 Stream Status")
        if api_status.get("is_running"):
            progress = api_status.get("progress_pct", 0)
            st.sidebar.progress(progress / 100, text=f"Progress: {progress:.1f}%")
            st.sidebar.caption(
                f"Pitch {api_status.get('current_pitch', 0)} / {api_status.get('total_pitches', 0)}"
            )
            if api_status.get("is_paused"):
                st.sidebar.warning("⏸️ Paused")
            else:
                st.sidebar.success("▶️ Playing")
        else:
            st.sidebar.info("Ready to stream")

    st.sidebar.markdown("---")

    # Game Selection
    st.subheader("📅 Select a Game")

    col_date, col_game = st.columns([1, 2])

    with col_date:
        available_dates, min_date, max_date = get_available_dates()
        if not available_dates:
            st.warning("No games found in database.")
            st.stop()

        selected_date = st.date_input(
            "Game Date", value=max_date, min_value=min_date, max_value=max_date
        )

    with col_game:
        games_df = get_games_for_date(str(selected_date))

        if games_df.empty:
            st.info(f"No games on {selected_date}")
            st.stop()

        game_options = []
        for _, game in games_df.iterrows():
            label = f"{game['AWAY_TEAM']} @ {game['HOME_TEAM']} ({int(game['AWAY_SCORE'])}-{int(game['HOME_SCORE'])}) - {game['TOTAL_PITCHES']} pitches"
            game_options.append((game["GAME_PK"], label))

        selected_game = st.selectbox(
            "Select Game", game_options, format_func=lambda x: x[1]
        )

    game_pk = selected_game[0]

    # Load game data if changed
    if st.session_state.sim_game_pk != game_pk:
        st.session_state.sim_game_pk = game_pk
        st.session_state.sim_pitches = get_game_pitches_ordered(game_pk)
        st.session_state.sim_pitch_idx = -1
        st.session_state.sim_running = False
        # Stop any running Kafka simulation when game changes
        if kafka_available:
            stop_kafka_simulation()

    pitches_df = st.session_state.sim_pitches

    if pitches_df is None or pitches_df.empty:
        st.warning("No pitch data available for this game.")
        st.stop()

    total_pitches = len(pitches_df)

    # Game info header
    first_pitch = pitches_df.iloc[0]
    away_team = first_pitch["AWAY_TEAM"]
    home_team = first_pitch["HOME_TEAM"]

    st.markdown(f"### {away_team} @ {home_team}")
    mode_label = "🔴 Kafka Stream" if mode == "kafka" else "💻 Local"
    st.caption(
        f"Game PK: {game_pk} | Total Pitches: {total_pitches} | Mode: {mode_label}"
    )

    st.divider()

    # Control buttons
    col_ctrl1, col_ctrl2, col_ctrl3, col_ctrl4, col_ctrl5 = st.columns(5)

    with col_ctrl1:
        if st.button("⏮️ Reset", use_container_width=True):
            st.session_state.sim_pitch_idx = -1
            st.session_state.sim_running = False
            if kafka_available:
                stop_kafka_simulation()
            st.rerun()

    with col_ctrl2:
        if st.button("⏪ -10", use_container_width=True):
            st.session_state.sim_pitch_idx = max(
                -1, st.session_state.sim_pitch_idx - 10
            )
            st.rerun()

    with col_ctrl3:
        if st.session_state.sim_running:
            if st.button("⏸️ Pause", use_container_width=True):
                st.session_state.sim_running = False
                if kafka_available:
                    pause_kafka_simulation()
                st.rerun()
        else:
            if st.button("▶️ Play", use_container_width=True):
                st.session_state.sim_running = True
                if kafka_available and mode == "kafka":
                    start_kafka_simulation(game_pk, st.session_state.sim_speed)
                st.rerun()

    with col_ctrl4:
        if st.button("⏩ +10", use_container_width=True):
            st.session_state.sim_pitch_idx = min(
                total_pitches - 1, st.session_state.sim_pitch_idx + 10
            )
            st.rerun()

    with col_ctrl5:
        if st.button("⏭️ End", use_container_width=True):
            st.session_state.sim_pitch_idx = total_pitches - 1
            st.session_state.sim_running = False
            if kafka_available:
                stop_kafka_simulation()
            st.rerun()

    # Progress slider
    pitch_idx = st.slider(
        "Pitch Progress",
        min_value=0,
        max_value=total_pitches,
        value=st.session_state.sim_pitch_idx + 1,
        format="Pitch %d",
    )

    if pitch_idx - 1 != st.session_state.sim_pitch_idx:
        st.session_state.sim_pitch_idx = pitch_idx - 1
        st.session_state.sim_running = False

    current_idx = st.session_state.sim_pitch_idx

    # In Kafka mode, we no longer sync with producer's pitch index
    # The dashboard controls its own pace to avoid skipping pitches due to slow refresh
    # The producer still runs (for Spark/other consumers) but dashboard is independent
    if mode == "kafka" and st.session_state.sim_running:
        # Just check if simulation is still active on the server
        status = get_simulation_status()
        if status and not status.get("is_running") and not status.get("is_paused"):
            # Producer stopped unexpectedly, but we keep going locally
            pass

    st.divider()

    # Main display area
    if current_idx >= 0:
        current_pitch = pitches_df.iloc[current_idx]

        # Scoreboard and current pitch info
        col_score, col_count, col_pitch = st.columns([2, 1, 3])

        with col_score:
            st.markdown("#### 📊 Scoreboard")
            inning = int(current_pitch["INNING"])
            half = "▲" if current_pitch["INNING_TOPBOT"] == "Top" else "▼"
            home_score = (
                int(current_pitch["HOME_SCORE"])
                if pd.notna(current_pitch["HOME_SCORE"])
                else 0
            )
            away_score = (
                int(current_pitch["AWAY_SCORE"])
                if pd.notna(current_pitch["AWAY_SCORE"])
                else 0
            )

            st.markdown(
                f"""
            <div style="font-size: 24px; font-family: monospace;">
                <b>{half} {inning}</b>
            </div>
            <div style="font-size: 20px;">
                {away_team}: <b>{away_score}</b><br>
                {home_team}: <b>{home_score}</b>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col_count:
            st.markdown("#### Count")
            balls = (
                int(current_pitch["BALLS"]) if pd.notna(current_pitch["BALLS"]) else 0
            )
            strikes = (
                int(current_pitch["STRIKES"])
                if pd.notna(current_pitch["STRIKES"])
                else 0
            )
            outs = int(current_pitch["OUTS"]) if pd.notna(current_pitch["OUTS"]) else 0
            st.markdown(
                create_pitch_count_display(balls, strikes, outs), unsafe_allow_html=True
            )

        with col_pitch:
            st.markdown("#### ⚾ Current Pitch")

            velo = current_pitch["RELEASE_SPEED"]
            velo_str = f"{velo:.1f} mph" if pd.notna(velo) else "N/A"

            spin = current_pitch["SPIN_RATE"]
            spin_str = f"{int(spin)} rpm" if pd.notna(spin) else "N/A"

            st.markdown(
                f"""
            **Pitcher:** {current_pitch['PITCHER_NAME']}  
            **Pitch:** {current_pitch['PITCH_NAME']} ({current_pitch['PITCH_TYPE']})  
            **Velocity:** {velo_str}  
            **Spin:** {spin_str}  
            **Result:** {current_pitch['DESCRIPTION']}
            """
            )

            if pd.notna(current_pitch["EVENTS"]) and current_pitch["EVENTS"]:
                st.success(f"🎯 **{current_pitch['EVENTS']}**")

        st.divider()

        # Visualizations
        col_zone, col_data = st.columns([1, 1])

        with col_zone:
            st.markdown("#### Strike Zone")
            zone_fig = create_live_strike_zone(pitches_df, current_idx)
            st.plotly_chart(zone_fig, use_container_width=True)

        with col_data:
            if show_velocity:
                st.markdown("#### Velocity Tracker")
                velo_fig = create_velocity_tracker(pitches_df, current_idx)
                st.plotly_chart(velo_fig, use_container_width=True)

        # Pitch log
        if show_pitch_log:
            with st.expander("📋 Pitch Log", expanded=False):
                log_df = pitches_df.iloc[: current_idx + 1][
                    [
                        "INNING",
                        "INNING_TOPBOT",
                        "PITCHER_NAME",
                        "PITCH_NAME",
                        "RELEASE_SPEED",
                        "DESCRIPTION",
                        "EVENTS",
                    ]
                ].copy()
                log_df = log_df.iloc[::-1]  # Reverse to show most recent first
                st.dataframe(log_df.head(20), use_container_width=True, hide_index=True)

    else:
        # No pitch selected yet
        st.info("👆 Press **Play** or use the slider to start the simulation")

        # Show game summary
        st.markdown("#### Game Preview")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Pitches", total_pitches)
        col2.metric("Pitchers", pitches_df["PITCHER_NAME"].nunique())
        col3.metric("Innings", pitches_df["INNING"].max())

    # Auto-advance logic
    if st.session_state.sim_running and current_idx < total_pitches - 1:
        # Both modes now work the same way - dashboard controls its own pace
        # Speed = pitches per second, delay = 1/speed
        delay = 1.0 / st.session_state.sim_speed
        time.sleep(delay)
        st.session_state.sim_pitch_idx += 1
        st.rerun()
    elif st.session_state.sim_running and current_idx >= total_pitches - 1:
        st.session_state.sim_running = False
        if kafka_available:
            stop_kafka_simulation()
        st.success("🎉 Game Complete!")


# =============================================================================
# MAIN APP
# =============================================================================


def main():
    # Check connection first
    conn = get_snowflake_connection()
    if conn is None:
        st.error("Cannot connect to Snowflake. Please check your credentials.")
        st.stop()

    # Sidebar Navigation
    st.sidebar.title("⚾ MLB Statcast")
    st.sidebar.markdown("---")

    page = st.sidebar.radio(
        "Navigation",
        ["🏠 Home", "🎯 Game Explorer", "🆚 Team Matchup", "🎬 Live Simulation"],
        label_visibility="collapsed",
    )

    st.sidebar.markdown("---")

    # Auto-refresh toggle (only for non-simulation pages)
    if page != "🎬 Live Simulation":
        auto_refresh = st.sidebar.checkbox("Auto-refresh (60s)", value=False)
        if auto_refresh:
            st.sidebar.info("Dashboard will refresh every 60 seconds")

    st.sidebar.markdown("---")
    st.sidebar.caption("Data: MLB Statcast via pybaseball")
    st.sidebar.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")

    # Route to appropriate page
    if page == "🏠 Home":
        page_home()
    elif page == "🎯 Game Explorer":
        page_game_explorer()
    elif page == "🆚 Team Matchup":
        page_team_matchup()
    elif page == "🎬 Live Simulation":
        page_live_simulation()


if __name__ == "__main__":
    main()
