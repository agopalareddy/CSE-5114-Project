# MLB Game Simulation Dashboard

A lambda architecture-based analytics dashboard for Major League Baseball that enables historical game simulation, pitcher analytics, and team matchup analysis.

## Features

- **Game Simulation**: Replay historical games pitch-by-pitch with configurable playback speeds (0.5–5 pitches/second)
- **Game Explorer**: Analyze pitcher statistics including velocity, spin rate, and strikeout percentage
- **Team Matchups**: Compare head-to-head statistics between teams across seasons
- **Strike Zone Visualizations**: Interactive pitch location displays

## Tech Stack

- **Data Warehouse**: Snowflake
- **Stream Processing**: Apache Kafka, Apache Spark Streaming
- **Orchestration**: Apache Airflow
- **Dashboard**: Streamlit
- **Data Source**: pybaseball (Statcast data)

## Project Structure

```
├── dags/           # Airflow DAGs for batch processing
├── dashboard/      # Streamlit dashboard application
├── streaming/      # Kafka producer and Spark consumer
├── sql/            # Database schema and queries
├── docs/           # Documentation and reports
└── eda/            # Exploratory data analysis notebooks
```

## Setup

1. Clone the repository
2. Copy `.env.sample` to `.env` and configure your Snowflake credentials
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Start the Kafka and Zookeeper containers (see `streaming/` for Docker setup)
5. Run Airflow DAGs to populate historical data
6. Launch the dashboard:
   ```bash
   streamlit run dashboard/app.py
   ```

## Demo

- **Video**: [YouTube Demo](https://www.youtube.com/watch?v=z01fHvzd-8w)

## Authors

- Aadarsha Gopala Reddy
- Eddy Sul

## Course

CSE 5114: Data Manipulation and Management at Scale - Washington University in St. Louis
