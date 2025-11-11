from pybaseball import statcast
import pandas as pd
from datetime import datetime, timedelta
from pybaseball import playerid_lookup
from pybaseball import statcast_pitcher
from pybaseball import statcast_batter
from pybaseball import pitching_stats
from pybaseball import batting_stats


# --- Configuration ---
# Set the start and end dates for the season you want
# Example: 2024 regular season
# START_DATE = "2024-03-28"
START_DATE = "2025-07-28"
END_DATE = "2025-07-29"
SEASON_YEAR = 2025

# --- Main Script ---
def get_daily_statcast_data(start_str, end_str):
    """
    Fetches Statcast data for a given date range, looping day-by-day
    to avoid timeouts from the MLB API.
    """
    
    # Convert string dates to datetime objects
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    
    print(f"Starting Statcast pull for {start_str} to {end_str}...")
    
    all_daily_data = []  # List to hold each day's DataFrame
    current_dt = start_dt
    
    while current_dt <= end_dt:
        day_str = current_dt.strftime("%Y-%m-%d")
        print(f"Fetching data for: {day_str}")
        
        try:
            # This is the core pybaseball function
            daily_data = pybaseball.statcast(start_dt=day_str, end_dt=day_str)
            
            if not daily_data.empty:
                all_daily_data.append(daily_data)
                print(f"  -> Success: Found {len(daily_data)} pitches.")
            else:
                print(f"  -> No games on this day.")
                
        except Exception as e:
            print(f"  -> ERROR fetching data for {day_str}: {e}")
        
        # Move to the next day
        current_dt += timedelta(days=1)
        
    print("...All days processed. Concatenating DataFrames...")
    
    # Combine all the daily DataFrames into one large DataFrame
    if not all_daily_data:
        print("No data found for the entire date range.")
        return None
        
    season_df = pd.concat(all_daily_data, ignore_index=True)
    return season_df

def get_pitcher_data(START_DATE, END_DATE)
    pitcher_data = statcast_pitcher(start_dt=start_str, end_dt=end_str, id=player_id)
    return pitcher_data

def get_batter_data(START_DATE, END_DATE):
    batter_data = statcast_batter(start_dt=start_str, end_dt=end_str)
    return batter_data

if __name__ == "__main__":
    # season_data = get_daily_statcast_data(START_DATE, END_DATE)

    print(statcast(start_dt=START_DATE, end_dt=END_DATE).columns)
    
    # if season_data is not None:
    #     print(f"\nTotal pitches downloaded for {SEASON_YEAR}: {len(season_data)}")
        
    #     # --- Save the Data ---
    #     # We save to Parquet because it's *much* faster for Spark to read
    #     OUTPUT_FILE_PARQUET = f"statcast_{SEASON_YEAR}.parquet"
    #     season_data.to_parquet(OUTPUT_FILE_PARQUET, index=False)
    #     print(f"Successfully saved data to {OUTPUT_FILE_PARQUET}")

        # You can also save to CSV, but it will be slower and larger
        # OUTPUT_FILE_CSV = f"statcast_{SEASON_YEAR}.csv"
        # season_data.to_csv(OUTPUT_FILE_CSV, index=False)
        # print(f"Successfully saved data to {OUTPUT_FILE_CSV}")