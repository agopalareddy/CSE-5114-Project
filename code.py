import pybaseball as pyb
import pandas as pd
import warnings
import os

# Suppress FutureWarnings from pybaseball's postprocessing
warnings.filterwarnings("ignore", category=FutureWarning, module="pybaseball")

# Set pandas to show all columns
pd.set_option("display.max_columns", None)

# CONFIGURATIONS
export_dir = "exports"
eda_dir = "eda"
docs_dir = "docs"

# --- RE-RUN YOUR CODE TO GET DATA ---
data = pyb.statcast(start_dt="2024-07-01", end_dt="2024-07-02")

# Drop these columns as they are empty
data = data.drop(
    columns=[
        "spin_dir",
        "spin_rate_deprecated",
        "break_angle_deprecated",
        "break_length_deprecated",
        "tfs_deprecated",
        "tfs_zulu_deprecated",
        "umpire",
        "sv_id",
    ]
)

# export column names to statcast_columns_from_fetch.txt
with open(os.path.join(docs_dir, "statcast_columns_from_fetch.txt"), "w") as f:
    f.write("\n".join(data.columns))

# Exploratory Data Analysis (EDA)
os.makedirs(eda_dir, exist_ok=True)
with open(os.path.join(eda_dir, "zzz.txt"), "w") as f:
    f.write(str(data.info()))

data.describe().to_csv(os.path.join(eda_dir, "describe.csv"))
data.head().to_csv(os.path.join(eda_dir, "head.csv"))
data.tail().to_csv(os.path.join(eda_dir, "tail.csv"))
# pd.DataFrame(data.columns, columns=["columns"]).to_csv(
#     os.path.join(eda_dir, "columns.csv")
# ) # Not needed since columns are already documented
