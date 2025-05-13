import os
import sys
import pm4py
import pandas as pd
from datetime import datetime

dataset = sys.argv[1]
print(dataset)
# Load the XES file
inputFilePath = "xes_datasets/" + dataset + ".xes"
print(inputFilePath)
outputFilePath = "ts_datasets/" + dataset + ".withTimestamp"
log = pm4py.read_xes(inputFilePath)
df = pm4py.convert_to_dataframe(log)

# Rename columns to a standard format
df = df.rename(columns={'case:concept:name': 'case_id', 'concept:name': 'activity', 'time:timestamp': 'timestamp'})
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Write to the required format
with open(outputFilePath, 'w') as f:
    for trace_id, group in df.groupby('case_id'):
        # Combine activities and timestamps into the required format
        activities_with_timestamps = [
            f"{row['activity']}/delab/{row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}"
            for _, row in group.iterrows()
        ]
        # Join the activities with timestamps into a single string
        activities_str = ",".join(activities_with_timestamps)
        # Write the formatted line to the file
        f.write(f"{trace_id}::{activities_str}\n")

print(f"DataFrame successfully written to {outputFilePath}")
