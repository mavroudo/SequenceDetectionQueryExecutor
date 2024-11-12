import sys
import pm4py
import pandas as pd
from pm4py.objects.log.obj import EventLog, Trace, Event

# XES to sorted XES conversion
inputFilePath = sys.argv[1]
outputFilePath = str(inputFilePath.split('/')[-1].split('.')[0]) + "_sorted.xes"

# Load the XES file
log = pm4py.read_xes(inputFilePath)

# Initialize an empty EventLog to hold sorted traces
sorted_log = EventLog()

# Process each trace in the original log
for trace in log:
    # Convert trace to a DataFrame for sorting
    trace_df = pd.DataFrame(
        {
            "Activity": [event["concept:name"] for event in trace],
            "Complete Timestamp": [event["time:timestamp"] for event in trace]
        }
    )
    # Sort the events by timestamp
    trace_df = trace_df.sort_values(by="Complete Timestamp").reset_index(drop=True)

    # Create a new sorted Trace with the original trace attributes
    sorted_trace = Trace(attributes=trace.attributes)

    # Add sorted events back to the trace
    for _, row in trace_df.iterrows():
        event = Event()
        event["concept:name"] = row["Activity"]
        event["time:timestamp"] = row["Complete Timestamp"]
        sorted_trace.append(event)

    # Append the sorted trace to the new EventLog
    sorted_log.append(sorted_trace)

# Save the sorted EventLog to an XES file
pm4py.write_xes(sorted_log, outputFilePath)
