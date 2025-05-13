import pm4py
import sys
import pandas as pd

inputFilePath = sys.argv[1]
outputFilePath = str(inputFilePath.split('/')[-1].split('.')[0]) + ".xes"
dataframe = pd.read_csv(inputFilePath, sep = ',')
print(dataframe)
dataframe['time:timestamp'] = pd.to_datetime(dataframe['time:timestamp'])
dataframe = pm4py.format_dataframe(dataframe, case_id='case:concept:name', activity_key='concept:name', timestamp_key='time:timestamp')
event_log = pm4py.convert_to_event_log(dataframe)
pm4py.write_xes(event_log, outputFilePath)


# Taken from https://pm4py.fit.fraunhofer.de/documentation