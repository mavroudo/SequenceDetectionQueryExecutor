import os

import pm4py
import sys
import pandas as pd

# xes to csv conversion
inputFilePath = sys.argv[1]
outputFilePath = str(inputFilePath.split('/')[-1].split('.')[0]) + "_sorted.csv"
# outputFilePath =
log = pm4py.read_xes(inputFilePath)
df = pm4py.convert_to_dataframe(log)
# df = df.rename(columns={'case:concept:name': 'Case ID', 'concept:name': 'Activity', 'time:timestamp': 'Complete Timestamp'})
df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
df['time:timestamp'] = df['time:timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
df_sorted = df.sort_values(by=['case:concept:name', 'time:timestamp']).reset_index(drop=True)

df_sorted.to_csv(outputFilePath, index=False)

# df.to_csv(outputFilePath)
##############################################
# df = pd.read_csv(inputFilePath)
# df = df.rename(columns={'caseType': 'Case ID', 'concept:name': 'Activity', 'time:timestamp': 'Complete Timestamp'})
# df.to_csv(inputFilePath, index=False)
# df = pd.read_csv("/home/antonis/PycharmProjects/ProcessTransformerZaharah/datasets/finale.csv")
# df['Complete Timestamp'] = df['Complete Timestamp'].str.replace('/','-')
# datetime_format = "%Y-%m-%d %H:%M:%S.%f"
#
# # Assuming df is your DataFrame and 'Complete Timestamp' is the column name
# df['Complete Timestamp'] = pd.to_datetime(df['Complete Timestamp'])
# df['Complete Timestamp'] = df['Complete Timestamp'].dt.strftime('%Y-%d-%m %H:%M:%S.%f')
#
# # df = df.rename(columns = {'time:timestamp': 'Complete Timestamp', 'caseType': 'Case ID', 'diagnosis': 'Activity'})


# Taken from https://www.kaggle.com/code/lucaspoo/xes-to-csv/notebook
