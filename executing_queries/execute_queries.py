#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 17 19:32:51 2020

@author: mavroudo
"""
import requests
import sys
import time
from datetime import datetime

url ="http://localhost:8080/api/funnel/quick_stats"

url2 ="http://localhost:8080/api/funnel/detect"

url2 ="http://localhost:8080/api/funnel/explore/accurate"
def create_query(steps,log_name,max_duration=0):
    #create steps
    #add max duration optional
    #add logfile name
    steps_json=[]
    for step in steps:
        step_dict=dict()
        step_dict["match_name"]=[{"log_name":step}]
        step_dict["match_details"]=[]
        steps_json.append(step_dict)
    final_json=dict()
    final_json["funnel"]=dict()
    final_json["funnel"]["steps"]=steps_json
    final_json["funnel"]["max_duration"]=max_duration
    final_json["funnel"]["log_name"]=log_name
    return final_json

def execute_query(steps,log_name):
    resp=requests.post(url=url,json=create_query(steps,log_name))
    return resp.ok
    
def read_sequence(log_file):
    if log_file.split(".")[1]=="withTimestamp":
        data=[]
        with open(logfile,"r") as f:
            for l in f:
                d=[]
                t=[]
                for ev in l.replace("\n","").split(","):
                    d+=[ev.split("/delab/")[0]]
                    
                    t+=[datetime.strptime(ev.split("/delab/")[1], "%Y-%m-%d %H:%M:%S")]
                data.append([x for _, x in sorted(zip(t,d), key=lambda pair: pair[0])])
        return [(i,",".join(d)) for i,d in enumerate(data)]
#[log[3640][i]["concept:name"] for i in range(len(log[3640]))]
#steps=["W_Assess potential fraud","O_Refused","O_Created","O_Returned","A_Incomplete"]
#steps=["O_Created","A_Incomplete","O_Returned"]
steps=["V1TEB7JW","6VSH0CAX","V1TEB7JW"]
logfile="ten.withTimestamp"
#log_name="BPI Challenge 2017.xes"

if __name__ == "__main__":  
    arguments=sys.argv
    logfile=arguments[1] 
    count=0
    with open(logfile,"r") as f:
        for line in f:
            steps=line.replace("\n","").split(",")
            executed=execute_query(steps,logfile)
            time.sleep(2)
            count+=1
            if count==20:
                break
            if not executed:
                print(steps)

