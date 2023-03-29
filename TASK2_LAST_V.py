#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import os
import time
import datetime
from urllib.parse import urlsplit
import argparse

def transform_to_csv(directory_path, unix_time=False):
    
    json_files = [f for f in os.listdir(directory_path) if f.endswith(".json")]
    start_time = time.time()

    for json_file in json_files:
        
        df = pd.read_json(os.path.join(directory_path, json_file), lines=True)

        
        df = df[["a", "tz", "r", "u", "t", "hc", "cy", "ll"]]

       
        df = df.rename(columns={
            "a": "web_browser",
            "tz": "time_zone",
            "r": "from_url",
            "u": "to_url",
            "t": "time_in",
            "hc": "time_out",
            "cy": "city",
            "ll": "latlong",
        })

        
        df['short_url'] = df['from_url'].apply(lambda x: urlsplit(x).hostname)
        df = df.drop(columns=['from_url'])
        df = df.rename(columns={'short_url': 'from_url'})

        
        df['short_url'] = df['to_url'].apply(lambda x: urlsplit(x).hostname)
        df = df.drop(columns=['to_url'])
        df = df.rename(columns={'short_url': 'to_url'})

        
        df[["latitude", "longitude"]] = df["latlong"].apply(pd.Series)
        df = df.drop(columns=["latlong"])


        
        df['operating_sys'] = df['web_browser'].apply(lambda x: x.split()[0])

       
        df = df[["web_browser", "operating_sys", "from_url", "to_url", "city", "longitude", "latitude", "time_zone", "time_in", "time_out"]]

        
        df = df.dropna()
        
        if unix_time == False:
            df['time_in'] = pd.to_datetime(df['time_in'], unit='s')       
            df['time_out'] = pd.to_datetime(df['time_out'], unit='s')
        
        checksums = df.apply(lambda row: hash(tuple(row)), axis=1)
        df["checksum"] = checksums

        
        duplicates = df[df.duplicated(subset=["checksum"])]

        if len(duplicates) > 0:
            print(f"Warning: {len(duplicates)} duplicates found in dataframe")

        
        df = df.drop(columns=["checksum"])

        
        csv_file = os.path.splitext(json_file)[0] + ".csv"
        df.to_csv(os.path.join(directory_path, csv_file), index=False)

        print(f"Converted {len(df)} rows and saved to {csv_file}")
        end_time = time.time()
    print(f"Total execution time: {end_time - start_time} seconds")

def main():
    parser = argparse.ArgumentParser(description='Transform JSON files to CSV')
    parser.add_argument('directory_path', type=str, help='Path to JSON file')
    parser.add_argument("-u", '--unix_time', action='store_true', help='convert time_in and time_out to Unix format')
    args = parser.parse_args()

    transform_to_csv(args.directory_path, args.unix_time)



if(__name__=="__main__"):
    main()




