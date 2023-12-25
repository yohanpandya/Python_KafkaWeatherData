import json
import pandas as pd 
import matplotlib.pyplot as plt
from itertools import islice
month_series = pd.Series()
jfm = ["January","February","March"]
for partition in range(4):
    file_path = f"/files/partition-{partition}.json"
    try:
        with open(file_path) as f:
            data = json.load(f)
            for month in data.items():
                
                latest_year = 0
                month_name, year_data = month
                #print(month_name)
                if month_name not in jfm:
                    continue
                for year in year_data:
                    #print(year)
                    latest_year = max(int(year), latest_year) 
                str_year = str(latest_year)
                #data[month_name][str_year]
                #print(data[month_name][str_year]['avg'])
                
                month_series[f'{month_name}-{str_year}'] = data[month_name][str_year]['avg']
    except FileNotFoundError:
        pass
        
fig, ax = plt.subplots()  
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig('/files/month.svg')
