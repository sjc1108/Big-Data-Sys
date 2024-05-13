import os
import json
import pandas as pd
import matplotlib.pyplot as plt


path = '/files/'


my_partitions = [0, 1, 2, 3]
months_of_interest = ['January', 'February', 'March']
my_dict = {}


for p in my_partitions: #iteraete to get latest data 
    file_path = f"{path}partition-{p}.json"

    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            content = json.load(file)

            for m in months_of_interest:  #loop thru each month 
                if m in content:  #getting data for most recent year 
                    years = content[m] 
                    latest_year = max(years, key=lambda k: int(k)) #find most recent
                    my_dict[f'{m}-{latest_year}'] = years[latest_year]['avg'] #avg max temp
    else:
        print(f"Warning: File {file_path} not found. Skipping...")


if not my_dict: #checking we have  data to plot
    print("No data available for plotting.")
else:
    my_month = pd.Series(my_dict)
    
    plt.figure(figsize=(10, 6))
    my_month.plot(kind='bar', color = 'blue')
    plt.ylabel('Avg. Max Temperature')
    plt.xticks(rotation = 90)
    plt.tight_layout()
    plt.savefig("/files/month.svg")


    print("Plot saved to /files/month.svg.")
