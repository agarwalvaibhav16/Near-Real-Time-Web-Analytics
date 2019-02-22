import os
import datetime,time
directory = os.path.normpath("/home/centos/Downloads/final_project/output_dir/")
a = []
for subdir, dirs, files in os.walk(directory):
    for file in files:
        if file.startswith("part"):
            f=open(os.path.join(subdir, file),'r')
            content=f.read().strip()
            content=content.replace("\n","&")
            lists=content.split("&")
            for x in lists:
                a.append(x)
            f.close()

for x in a:
    print(x)

a=list(map(eval,a))

b=[]
for ele in a:
    c=[]
    for i in range(len(ele)):
        if i!=2:
            pairs=ele[i].split(":")
            c.append(pairs)
        else:
            pairs = ele[i].split(':"')
            c.append(pairs)
    print(c)
    b.append(c)

data={}
for x in b:
    for y in x:
        print(y)
        if y[0] not in data:
            data[y[0]]=[]
            data[y[0]].append(y[1].strip('"'))
        else:
            data[y[0]].append(y[1].strip('"'))

import pandas as pd
data=pd.DataFrame(data=data)
data['date'] = data['current_date_time'].apply(lambda x: x.strftime('%d%m%Y'))
data['time'] = data['current_date_time'].apply(lambda x: x.strftime('%H%M%S'))
data.to_csv('visualization.csv', encoding='utf-8', index=False)
