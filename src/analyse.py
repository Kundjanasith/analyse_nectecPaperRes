import glob

max_node = "10"

def date_format(date):
    return date.replace("/","-").replace(" ","_")[2:]

def value_format(value):
    return float(value)

def ana(st,typ):
    file_cu = open(st,"r")
    content_file_cu = []
    for cu in file_cu:
        content_file_cu.append(cu)      
    x_name = st.split("/")[4].split(".")[0]
    print(x_name)
    f_cu = open(max_node+"/"+typ+"/"+x_name+".csv","w")
    for i in content_file_cu[35:len(content_file_cu)-3]:
        line = i.split("</v></row>\n")[0].split("<row><v> ")
        if len(line) == 2:
            key = date_format(line[0].split(" +07 / ")[0][8:])
            value = value_format(line[1][:-1])
            f_cu.write(key+","+str(value)+"\n")
    f_cu.close()

import pandas as pd
import numpy as np

for file in glob.glob("../sparksample_x/nectec-paperRes/"+max_node+"/*.xml"):
    typ = file.split("/")[4].split("-")[0]
    ana(file,typ)

for c in glob.glob(max_node+"/bytes_in/*.csv"):
    temp = c.split("/")[2].split("-ssw1-")[1]
    cpuUser = pd.read_csv(max_node+"/cpu_user/cpu_user-ssw1-"+temp,header=None)
    cpuUser.columns = ['time', 'cpu_user']
    cpuSystem = pd.read_csv(max_node+"/cpu_system/cpu_system-ssw1-"+temp,header=None)
    cpuSystem.columns = ['time', 'cpu_system']
    result = cpuUser.join(cpuSystem.set_index('time'), on='time')
    byteIn = pd.read_csv(max_node+"/bytes_in/bytes_in-ssw1-"+temp,header=None)
    byteIn.columns = ['time', 'bytes_in']
    result = result.join(byteIn.set_index('time'), on='time')
    byteOut = pd.read_csv(max_node+"/bytes_out/bytes_out-ssw1-"+temp,header=None)
    byteOut.columns = ['time', 'bytes_out']
    result = result.join(byteOut.set_index('time'), on='time')
    result.to_csv(max_node+"/nodes/ssw1-"+temp, index=False)

for c in glob.glob(max_node+"/bytes_in/*.csv"):
    temp = c.split("/")[2].split("-ssw1-")[1]
    file = pd.read_csv(max_node+"/nodes/ssw1-"+temp)
    file.groupby(['time']).mean()
    file.to_csv(max_node+"/nodez/ssw1-"+temp, index=False)

import os
os.system("rm -rf "+max_node+"/system.csv")
index = 0
worker = 0
for c in glob.glob(max_node+"/bytes_in/*.csv"):
    temp = c.split("/")[2].split("-ssw1-")[1]
    if "worker" in c:
        worker = worker + 1
        if index == 0:
            file = pd.read_csv(max_node+"/nodez/ssw1-"+temp)
            file.to_csv(max_node+"/system.csv", index=False)
        else:
            file_res = pd.read_csv(max_node+"/system.csv")
            file = pd.read_csv(max_node+"/nodez/ssw1-"+temp)
            file_res = pd.concat([file_res,file]).groupby('time').mean()
            file_res.to_csv(max_node+"/system.csv")
        index = index + 1

file = pd.read_csv(max_node+"/system.csv")
file['worker'] = worker
file.to_csv(max_node+"/system.csv", index=False)

