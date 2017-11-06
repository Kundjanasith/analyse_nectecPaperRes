# import glob

max_node = "10"

# def date_format(date):
#     return date.replace("/","-").replace(" ","_")[2:]

# def value_format(value):
#     return float(value)

# def ana(st,typ):
#     file_cu = open(st,"r")
#     content_file_cu = []
#     for cu in file_cu:
#         content_file_cu.append(cu)      
#     x_name = st.split("/")[4].split(".")[0]
#     print(x_name)
#     f_cu = open(max_node+"/"+typ+"/"+x_name+".csv","w")
#     for i in content_file_cu[35:len(content_file_cu)-3]:
#         line = i.split("</v></row>\n")[0].split("<row><v> ")
#         if len(line) == 2:
#             key = date_format(line[0].split(" +07 / ")[0][8:])
#             value = value_format(line[1][:-1])
#             f_cu.write(key+","+str(value)+"\n")
#     f_cu.close()

# import pandas as pd
# import numpy as np

# for file in glob.glob("../sparksample_x/nectec-paperRes/"+max_node+"/*.xml"):
#     typ = file.split("/")[4].split("-")[0]
#     ana(file,typ)

# for c in glob.glob(max_node+"/bytes_in/*.csv"):
#     temp = c.split("/")[2].split("-ssw1-")[1]
#     cpuUser = pd.read_csv(max_node+"/cpu_user/cpu_user-ssw1-"+temp,header=None)
#     cpuUser.columns = ['time', 'cpu_user']
#     cpuSystem = pd.read_csv(max_node+"/cpu_system/cpu_system-ssw1-"+temp,header=None)
#     cpuSystem.columns = ['time', 'cpu_system']
#     result = cpuUser.join(cpuSystem.set_index('time'), on='time')
#     byteIn = pd.read_csv(max_node+"/bytes_in/bytes_in-ssw1-"+temp,header=None)
#     byteIn.columns = ['time', 'bytes_in']
#     result = result.join(byteIn.set_index('time'), on='time')
#     byteOut = pd.read_csv(max_node+"/bytes_out/bytes_out-ssw1-"+temp,header=None)
#     byteOut.columns = ['time', 'bytes_out']
#     result = result.join(byteOut.set_index('time'), on='time')
#     result.to_csv(max_node+"/nodes/ssw1-"+temp, index=False)

# for c in glob.glob(max_node+"/bytes_in/*.csv"):
#     temp = c.split("/")[2].split("-ssw1-")[1]
#     file = pd.read_csv(max_node+"/nodes/ssw1-"+temp)
#     file.groupby(['time']).mean()
#     file.to_csv(max_node+"/nodez/ssw1-"+temp, index=False)

# import os
# os.system("rm -rf "+max_node+"/system.csv")
# index = 0
# worker = 0
# for c in glob.glob(max_node+"/bytes_in/*.csv"):
#     temp = c.split("/")[2].split("-ssw1-")[1]
#     if "worker" in c:
#         worker = worker + 1
#         if index == 0:
#             file = pd.read_csv(max_node+"/nodez/ssw1-"+temp)
#             file.to_csv(max_node+"/system.csv", index=False)
#         else:
#             file_res = pd.read_csv(max_node+"/system.csv")
#             file = pd.read_csv(max_node+"/nodez/ssw1-"+temp)
#             file_res = pd.concat([file_res,file]).groupby('time').mean()
#             file_res.to_csv(max_node+"/system.csv")
#         index = index + 1

# file = pd.read_csv(max_node+"/system.csv")
# file['worker'] = worker
# file.to_csv(max_node+"/system.csv", index=False)

# spf = glob.glob("../sparksample_x/nectec-paperRes/"+max_node+"/*")
# for sp in spf:
#     if len(sp.split("/")[4])==3:
#         os.system("rm -rf "+max_node+"/spark/"+sp.split("/")[4]+".txt")
#         for ti in glob.glob("../sparksample_x/nectec-paperRes/"+max_node+"/"+sp.split("/")[4]+"/*"):
#             os.system("cat "+ti+" >> "+max_node+"/spark/raw/"+sp.split("/")[4]+".txt")

# def MB2KB(x):
#     res = 0
#     if x[-2:]=="MB":
#         res = float(x[:-2])*1000
#     if x[-2:]=="KB":
#         res = float(x[:-2])
#     return res

# def date_format(date):
#     return date.replace("/","-").replace(" ","_")

# def date_norm(date):
#     year = "20" + date.split("/")[0]
#     month = date.split("/")[1]
#     day = date.split("/")[2].split(" ")[0]
#     hour = date.split(":")[0].split(" ")[1]
#     minu = date.split(":")[1]
#     sec = date.split(":")[2]
#     return [int(year),int(month),int(day),int(hour),int(minu),int(sec)]

# for r in glob.glob(max_node+"/spark/raw/*.txt"):
#     file = open(r, "r") 
#     blockManagerInfo_list = []
#     memoryStore_list = []
#     status = "NONE"
#     index_start = 0
#     index_stop = 0
#     start_time = "start_time"
#     stop_time = "stop_time"
#     data_size = r.split("/")[3].split(".txt")[0]
#     print(data_size)
#     for line in file:
#         l = line.split(" ")
#         if len(l) > 3:
#             if l[3] == "MemoryStore:": #transformation
#                 memoryStore_list.append(l)
#             if l[3] == "BlockManagerInfo:": #action
#                 blockManagerInfo_list.append(l)
#         if "java.lang.OutOfMemoryError:" in line:
#             status = "-1"
#         if index_stop == 0:
#             stop_time = line
#         if index_start == 0:
#             start_time = line
#         index_start = index_start + 1

#     if status != "-1" and start_time != "start_time" and stop_time != "stop_time":
#         if len(start_time.split(" ")) != 2 or len(stop_times.split(" ")) != 2 :
# 	        status = str(0)
#         else:
#             startTime = start_time.split(" ")[0] + " " + start_time.split(" ")[1]
#             stopTime = stop_time.split(" ")[0] + " " + stop_time.split(" ")[1]
#             startNorm = date_norm(startTime)
#             startDate = datetime(startNorm[0], startNorm[1], startNorm[2], startNorm[3], startNorm[4], startNorm[5])
#             startDate = mktime(startDate.timetuple())
#             stopNorm = date_norm(stopTime)
#             stopDate = datetime(stopNorm[0], stopNorm[1], stopNorm[2], stopNorm[3], stopNorm[4], stopNorm[5])
#             stopDate = mktime(stopDate.timetuple())
#             status = str(stopDate - startDate)

#     file_output = open(max_node+"/spark/csv/"+data_size+".csv","w")
#     file_output.write("time,status,mem_trans,mem_act,size\n")
#     for m in memoryStore_list:
#         time = m[0]+" "+m[1]
#         time = date_format(time)
#         if m[4]=="Block":
#             size = (m[13]+m[14])[:-1]
#             free = (m[16]+m[17])[:-2]
#             file_output.write(time+","+status+","+str(MB2KB(size))+",0,"+data_size+"\n")
#     for b in blockManagerInfo_list:
#         time = m[0]+" "+m[1]
#         time = date_format(time)
#         if b[4]=="Added": 
#             storage = b[7]
#             size = (b[11]+b[12])[:-1]
#             free = (b[14]+b[15])[:-2]
#             file_output.write(time+","+status+",0,"+str(MB2KB(size))+","+data_size+"\n")
#         if b[4]=="Removed":
#             storage = b[9]
#             if storage == "disk":
#                 size = (b[11]+b[12])[:-2]
#                 file_output.write(time+","+status+",0,"+str(MB2KB(size))+","+data_size+"\n")
#             if storage ==  "memory":
#                 size = (b[11]+b[12])[:-1]
#                 free = (b[14]+b[15])[:-2]
#                 file_output.write(time+","+status+",0,"+str(MB2KB(size))+","+data_size+"\n")
#         if b[4]=="Updated":
#             storage = b[7]
#             size = (b[12]+b[13])[:-1]
#             free = (b[16]+b[17])[:-2]
#             file_output.write(time+","+status+",0,"+str(MB2KB(size))+","+data_size+"\n")

# import os
# import pandas as pd
# os.system("rm -rf "+max_node+"/spark.csv")
# index = 0
# for filenameZ in glob.glob(max_node+"/spark/csv/*.csv"):
#     if index == 0:
#         file = pd.read_csv(filenameZ)
#         file.to_csv(max_node+"/spark.csv", index=False)
#     else:
#         file_res = pd.read_csv(max_node+"/spark.csv")
#         file = pd.read_csv(filenameZ)
#         file_res = file_res.append(file)
#         file_res = file_res.sort_values(['time'])
#         file_res.to_csv(max_node+"/spark.csv", index=False)
#     index = index + 1

# spark_file = pd.read_csv(max_node+"/spark.csv")
# system_file = pd.read_csv(max_node+"/system.csv")

# file = spark_file.join(system_file.set_index('time'), on='time')
# file = file.dropna(subset=['cpu_user','cpu_system','bytes_in','bytes_out','worker'])

# file.to_csv(max_node+"/res.csv", index=False)

# import time
# file = open(max_node+"/res_u.csv","w")
# for i in open(max_node+"/res.csv","r"):
#     ti = i.split(",")[0] 
#     if ti != "time":
#         year = ti.split("-")[0]
#         month = ti.split("-")[1]
#         day = ti.split("-")[2].split("_")[0]
#         h = ti.split("_")[1].split(":")[0]
#         m = ti.split(":")[1]
#         s = ti.split(":")[2]
#         t = time.mktime(time.strptime(day+"/"+month+"/"+"20"+year, "%d/%m/%Y"))
#         print(t)
#         t = t + int(h)*60*60 + int(m)*60 + int(s)
#         print(t)
#         file.write(i[:-1]+","+str(t)+"\n")
#     else:
#         file.write(i[:-1]+",time_u\n")
