import sys
import glob
import math
import numpy as np
import os
"""
cu = sys.argv[1]
cs = sys.argv[2]
mt = sys.argv[3]
ma = sys.argv[4]
bi = sys.argv[5]
bo = sys.argv[6]
wo = sys.argv[7]
ti = sys.argv[8]
"""


ti = sys.argv[1]
wo = sys.argv[2]
cluster_name = sys.argv[3]


#cluster_name = "ssw3"

def xxx(x): 
    n = x.split("e")[1][:-2]
    a = float(x.split("e")[0])
    b = int(x.split("e")[1][1:])
    if n == "+":
       return a*math.pow(10,b)
    if n == "-":
       return a*math.pow(10,-b)

def mean_current(cur):
    res_cu = []
    for f_cu in glob.glob(cluster_name+"*_"+cur+".xml"):
        num_lines = sum(1 for line in open(f_cu))
        index = 0
        for i_cu in open(f_cu,"r"):
            index = index + 1
            if index==num_lines-3: 
               i_cu = i_cu.split("<row><v>")[1].split("</v></row>")[0]
               i_cu = xxx(i_cu)
               res_cu.append(i_cu)
    print(np.mean(res_cu)) 
    return np.mean(res_cu)

 
def RT(x):
    if math.isnan(x):  
       return 0
    else:
       return x
         
Rcu = RT(mean_current("cpu_user"))
Rcs = RT(mean_current("cpu_system"))
Rbi = RT(mean_current("bytes_in"))
Rbo = RT(mean_current("bytes_out"))

blockManagerInfo_list = []
memoryStore_list = []
file = open("spark.txt","r")
for line in file:
    l = line.split(" ")
    if len(l) >= 3:
       if l[3]=="MemoryStore:":
          memoryStore_list.append(l)
       if l[3]=="BlockManagerInfo:":
          blockManagerInfo_list.append(l)

def MB2KB(x):
    res = 0
    if x[-2:]=="MB":
        res = float(x[:-2])*1000
    if x[-2:]=="KB":
        res = float(x[:-2])
    return res
"""
def RT(x):
    if math.isnan(x):
       return 0
    else:
       return x
"""
b_list = []
m_list = []
for b in blockManagerInfo_list:
    if b[4]=="Added": 
        size = (b[11]+b[12])[:-1]
        free = (b[14]+b[15])[:-2]
        size = MB2KB(size)
        b_list.append(size)
    elif b[4]=="Removed":
        storage = b[9]
        if storage == "disk":
            size = (b[11]+b[12])[:-2]
            size = MB2KB(size)
            b_list.append(size)
        if storage ==  "memory":
            size = (b[11]+b[12])[:-1]
            free = (b[14]+b[15])[:-2]
            size = MB2KB(size)
            b_list.append(size)
    elif b[4]=="Updated":
        current = (b[12]+b[13])[:-1]
        original = (b[16]+b[17])[:-2]
        current = MB2KB(current)
        b_list.append(current)
    else:
        print("ERROR")
bL = np.mean(b_list)
print(bL)


for m in memoryStore_list:
    if m[4]=="Block":
        size = (m[13]+m[14])[:-1]
        size = MB2KB(size)
        free = (m[16]+m[17])[:-2]
        m_list.append(size)
    """
    elif m[4]=="MemoryStore":
        print("MemoryStore")
    else:
        print("ERROR")
    """
mL = np.mean(m_list)
print(mL)

memT = mL
memA = bL
os.system("Rscript ../predict/src/use_model1.R "+str(Rcu)+" "+str(Rcs)+" "+str(memT)+" "+str(memA)+" "+str(Rbi)+" "+str(Rbo)+" "+str(wo)+" "+str(ti))

index = 0
for l in open("../predict/src/predict.csv","r"):
    #index = index + 1
    #if index == 2: 
    #   print(l)
    print(l)
