import sys
import glob
import math
import numpy as np
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
cluster_name = "ssw3"

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
    for f_cu in glob.glob("../stream/"+cluster_name+"*_"+cur+".xml"):
        num_lines = sum(1 for line in open(f_cu))
        index = 0
        for i_cu in open(f_cu,"r"):
            index = index + 1
            if index==num_lines-3: 
               i_cu = i_cu.split("<row><v>")[1].split("</v></row>")[0]
               i_cu = xxx(i_cu)
               res_cu.append(i_cu)
    print(np.mean(res_cu)) 
          
Rcu = mean_current("cpu_user")
Rcs = mean_current("cpu_system")
Rbi = mean_current("bytes_in")
Rbo = mean_current("bytes_out")
