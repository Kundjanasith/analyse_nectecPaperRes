args = commandArgs(trailingOnly=TRUE)
model = readRDS("src/model.data")
index = c()
result = c()
cu = as.numeric(args[1])
cs = as.numeric(args[2])
mt = as.numeric(args[3])
ma = as.numeric(args[4])
bi = as.numeric(args[5])
bo = as.numeric(args[6])
wo = as.numeric(args[7])
ti = as.numeric(args[8])
print(paste("cpu_user",cu))
print(paste("cpu_system",cs))
print(paste("mem_trans",mt))
print(paste("mem_act",ma))
print(paste("bytes_in",bi))
print(paste("bytes_out",bo))
print(paste("worker",wo))
print(paste("time",ti))

for (i in 1:wo){
   newdata = data.frame(
        cpu_user = cu,
        cpu_system = cs,
        mem_trans = mt,
        mem_act = ma,
        bytes_in = bi,
        bytes_out = bo,
        worker = i
    )   
    res = predict(model,newdata)
    index = c(index,i)
    result = c(result,res)
}
df = data.frame(index,result)
df = subset(df,result<ti)
write.csv(df,file="src/predict.csv",row.names=FALSE)
