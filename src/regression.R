mydata <- read.csv("res_uu.csv")
# dat <- dat[!is.na(as.numeric(as.character(dat$A))),]
model <- lm(status ~ cpu_user + cpu_system + mem_trans + mem_act + bytes_in + bytes_out + worker , data=mydata)
print(summary(model))
saveRDS(model, file="model.data")
# export.model(model, replace = TRUE)
