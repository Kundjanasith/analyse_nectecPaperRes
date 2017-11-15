mydata <- read.csv("res_u.csv")
# dat <- dat[!is.na(as.numeric(as.character(dat$A))),]
model <- lm(status ~ cpu_user + cpu_system, data=mydata)
# print(summary(model))
# save(model, file="model.data")
# export.model(model, replace = TRUE)