import statsmodels.api as sm
from sklearn import datasets
import numpy as np
import pandas as pd
# data = datasets.load_boston()
# print(data.target)


# df = pd.DataFrame(data.data, columns=data.feature_names)
# target = pd.DataFrame(data.target, columns=['MEDV'])
# print(df.head())
# print(target.head())
df= pd.read_csv("res_u.csv",low_memory=False)
# print(targetZ.head())
X = df[['cpu_user','cpu_system']]
y = df['status']

# X = df[["RM", "LSTAT"]]
# print(X)
# y = target["MEDV"]

# model = sm.OLS(y,X).fit()
# print(model.summary())
# print(type(X))
# z = [{'RM': 7, 'LSTAT': 5}]
# Z = pd.DataFrame(z)
# #Z = Z[["RM", "LSTAT"]]
# print(model.predict(Z))