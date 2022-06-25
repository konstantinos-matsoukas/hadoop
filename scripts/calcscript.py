import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
#https://www.pluralsight.com/guides/cleaning-up-data-from-outliers
f=pd.read_csv("clean.csv",sep=',')



f.info();
f.describe()


print(f['Income'].quantile(0.10))
print(f['Income'].quantile(0.90))


print(f['Year_Birth'].quantile(0.10))
print(f['Year_Birth'].quantile(0.90))

f["Income"] = np.where(f["Income"] <24117.5, 24117.5,f['Income'])
f["Income"] = np.where(f["Income"] >79844.0, 79844.0,f['Income'])
print(f['Income'].skew())


f["Year_Birth"] = np.where(f["Year_Birth"] <1952.0, 1952.0,f['Year_Birth'])
f["Year_Birth"] = np.where(f["Year_Birth"] >1984.0, 1984.0,f['Year_Birth'])
print(f['Year_Birth'].skew())


plt.boxplot(f['Year_Birth'])
f.Income.hist()



f.to_csv("outliers.csv", index=False)