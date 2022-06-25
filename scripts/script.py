import pandas as pd
f=pd.read_csv("personality_analysis.csv",sep=';');

f.info();
f = f.loc[:, ~f.columns.str.contains('^Unnamed')];
f.drop_duplicates(subset=None, inplace=True)

#HADOOP1
keep_col = ['ID','Education']


#HADOOP3
#f['Spending']=f['MntWines']+f['MntFruits']+f['MntMeatProducts']+f['MntFishProducts']+f['MntSweetProducts']+f['MntGoldProds']
#keep_col = ['ID','Dt_Customer','Income','Spending']



f.to_csv("clean.csv", index=False)