from functions.preprocessing import preprocess
import pandas as pd
from pandarallel import pandarallel
import sys
from time import time

pandarallel.initialize()

df = pd.read_json("data/train.json")
label = pd.read_csv("data/label.csv")
categories = pd.read_csv("data/categories_string.csv")

df = pd.merge(df, label, on='Id')

# test avec top 5 des categories
top_5 = df['Category'].value_counts().index.values[:5]
df_top5 = df[df['Category'].isin(top_5)]
df_test = df_top5.sample(100000)
start_time = time()
df_test.loc[:, ['description_pre']] = df_test['description'].parallel_map(preprocess)
print(str(time()-start_time)+" s")
#df_top5.loc[:, ['description_pre']] = df_top5['description'].parallel_map(preprocess)

