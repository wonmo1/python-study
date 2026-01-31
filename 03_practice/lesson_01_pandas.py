# Practice code from video lectures
# This folder contains learning-only code

import pandas as pd
from pandas import Series
from pandas import DataFrame

import numpy as np

### Built-in functions

df = pd.read_csv("data/wages.csv")
df.head(2).T

df.describe()

key = df.race.unique()
value = range(len(df.race.unique()))
df["race"].replace(to_replace=key, value=value)

dict(enumerate(sorted(df["race"].unique())))

value = list(map(int, np.array(list(enumerate(df["race"].unique())))[:, 0].tolist()))
key = np.array(
    list(enumerate(df["race"].unique())), dtype=str)[:, 1].tolist()

value, key

df["race"].replace(to_replace=key, value=value, inplace=True)

df["race"]

value = list(map(int, np.array(list(enumerate(df["sex"].unique())))[:, 0].tolist()))
key = np.array(list(enumerate(df["sex"].unique())), dtype=str)[:, 1].tolist()

value, key

df["sex"].replace(to_replace=key, value=value, inplace=True)
df.head(5)

numueric_cols = ["earn", "height", "ed", "age"]
df[numueric_cols].sum(axis=1)

df.sum(axis=1)

df.isnull().sum() / len(df)

pd.options.display.max_rows = 100

df.sort_values(["age", "earn"], ascending=True)

df.sort_values("age", ascending=False).head(10)

df.age.corr(df.earn)

df.age[(df.age<45) & (df.age>15)].corr(df.earn)

df.age.cov(df.earn)

df["sex_code"] = df["sex"].replace({"male":1, "female":0})

df.corr()

df.corrwith(df.earn)

df.sex.value_counts(sort=True)
