# %%
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import isnan, when, count, col, lit, trim, avg, ceil
from pyspark.sql.types import StringType
import matplotlib.pyplot as plt
import seaborn as sns

# %%
sc = SparkSession.builder.master("local[*]").getOrCreate()

# %%
feature = sc.read.csv("/home/iron/Documents/1.Learning/features.csv",
                      inferSchema=True,
                      header=True)
label = sc.read.csv("/home/iron/Documents/1.Learning/labels.csv",
                    inferSchema=True,
                    header=True)


# %%
print(feature.count())
print(label.count())
print(feature.columns)
print(label.columns)

# %%
data = feature.join(label, on=("id"))
print("Count - " + str(data.count()))

# %%
print(data.printSchema())
print(data.show(10))