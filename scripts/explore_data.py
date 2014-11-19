import os
import pandas
from matplotlib import pyplot
import numpy as np

my_data = pandas.read_csv('/Users/jpestrak/Downloads/crx.data.txt', sep = ',', header = None)

# dreampy
# seaborn
#     regplot

numerics = []
for col in my_data:
    colData = d[col]
    if np.issubdtype(my_data[col].dtype, np.number):
        numerics.append(col)
        pyplot.boxplot(colData)
        continue
    if colData.nunique() <= 10:
        colData.value_counts().plot(kind = 'bar')
    else:
        print("Column", col, "has", colData.nunique(), "unique values")

d.boxplot('C', by = 'A')

for c1, c2 in itertools.combinations(numerics, 2):
    seaborn.regplot(d[c1], d[c2])
