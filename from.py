"""
TODO: R1 * R2 ... Certasian Product.
pandas 
"""


import pandas as pd 
from pandas import DataFrame, read_csv





file1 = r'Students.csv'
table1 = pd.read_csv(file1)
print(table1)

file2 = r'Enrolls.csv'
table2 = pd.read_csv(file2)
print(table2)

table1['key'] = 0
table2['key'] = 0

prod = pd.merge(table1, table2, on='key')
prod.drop('key',1, inplace=True)
print(prod)

print(prod[(prod['name'] == 'Bugs Bunny') & (prod['grade'] == 'A+')])

