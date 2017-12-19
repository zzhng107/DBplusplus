"""
@ DB++ Group
@ Members: Lihao Yu, Zhichun Wan, Zhiwei Zhang, Rui Lan 
@ Dec 2017
"""


import pandas as pd 
import sys
from pandas import DataFrame, read_csv
import re
import time 
import os
import numpy as np
import querycsv 

path = './database/'

pd.options.mode.chained_assignment = None
pd.options.display.max_rows = None


data_table_lst = querycsv._get_table_list()
after_read_lst, table_names = querycsv._read_dataset(data_table_lst)


while(1):

	select_lst, from_lst, where_lst, rename_list, distinct = querycsv._parse()

	start_time = time.time()

	after_read_lst_used = querycsv._select_used_tables(from_lst, after_read_lst, table_names)

	table = querycsv._from(from_lst, after_read_lst_used, where_lst, rename_list)
	result = querycsv._select_and_where(table, where_lst, select_lst, distinct)
	

	duration = time.time() - start_time
	print(result)
	print("Total number of rows:", len(result))
	print("Duration", duration)
	querycsv.undo_rename(after_read_lst_used)







