import pandas as pd 
import csv
from pandas import DataFrame, read_csv
import re
import time 
import os
import numpy as np
import re

index_path = "./index/"
data_path = "./database/"




def build_index_for_attrib(filename,attrib_number):
	offset = 0
	out = dict()
	i = 0
	with open(filename) as rf:
		attrib_row = rf.readline()
		curr_offset = rf.tell()
		row = rf.readline()
		while row:
			# offset = rf.tell()
			if ord(row[-1]) == 10:
				if row.count('\"') % 2 != 0:
					row += rf.readline()
					continue


				# PATTERN = re.compile( r"((?:[^,\"]|\"[^\"]*\"|'[^']*')+)"  )
				# print(PATTERN.findall(row))
				# print(row)
				# temp = PATTERN.split(row)
				# temp[-1] = temp[-1][0:-1]
				r = csv.reader([row])
				temp = list(r)[0]
				# print(temp)

				print(row)
			
				if temp[attrib_number]:
					if temp[attrib_number] in out:
						out[temp[attrib_number]].append(curr_offset)
						curr_offset = rf.tell()
					else:
						out[temp[attrib_number]] = [curr_offset]
						curr_offset = rf.tell()
				row = ""
			
			row += rf.readline()
	# print(out)

	return out



#main function to build index
filename = "Enrolls.csv"


with open(data_path + filename) as f:
    reader = csv.reader(f)
    columns = next(reader)

for i in range(len(columns)):
	#donr crate index for these cols, won't improve performance
	if columns[i] != "text" and columns[i] != 'address':
		out = build_index_for_attrib(data_path + filename, i)
		destination = filename.split('.')[0] + '_attrib' + columns[i]
		np.save(index_path + destination+'.npy', out)


