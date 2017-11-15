"""
@ DB++ Group
@ Members: Lihao Yu, Zhichun Wan, Zhiwei Zhang, Rui Lan 
@ Nov 2017
"""


import pandas as pd 
import sys
from pandas import DataFrame, read_csv
import re
import time 


def _welcome():
	print('********************************************')
	print('A DB++ Group Product.')
	print('********************************************')

def _parse():

	_welcome()

	attrib = input("SELECT ")
	table = input("FROM ")
	condition = input("WHERE ")

	attrib = attrib.replace(" ", "")
	table = table.replace(" ", "")

	list_select = []
	list_from = []
	#list_where = ""
	list_where = condition

	# select all 
	if attrib == "*":
		#print("select all")
		list_select.append("*")

	# select only one attribute
	elif attrib.find(",") == -1:
		#print("only one attrib")
		list_select.append(attrib)

	# select more than one attrib
	else:
		#print("multiple attrib")
		list_select = attrib.split(",")

	# from none table
	if table == "":
		#print("ERROR: from clause cannot be empty")
		return

	# from one table
	elif table.find(",") == -1:
		#print("only one table")
		list_from.append(table)

	# from more than one table
	else:
		#print("multiple table")
		list_from = table.split(",")
	
	#print("SELECT: ", _select)
	#print("FROM: ", _from)

	return list_select, list_from, list_where 

def _read_dataset(table_lst):
	real_table_list = []

	for item in table_lst:
		file = item + '.csv'
		table = pd.read_csv(file)
		real_table_list.append(table)

	return real_table_list 

def _from(table_lst, after_read_lst):

	if len(table_lst) == 1:
		#file1 = table_lst[0] + '.csv'
		#table1 = pd.read_csv(file1)
		table1 = after_read_lst[0]
		table1.columns = [table_lst[0] +'.' + s for s in list(table1.columns.values)]
		return table1

	#file1 = table_lst[0] + '.csv'
	#table1 = pd.read_csv(file1)
	table1 = after_read_lst[0]
	table1.columns = [table_lst[0] +'.' + s for s in list(table1.columns.values)]

	#file2 = table_lst[1] + '.csv'
	#table2 = pd.read_csv(file2)
	table2 = after_read_lst[1]
	table2.columns = [table_lst[1] +'.' + s for s in list(table2.columns.values)]

	table1['key'] = 0
	table2['key'] = 0

	prod = pd.merge(table1, table2, on='key')
	prod.drop('key', 1, inplace=True)

	for i in range(len(table_lst)-2):
		#file = item + '.csv'
		#table = pd.read_csv(file)
		table = after_read_lst[i+2]
		table.columns = [table_lst[i+2] +'.' + s for s in list(table.columns.values)]

		prod['key'] = 0
		table['key'] = 0

		prod = pd.merge(prod, table, on='key')
		prod.drop('key', 1, inplace=True)

	return prod 

"""
TODO
"""

def operatorLIKE(table, substring):
	ret = []
	for i in range(len(data)):
		if substring[0]== '%' and substring[-1] == '%':
			if re.search(substring[1:-1], data[i]):
				ret.append(i)
		elif substring[0] == '_' and substring[-1] == '_':
			if re.search("^." + substring[1:-1] + ".$", data[i]):
				ret.append(i)
		elif substring[0] == '%' and substring[-1] =='_':
			if re.search(substring[1:-1] + ".$", data[i]):
				ret.append(i)
		elif substring[0] == '_' and substring[-1] == '%':
			if re.search("^." + substring[1:-1], data[i]):
				ret.append(i)
		elif substring[0] != '_' and substring[0] != '%' and substring[-1] == '_':
			if re.search("^"+ substring[0:-1] + ".$", data[i]):
				ret.append(i)
		elif substring[0] != '_' and substring[0] != '%' and substring[-1] == '%':
			if re.search("^" + substring[0:-1], data[i]):
				ret.append(i)
		elif substring[-1] != '_' and substring[-1] != '%' and substring[0] == '_':
			if re.search("^."+ substring[1:] + "$", data[i]):
				ret.append(i)
		elif substring[-1] != '_' and substring[-1] != '%' and substring[0] == '%':
			if re.search(substring[1:] + "$", data[i]):
				ret.append(i)
		else:
			print("Invalid arguments, please specify qualifiers")
			return []
	return ret 

def _where_parse(args):
	conds = [p for p in re.split("( |\\\".*?\\\"|'.*?')" , args) if p.strip()]
	i,j = 0, 0
	ret = []
	while(len(conds)>0):
		i = min(conds.index("AND") if "AND" in conds else 99999, conds.index("OR") if "OR" in conds else 99999, conds.index("NOT") if "NOT" in conds else 99999)
		if(i != 99999):
			cond = conds[i-3:i]
			cond.append(conds[i])
			conds = conds[i+1:]			
		else:
			cond = conds
			conds = []
		ret.append(cond)
	return ret	

def _select_and_where(table, args, select_lst):
	conds = _where_parse(args)
	new =  table[generate_result(table, conds)]
	return new[select_lst]

def generate_result(table, conds):
	next_logic_op = ""
	ret = operators(table, conds[0][0], conds[0][2], conds[0][1])
	for cond in conds:
		t = operators(table, cond[0], cond[2], cond[1])

		if next_logic_op == "AND":
			ret = ret & t
		elif next_logic_op == "OR":
			ret = ret | t
		elif next_logic_op == "NOT":
			ret = ~ret
		if(len(cond) == 4):
			next_logic_op = cond[3]
	return ret


def operators(table, op1, op2, op):
	if(op2.replace('.','',1).isdigit()):
		op2 = float(op2)
	else:
		op2 = op2.replace("\"","")

	if op == "=":
		return table[op1] == op2
	elif op == "<":
		return table[op1] < op2
	elif op == ">":
		return table[op1] > op2
	elif op == "LIKE":
		return operatorLIKE(table, op2)



select_lst, from_lst, where_lst = _parse()

after_read_lst = _read_dataset(from_lst)

start_time = time.time()

table = _from(from_lst, after_read_lst)

result = _select_and_where(table, where_lst, select_lst)

duration = time.time() - start_time

print(result)

print("Duration", duration)

#_select_and_where()
#print(table)
#print(from_lst)
#lst = ['Enrolls', 'Students', 'Beers']
#table = _from(lst)
#print(table)
#print(table[['Enrolls.id', 'Beers.Price']])






