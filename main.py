"""
@ DB++ Group
@ Members: Lihao Yu, Zhichun Wan, Zhiwei Zhang, Rui Lan 
@ Nov 2017
@ Last Edit: 11/14
"""


import pandas as pd 
import sys
from pandas import DataFrame, read_csv
import re
import time 
import os
path = './database/'

pd.options.mode.chained_assignment = None

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
	#table = table.replace(" ", "")

	list_select = []
	list_from = []
	rename_list = []
	list_where = condition

	if attrib == "*":
		list_select.append("*")

	elif attrib.find(",") == -1:
		list_select.append(attrib)

	else:
		list_select = attrib.split(",")



	print("Fuck")
	if table == "":
		return

	elif table.find(",") == -1:
		temp = table.split(" ")
		print(temp)
		if len(temp) == 2: 
			list_from.append(temp[0])
			rename_list.append(temp[1])
		else:
			list_from.append(temp[0])
			rename_list.append(None)

	else:
		ttemp = table.split(", ")
		print(ttemp)
		for i in ttemp: 
			temp = i.split(" ")
			if len(temp) == 2: 
				list_from.append(temp[0])
				rename_list.append(temp[1])
			else:
				list_from.append(temp[0])
				rename_list.append(None)
	
	print(list_from)
	print(rename_list)
	return list_select, list_from, list_where, rename_list

def _get_table_list():
	out = []
	for fn in os.listdir(path):
		out.append(fn)
	return out

def _read_dataset(table_lst):
	real_table_list = []
	table_names = []
	for item in table_lst:
		file = path + item
		table = pd.read_csv(file)
		table_names.append(item.split('.')[0])
		real_table_list.append(table)
	return real_table_list, table_names

def _select_used_tables(from_lst, after_read_lst, table_names):
	out = []
	for item in from_lst:
		count = 0		
		for df in table_names:
			if(item == df):
				out.append(after_read_lst[count])
				break
			count += 1
	return out

def _from(table_lst, after_read_lst, args, rename_list):
	conds = _where_parse(args)

	attr_all = []
	for s in after_read_lst:
		for i in list(s.columns.values): 
			attr_all.append(i)

	if len(table_lst) == 1:

		table1 = after_read_lst[0]
		if rename_list[0] is not None: 
			table1.columns = [rename_list[0] +'.' + s for s in list(table1.columns.values)]
		return table1

	table1 = after_read_lst[0]
	attr1 = list(table1.columns.values)
	for i in range(len(conds)):

		if ((conds[i][0] in attr1) and (conds[i][2] in attr1)) or ((conds[i][0] in attr1) and (conds[i][2] not in attr_all)):
			table1 = table1[generate_result(table1, [conds[i]])]
	if rename_list[0] is not None: 
		table1.columns = [rename_list[0] +'.' + s for s in list(table1.columns.values)]

	table2 = after_read_lst[1]
	
	attr2 = list(table2.columns.values)
	for i in range(len(conds)):
		if ((conds[i][0] in attr2) and (conds[i][2] in attr2)) or ((conds[i][0] in attr2) and (conds[i][2] not in attr_all)):
			table2 = table2[generate_result(table2, [conds[i]])]
	if rename_list[1] is not None: 
		table2.columns = [rename_list[1] +'.' + s for s in list(table2.columns.values)]	

	table1['key'] = 0
	table2['key'] = 0

	prod = pd.merge(table1, table2, on='key')

	prod.drop('key', 1, inplace=True)

	for i in range(len(table_lst)-2):

		table = after_read_lst[i+2]
		attr = list(table.columns.values)
		for i in range(len(conds)):
			if ((conds[i][0] in attr1) and (conds[i][2] in attr)) or ((conds[i][0] in attr) and (conds[i][2] not in attr_all)):
				table = table[generate_result(table, [conds[i]])]
			if rename_list[i+2] is not None: 
				table.columns = [rename_list[i+2] +'.' + s for s in list(table.columns.values)]

		prod['key'] = 0
		table['key'] = 0

		prod = pd.merge(prod, table, on='key')
		prod.drop('key', 1, inplace=True)

	return prod 



def operatorLIKE(table,col, substring):
	if substring[0]== '%' and substring[-1] == '%':
		return table[col].str.contains(substring[1:-1], na = False)
	elif substring[0] == '_' and substring[-1] == '_':
		return table[col].str.contains("^." + substring[1:-1] + ".$", na = False)
	elif substring[0] == '%' and substring[-1] =='_':
		return table[col].str.contains(substring[1:-1] + ".$", na = False)
	elif substring[0] == '_' and substring[-1] == '%':
		return table[col].str.contains("^." + substring[1:-1], na = False)
	elif substring[0] != '_' and substring[0] != '%' and substring[-1] == '_':
		return table[col].str.contains("^"+ substring[0:-1] + ".$", na = False)
	elif substring[0] != '_' and substring[0] != '%' and substring[-1] == '%':
		return table[col].str.contains("^" + substring[0:-1], na = False)
	elif substring[-1] != '_' and substring[-1] != '%' and substring[0] == '_':
		return table[col].str.contains("^."+ substring[1:] + "$", na = False)
	elif substring[-1] != '_' and substring[-1] != '%' and substring[0] == '%':
		return table[col].str.contains(substring[1:] + "$", na = False)
	else:
		print("Invalid arguments, please specify qualifiers")
		return []
	return ret 



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

def _where_parse(args):
	conds = [p for p in re.split("( |\\\".*?\\\"|'.*?')" , args) if p.strip()]
	return conds	

def _select_and_where(table, args, select_lst):
	conds = _where_parse(args)
	ops = generate_boolean(table, conds)
	new =  table[evaluate_boolean(ops)]

	if (select_lst[0] == "*"):
		return new
	else:
		return new[select_lst]
def generate_boolean(table, conds):
	i = 0;
	boolean_ops = []
	while i < len(conds):
		#=, >, <, <>, >=, <=
		if conds[i] == '=' or conds[i] == '>' or conds[i] == '<' or conds[i] == '<>' or conds[i] == '>=' or conds[i] == '<=' or conds[i] == 'LIKE':
			t = operators(table, conds[i-1], conds[i+1], conds[i])
			boolean_ops.append(t)
		elif conds[i] == '(' or conds[i] == ')' or conds[i] == 'AND' or conds[i] == 'OR' or conds[i] == 'NOT':
			boolean_ops.append(conds[i])
		i += 1	
	return boolean_ops

def evaluate_boolean(ops):
	stack = []
	for i in range(len(ops)):
		if isinstance(ops[i], str) and ops[i] == '(':
			stack.append(i) 
	i = 0

	while(i< len(ops)):
		if (len(stack) == 0):
			return helper(ops)
		if isinstance(ops[i], str) and ops[i] == ')':

			starting_idx = stack.pop()
			t = helper(ops[starting_idx + 1: i])
			temp = ops[0:starting_idx]
			temp.append(t)
			temp += ops[i+1:]
			ops = temp
			i = starting_idx
		i += 1
	return helper(ops)	


def helper(ops):
	i = 0
	while(len(ops) > 1):
		if isinstance(ops[i], str) and ops[i] == 'AND':
			ops[i-1] = ops[i-1] & ops[i+1]
			ops.pop(i+1)
			ops.pop(i)
			i -= 1
		elif isinstance(ops[i], str) and ops[i] == 'OR':
			ops[i-1] = ops[i-1] | ops[i+1]
			ops.pop(i+1)
			ops.pop(i)
			i -= 1
		elif isinstance(ops[i], str) and ops[i] == 'NOT':
			ops[i] = ~ops[i+1]
			ops.pop(i+1)
		i += 1
	return ops[0]

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
		return operatorLIKE(table, op1, op2)



data_table_lst = _get_table_list()
after_read_lst, table_names = _read_dataset(data_table_lst)

while(1):

	select_lst, from_lst, where_lst, rename_list = _parse()

	start_time = time.time()

	after_read_lst_used = _select_used_tables(from_lst, after_read_lst, table_names)

	print(len(after_read_lst))
	# print(from_lst[0])
	print(from_lst)
	# print(after_read_lst_used)
	table = _from(from_lst, after_read_lst_used, where_lst, rename_list)
	print("Cetasian Product finished")

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






