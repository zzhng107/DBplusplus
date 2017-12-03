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
import os
import numpy as np
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
	if table == "":
		return

	elif table.find(",") == -1:
		temp = table.split(" ")
		if len(temp) == 2: 
			list_from.append(temp[0])
			rename_list.append(temp[1])
		else:
			list_from.append(temp[0])
			rename_list.append(None)

	else:
		ttemp = table.split(", ")
		for i in ttemp: 
			temp = i.split(" ")
			if len(temp) == 2: 
				list_from.append(temp[0])
				rename_list.append(temp[1])
			else:
				list_from.append(temp[0])
		
				rename_list.append(None)
	distinct = False
	if "DISTINCT" in list_select:
		distinct = True
		list_select.remove("DISTINCT")

	return list_select, list_from, list_where, rename_list, distinct


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
		table = table.replace(np.nan, 9999)
		table_names.append(item.split('.')[0])
		real_table_list.append(table)
	return real_table_list, table_names

def _select_used_tables(from_lst, after_read_lst, table_names):
	out = []
	readed = []
	index = []
	for item in from_lst:
		count = 0		
		for df in table_names:
			if(item == df):
				if item in readed:
					table = after_read_lst[index[readed.index(item)]].copy(deep = True)
					out.append(table)
					break
				out.append(after_read_lst[count])
				index.append(count)
				readed.append(item)
				break
			count += 1
	return out

def preprocess(original_table_list, conds):
	ret_list = []
	used_index = []
	flag = False
	for i in range(len(conds)):
		flag = False
		for j in range(len(ret_list)):
			if conds[i][0] in ret_list[j]:
				ret_list[j] = ret_list[j][operators(ret_list[j], conds[i][0], conds[i][2], conds[i][1])]
				flag = True

		if not flag:
			for j in range(len(original_table_list)):
				if conds[i][0] in original_table_list[j]:
					temp = original_table_list[j][operators(original_table_list[j], conds[i][0], conds[i][2], conds[i][1])]
					ret_list.append(temp)
					used_index.append(j)

	for i in range(len(original_table_list)):
		if i not in used_index:
			ret_list.append(original_table_list[i])
	
	return ret_list



def _from(table_lst, after_read_lst, args, rename_list):

	if len(after_read_lst) == 1:
		table1 = after_read_lst[0]
		if rename_list[0] is not None: 
			table1.columns = [rename_list[0] +'.' + s for s in list(table1.columns.values)]
		return table1

	for i in range(len(after_read_lst)):
		if rename_list[i] is not None: 
			after_read_lst[i].columns = [rename_list[i] +'.' + s for s in list(after_read_lst[i].columns.values)]

	conds = _where_parse(args)
	#preprocess the tables
	
	all_table_col_names = []
	for table in after_read_lst:
		all_table_col_names.extend(table.columns.values)

	join_conds = []
	preprocess_conds = []
	#find join conditions and preprocess conditions
	for i in range(len(conds)):
		if conds[i] == '=' or conds[i] == '>' or conds[i] == '<' or conds[i] == '>=' or conds[i] == '<=' or conds[i] == '<>':
			if conds[i+1] in all_table_col_names:
				if conds[i] == "=":
					join_conds.append([conds[i-1], conds[i+1]])
			else:
				preprocess_conds.append([conds[i-1], conds[i], conds[i+1]])


	preprocessd_tables = preprocess(after_read_lst, preprocess_conds)


	while(len(join_conds) > 0):
		#cond join
		col1, col2 = join_conds.pop()
		for i in range(len(preprocessd_tables)):
			if col1 in preprocessd_tables[i].columns:
				t1 = i
			if col2 in preprocessd_tables[i].columns:
				t2 = i

		prod = pd.merge(preprocessd_tables[t1], preprocessd_tables[t2], left_on = col1, right_on = col2)
		if t1 > t2:
			preprocessd_tables.pop(t1)
			preprocessd_tables.pop(t2)
		else:
			preprocessd_tables.pop(t2)
			preprocessd_tables.pop(t1)
		preprocessd_tables.append(prod)
	#cartesian product
	prod = preprocessd_tables[0]


	for i in range(1, len(preprocessd_tables)):
		prod['key'] = 1
		preprocessd_tables[i]['key'] = 1
		prod = pd.merge(prod,preprocessd_tables[i], on = 'key' )
		prod.drop('key', 1, inplace = True)

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

def _select_and_where(table, args, select_lst, distinct):
	if len(args) > 0:
		conds = _where_parse(args)
		ops = generate_boolean(table, conds)
		new =  table[evaluate_boolean(ops)]
	else:
		new = table

	if (select_lst[0] == "*"):
		return new
	else:
		if(distinct == True):
			return new[select_lst].drop_duplicates()
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
	
	temp = 0
	flag = False
	if '+' in str(op2) or '-' in str(op2) or '*' in str(op2) or '/' in str(op2) :
		for i in ['+','-','*','/']:
			if i in op2:
				flag = True
				[n1, n2] = op2.split(i)
				if i == '+': 
					temp = table[n1] + float(n2) 
				elif i == '-':
					#print(table[n1])
					temp = table[n1] - float(n2)
				elif i == '*':
					temp = table[n1] * float(n2)
				elif i == '/':
					temp = table[n1] / float(n2)
			

	
	if op == "=":
		return table[op1] == (temp if flag else (table[op2] if op2 in table.columns else op2))
	elif op == "<": 
		return table[op1] < (temp if flag else (table[op2] if op2 in table.columns else op2))
	elif op == ">":
		return table[op1] > (temp if flag else (table[op2] if op2 in table.columns else op2))
	elif op == ">=":
		return table[op1] >= (temp if flag else (table[op2] if op2 in table.columns else op2))
	elif op == "<=":
		return table[op1] <= (temp if flag else (table[op2] if op2 in table.columns else op2))
	elif op == "<>":
		return table[op1] != (temp if flag else (table[op2] if op2 in table.columns else op2))
	elif op == "LIKE":
		return operatorLIKE(table, op1, op2)

	flag = False

def undo_rename(table_list):
	for i in range(len(table_list)):
		table_list[i].columns = [s.split(".")[-1] for s in list(table_list[i].columns.values)]


data_table_lst = _get_table_list()
after_read_lst, table_names = _read_dataset(data_table_lst)


while(1):

	select_lst, from_lst, where_lst, rename_list, distinct = _parse()

	start_time = time.time()

	after_read_lst_used = _select_used_tables(from_lst, after_read_lst, table_names)

	table = _from(from_lst, after_read_lst_used, where_lst, rename_list)
	result = _select_and_where(table, where_lst, select_lst, distinct)
	

	duration = time.time() - start_time
	print(result)
	print("Duration", duration)
	undo_rename(after_read_lst_used)







