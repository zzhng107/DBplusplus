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
	table = table.replace(" ", "")

	list_select = []
	list_from = []
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
		list_from.append(table)

	else:
		list_from = table.split(",")
	
	return list_select, list_from, list_where 

def _read_dataset(table_lst):
	real_table_list = []

	for item in table_lst:
		file = item + '.csv'
		table = pd.read_csv(file)
		real_table_list.append(table)

	return real_table_list 

def _from(table_lst, after_read_lst, args):
	conds = _where_parse(args)

	attr_all = []
	for s in after_read_lst:
		for i in list(s.columns.values): 
			attr_all.append(i)

	if len(table_lst) == 1:

		table1 = after_read_lst[0]
		return table1


	table1 = after_read_lst[0]

	
	attr1 = list(table1.columns.values)
	for i in range(len(conds)):

		if ((conds[i][0] in attr1) and (conds[i][2] in attr1)) or ((conds[i][0] in attr1) and (conds[i][2] not in attr_all)):
			table1 = table1[generate_result(table1, [conds[i]])]

	table2 = after_read_lst[1]
	
	attr2 = list(table2.columns.values)
	for i in range(len(conds)):
		if ((conds[i][0] in attr2) and (conds[i][2] in attr2)) or ((conds[i][0] in attr2) and (conds[i][2] not in attr_all)):
			table2 = table2[generate_result(table2, [conds[i]])]
	

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

		prod['key'] = 0
		table['key'] = 0

		prod = pd.merge(prod, table, on='key')
		prod.drop('key', 1, inplace=True)

	return prod 

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
"""

def operatorLIKE(op, pattern):
	# %or% have "or" in any position
	if pattern[0] == '%' and pattern[-1] == '%':
		return op.find(pattern) >= 0
	# %a start with a
	elif pattern[0] != '%' and pattern[-1] == '%':
		return op.startswith(pattern[:-1])
	# %a end with a
	elif pattern[0] == '%' and pattern[-1] != '%':
		return op.endswith(pattern[1:])
	# '_r% have "r" in the second position
	elif pattern[0] == '_' and pattern[-1] == '%':
		return op.find(pattern) == 1
	# '%r_ have "r" in the last second position
	elif pattern[0] == '%' and pattern[-1] == '_':
		return op.find(pattern) == -1 - len(pattern[1:-1])
	# '_r_ have "r" in the middle with 2 single char at the end
	elif pattern[0] == '_' and pattern[-1] == '_':
		return op[1:-1] == pattern[1:-1]



"""
def _select_and_where(table, args, select_lst):
	conds = _where_parse(args)
	print(conds)
	new =  table[generate_result(table, conds)]

	if (select_lst[0] == "*"):
		return new
	else:
		return new[select_lst]
"""

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
		return operatorLIKE(op1, op2)



select_lst, from_lst, where_lst = _parse()

after_read_lst = _read_dataset(from_lst)

start_time = time.time()


table = _from(from_lst, after_read_lst, where_lst)

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






