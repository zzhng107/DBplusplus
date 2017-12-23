"""
@ DB++ Group
@ Members: Lihao Yu, Zhichun Wan, Zhiwei Zhang, Rui Lan 
@ Nov 2017
"""
from copy import deepcopy
import csv
import pandas as pd 
import sys
from pandas import DataFrame, read_csv
import re
import time 
import os
import numpy as np
data_path = './database/'
index_path = "./index/"

pd.options.mode.chained_assignment = None
pd.options.display.max_rows = None 


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


def form_table_from_index(filename, columns, final_index):
	lines = []
	# print("hi")
	with open(data_path + filename) as f:
		reader = csv.reader(f)
		for idx in final_index:
			f.seek(idx,0)
			row = f.readline()
			while (row.count('\"') % 2) != 0:			
				row += f.readline()
			r = csv.reader([row])
			temp = list(r)[0]
			lines.append(temp)
			row = ""
	# print(lines)
	return lines


def optimize_and_form_table(table_list, columns_collection, selection_conds,join_conds):
	# load index, perform selection 
	# call form_table lastly
	#0op1, 1op, 2op2, 3 and
	# print(selection_conds)
	out = []
	final_index_set = dict()
	for i,table in enumerate(table_list):
		columns = columns_collection[i]
		final_index = []
		for cond in selection_conds:
			if cond[0] in columns:
				#fetch index
				index_name = index_path + table.split('.')[0] + '_attrib' + cond[0].split('.')[-1] + ".npy"
				offsets = np.load(index_name).item() 
				if cond[1] == "=":
					# op2 = op2.replace("\"","")
					index = offsets[cond[2].replace("\"","")]
				elif cond[1] == ">":
					index = []
					for key in offsets:
						if(cond[2].replace('.','',1).isdigit()):
							if float(key) > float(cond[2]): index.extend(offsets[key])
						else:
							if key > cond[2]: index.extend(offsets[key])
				elif cond[1] == "<":
					index = []
					for key in offsets:
						if(cond[2].replace('.','',1).isdigit()):
							if float(key) < float(cond[2]): index.extend(offsets[key])
						else:
							if key < cond[2]: index.extend(offsets[key])
				elif cond[1] == ">=":
					index = []
					for key in offsets:
						if(cond[2].replace('.','',1).isdigit()):
							if float(key) >= float(cond[2]): index.extend(offsets[key])
						else:
							if key >= cond[2]: index.extend(offsets[key])
				elif cond[1] == "<=":
					index = []
					for key in offsets:
						if(cond[2].replace('.','',1).isdigit()):
							if float(key) <= float(cond[2]): index.extend(offsets[key])
						else:
							if key <= cond[2]: index.extend(offsets[key])
				elif cond[1] == "<>":
					index = []
					for key in offsets:
						if(cond[2].replace('.','',1).isdigit()):
							if float(key) != float(cond[2]): index.extend(offsets[key])
						else:
							if key != cond[2]: index.extend(offsets[key])
				#####
				final_index.append(index)
		#now read in table
		if len(final_index)>0:
			final_index =  set.intersection(*map(set,final_index))
		final_index_set[i] = final_index

	# print(final_index_set)

	# print(len(join_conds))
	if(len(join_conds) == 0):
		lines = form_table_from_index(table_list[0], columns_collection[0], final_index_set[0])
		df = pd.DataFrame(lines)
		if (len(lines) > 0):
			df.columns = columns_collection[0]
		out.append(df)
		return out


	exsited = []
	while(len(join_conds) > 0):
		join = join_conds.pop(0)

		for i,columns in enumerate(columns_collection):
			# print(join[0], columns, join[0] in columns, i)
			if join[0] in columns:
				t1 = i
				at1 = columns.index(join[0])
				# print("t1  ", t1)
			if join[1] in columns:
				t2 = i
				at2 = columns.index(join[1])
		
		if len(final_index_set[t1]) == 0 and len(final_index_set[t2]) == 0:
			join_conds.append(join)
			continue
		elif (len(final_index_set[t1]) > 0 and len(final_index_set[t2]) == 0):
			to_form = t2
			left_att = at1
			right_att = at2
			col_idx = t1
			lines = form_table_from_index(table_list[t1], columns_collection[t1], final_index_set[t1])
			index_name = index_path + table_list[t2].split('.')[0] + '_attrib' + columns_collection[t2][at2].split('.')[-1] + ".npy"
			offsets = np.load(index_name).item()
		elif (len(final_index_set[t1]) == 0 and len(final_index_set[t2]) > 0):
			to_form = t1
			left_att = at2
			right_att = at1
			col_idx = t2
			lines = form_table_from_index(table_list[t2], columns_collection[t2], final_index_set[t2])
			index_name = index_path + table_list[t1].split('.')[0] + '_attrib' + columns_collection[t1][at1].split('.')[-1] + ".npy"
			offsets = np.load(index_name).item()
		elif (len(final_index_set[t1]) < len(final_index_set[t2])):
			to_form = t2
			left_att = at1
			right_att = at2
			col_idx = t1
			lines = form_table_from_index(table_list[t1], columns_collection[t1], final_index_set[t1])
			index_name = index_path + table_list[t2].split('.')[0] + '_attrib' + columns_collection[t2][at2].split('.')[-1] + ".npy"
			offsets = np.load(index_name).item()
		elif (len(final_index_set[t1]) > len(final_index_set[t2])):
			to_form = t1
			left_att = at2
			right_att = at1
			col_idx = t2
			lines = form_table_from_index(table_list[t2], columns_collection[t2], final_index_set[t2])
			index_name = index_path + table_list[t1].split('.')[0] + '_attrib' + columns_collection[t1][at1].split('.')[-1] + ".npy"
			offsets = np.load(index_name).item()
		

		if col_idx not in exsited:
			df = pd.DataFrame(lines)
			if (len(lines) > 0):
				df.columns = columns_collection[col_idx]
			out.append(df) 
			exsited.append(col_idx)

		join_index = []
		for line in lines:
			if line[left_att] in offsets:
				join_index.extend(offsets[line[left_att]])
		join_index = list(set(join_index))
		if len(final_index_set[to_form]) > 0:
			final_index_set[to_form] = list(set(final_index_set[to_form]).intersection(join_index))
		else:
			final_index_set[to_form] = join_index

		if to_form not in exsited:
			lines2 = form_table_from_index(table_list[to_form], columns_collection[to_form], final_index_set[to_form])
			df2 = pd.DataFrame(lines2)
			if (len(lines) > 0):
				df2.columns = columns_collection[to_form]
			out.append(df2)
			exsited.append(to_form)
	

	return out


def _from(table_lst, args, rename_list):

	conds = _where_parse(args)
	# print(conds)
	all_table_col_names = []
	columns_by_table = []
	for i,filename in enumerate(table_lst):
		with open(data_path + filename) as f:
			reader = csv.reader(f)
			columns = next(reader)
			if(rename_list[0] is not None):
				columns = [rename_list[i]+ '.' + s for s in list(columns)]

		all_table_col_names.extend(columns)
		columns_by_table.append(columns)
	# print(rename_list)
	# print(all_table_col_names)

	join_conds = []
	selection_conds = []
	#find join conditions and preprocess conditions
	for i in range(len(conds)):
		if conds[i] == '=' or conds[i] == '>' or conds[i] == '<' or conds[i] == '>=' or conds[i] == '<=' or conds[i] == '<>':
			if conds[i+1] in all_table_col_names:
				if conds[i] == "=":
					join_conds.append([conds[i-1], conds[i+1]])p
			else:
				temp = [conds[i-1], conds[i], conds[i+1]]
				if i < 2 or conds[i-2] == "AND":
					temp.append("AND")
				else:
					temp.append(conds[i-2])
				selection_conds.append(temp)
	# print(len(join_conds))
	temp = deepcopy(join_conds)
	#####naming issue:	

	preprocessd_tables = optimize_and_form_table(table_lst, columns_by_table,selection_conds, join_conds)
	# for table in preprocessd_tables: print(table)
	# print(preprocessd_tables[0])
	
	join_conds = temp
	# print("hi", join_conds)
	while(len(join_conds) > 0):
		#cond join
		col1, col2 = join_conds.pop()
		for i in range(len(preprocessd_tables)):

			if col1 in preprocessd_tables[i].columns:
				t1 = i
			if col2 in preprocessd_tables[i].columns:
				t2 = i


		# print(len(preprocessd_tables))
		prod = pd.merge(preprocessd_tables[t1], preprocessd_tables[t2], left_on = col1, right_on = col2)
		# print(prod)
		if t1 > t2:
			preprocessd_tables.pop(t1)
			preprocessd_tables.pop(t2)
		else:
			preprocessd_tables.pop(t2)
			preprocessd_tables.pop(t1)
		preprocessd_tables.append(prod)
	#cartesian product
	prod = preprocessd_tables[0]

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
	# print(table)
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
		table[op1] = pd.to_numeric(table[op1], errors='coerce')
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
			

	
	if op == "=" :
		# print(type(op2))
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



while(1):
	select_lst, from_lst, where_lst, rename_list, distinct = _parse()
	start_time = time.time()
	# after_read_lst_used = _select_used_tables(from_lst, after_read_lst, table_names)
	# _from(table_lst, args, rename_list)
	table = _from(from_lst, where_lst, rename_list)
	result = _select_and_where(table, where_lst, select_lst, distinct)
	duration = time.time() - start_time
	print(result)
	print("Number of Rows:", len(result))
	print("Duration", duration)
	# undo_rename(after_read_lst_used)







