import sys


def parser():
	attrib = input("SELECT ")
	table = input("FROM ")
	condition = input("WHERE ")

	attrib = attrib.replace(" ", "")
	table = table.replace(" ", "")

	_select = []
	_from = []
	_where = [] 
	# select all 
	if attrib == "*":
		print("select all")
		_select.append("*")
	# select only one attribute
	elif attrib.find(",") == -1:
		print("only one attrib")
		_select.append(attrib)
	# select more than one attrib
	else:
		print("multiple attrib")
		_select = attrib.split(",")

	# from none table
	if table == "":
		print("ERROR: from clause cannot be empty")
		return
	# from one table
	elif table.find(",") == -1:
		print("only one table")
		_from.append(table)
	# from more than one table
	else:
		print("multiple table")
		_from = table.split(",")

	
	print("SELECT: ", _select)
	print("FROM: ", _from)





def main():
	parser()


if __name__== "__main__":
	main()