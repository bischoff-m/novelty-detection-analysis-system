# %%
from collections import namedtuple
import sys
import csv
import time
import mysql.connector
import json
from datetime import datetime
import os
import paramiko

from datacollector.databases import read_sql_query
import pandas as pd

currentPath = os.path.dirname(__file__)

Table = namedtuple('Table', ['name', 'type', 'relevant_columns', 'parameter_identifier_column'])
tables = []
with open("./Interface_tables.csv") as parameter_csv:
	csv_reader_object = csv.reader(parameter_csv, delimiter=";")
	firstLineFlag = True
	for row in csv_reader_object:
		if firstLineFlag:
			firstLineFlag = False
			continue
		tables.append(Table(name=row[0], type=int(row[1]), relevant_columns=row[2], parameter_identifier_column=row[3]))

Parameter = namedtuple('Parameter', ['name','database', 'tables', 'parameter_identifier'])
parameters = []
with open("./Interface_parameter.csv") as parameter_csv:
	csv_reader_object = csv.reader(parameter_csv, delimiter=";")
	firstLineFlag = True
	for row in csv_reader_object:
		if firstLineFlag:
			firstLineFlag = False
			continue
		parameterTables = row[2].split(",")
		tableReferences = []
		for table in tables:
			for parameterTable in parameterTables:
				if table.name == parameterTable:
					tableReferences.append(table)
		if len(tableReferences) == 0:
			print("Error reading parameter table: Table(s) not found in parameter " + row[0] + " (Database " + row[1] + ")")
			continue
		if tableReferences[0].type == 1:
			parameterIdentifier = row[3].split(",")
			parameterIdentifier = list(map(int, parameterIdentifier))
			parameters.append(Parameter(name=row[0], database=row[1], tables=tableReferences, parameter_identifier=parameterIdentifier))
		elif tableReferences[0].type == 2:
			parameters.append(Parameter(name=row[0], database=row[1], tables=tableReferences, parameter_identifier=row[3]))


def startInterface(argv):
	########## SSH LOGIN ##########
	# sshLoginDataFile = open("../local_data/sshSettings.json")
	# sshLoginData = json.load(sshLoginDataFile)

	# databaseConfigurationFile = open("../local_data/db_asic_scheme.json")
	# databaseConfiguration = json.load(databaseConfigurationFile)
	# # Establish ssh connection to the database server
	# host = "137.226.78.84"
	# port = 22
	# username = sshLoginData["username"]
	# password = sshLoginData["password"]
	# ssh = paramiko.SSHClient()
	# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	# ssh.connect(host, port, username, password)
	###############################
	ssh = None
	databaseConfiguration = None

	# TODO: need to call ssh.close() (?)

	# Calls
	# loadPatient:					startInterface(["interface", "db_asic_scheme.json", "selectPatient", str(patientid), database])
	# showPatients:					startInterface(["interface", "db_asic_scheme.json", "dataDensity", "bestPatients", "entriesTotal", numberOfPatients, db])
	# showPatientsWithParameter:	startInterface(["interface", "db_asic_scheme.json", "dataDensity", "bestPatients", self.parameters, numberOfPatients, db])
	
	# TODO: in some cases, the ssh connection is left open
	if argv[2] == "selectPatient":
		res = selectPatient(argv, ssh, databaseConfiguration)
	elif argv[2] == "dataDensity":
		if argv[4] == "entriesTotal":
			res = density_per_patient(argv)
		else:
			res = density_per_parameter(argv, ssh, databaseConfiguration)
	else:
		res = None
	
	if ssh:
		ssh.close()
	return res

# %%

def density_per_patient(argv):
	df = read_sql_query(f'select patientid, entriesTotal from SMITH_ASIC_SCHEME.asic_lookup_{argv[6]} order by entriesTotal desc limit {argv[5]}')
	return '\n'.join(df[['patientid', 'entriesTotal']].astype(str).agg(' | '.join, axis=1))

display(startInterface(["interface", "db_asic_scheme.json", "dataDensity", "bestPatients", "entriesTotal", 10, 'mimic']))


# %%



# %%

def density_per_parameter(argv, ssh, databaseConfiguration):
	# search for the patient who has the most entries in the given table for the specified parameters
	stdin, stdout, stderr = ssh.exec_command('mysql -h{} -u{} -p{} SMITH_SepsisDB -e "select patientid, {} from (select *, ({}) as numberOfEntries from SMITH_ASIC_SCHEME.asic_lookup_{} order by numberOfEntries desc limit {}) as sub;"'.format(databaseConfiguration['host'], databaseConfiguration['username'], databaseConfiguration['password'],argv[4], argv[4].replace(",","+"), argv[6], argv[5]))
	if stderr.readlines() != []:
		print(stderr.readlines())
		return -2
	results = stdout.readlines()
	if results[1:] == []:
		return -1	
	return results

# %%

def selectPatient(argv, ssh, databaseConfiguration):
	# selects the patient from the given table (argv[4]) with the specified patient id (argv[3])
	stdin, stdout, stderr = ssh.exec_command('mysql -h{} -u{} -p{} SMITH_SepsisDB -e "show columns from SMITH_ASIC_SCHEME.{}"'.format(databaseConfiguration['host'], databaseConfiguration['username'], databaseConfiguration['password'], argv[4]))
	columnNames = stdout.readlines()
	columnNames = columnNames[2:]
	firstLine = []
	units = ["", "mmHg", "mmHg", "%", "", "°C", "mmHg", "cmH2O", "/min", "/min", "mmol/L", "mmol/L", "µmol/L", "U/L", "mL/cmH2O", "mmHg", "%", "mmol/L", "", "µmol/L", "mmol/L", "10^3/µL", "ng/mL", "mmHg", "mmHg", "%", "mmHg", "", "s", "mmHg", "mL/kg", "U/L", "mmHg", "mmHg", "L/min/m2", "µmol/L", "L/min", "pmol/L", "dyn.s/cm-5/m2", "mmHg", "ng/mL", "dyn.s/cm-5/m2", "cmH2O", "mmHg", "%", "nmol/L", "L/min", "L/min/m2", "ml/m2", "/min", "L/min", "%", "µg/kg/min", "mg/h", "mL/h", "mg/h", "µg/kg/min", "IE/min", "µg/kg/min", "µg/kg/min", "mg/h", "µg/h", "mg", "mg", "mg/h", "mg/h", "mg/h", "µg/kg/min", "mg", "mg", "mg", "mg/h", "µg", "µg/kg/h", "mg", "%", "µg/L", "10^3/µL", "mL", "U/L", "mmol/L", "U/L", "U/L", "ppm", "cmH2O", "", "mL/m2", "mL/Tag", "/min", "%", "", "cmH2O", "mL/kg", "cmH2O", "cmH2O", "cmH2O"]
	index = 0
	for name in columnNames:
		print(name)
		name = name.split()
		if index < len(units):
			firstLine.append(name[0] + "(" + units[index] + ")")
		else:
			firstLine.append(name[0])
		index+=1
	stdin, stdout, stderr = ssh.exec_command('mysql -h{} -u{} -p{} SMITH_SepsisDB -e "select * from SMITH_ASIC_SCHEME.{} where patientid = {}"'.format(databaseConfiguration['host'], databaseConfiguration['username'], databaseConfiguration['password'], argv[4], argv[3]))
	result = stdout.readlines()
	result = result[1:]
	if result == []:
		return -1
	convertedRowsTemp = []
	smallestTimestamp = -1
	for row in result:
		row = row.split("\t")
		row = row[1:]
		temp = list(row)
		temp[0] = datetime.strptime(temp[0], "%Y-%m-%d %H:%M:%S").timestamp()
		if temp[0] < smallestTimestamp or smallestTimestamp == -1:
			smallestTimestamp = temp[0]
		row = tuple(temp)
		convertedRowsTemp.append(row)
		convertedRows = [] 
	for row in convertedRowsTemp:
		temp = list(row)
		temp[0] = temp[0] - smallestTimestamp
		convertedRows.append(tuple(temp))

	if not os.path.exists(os.getcwd() + "/ndas/local_data/imported_patients"):
		os.makedirs(os.getcwd() + "/ndas/local_data/imported_patients")
	filename = os.getcwd()+"/ndas/local_data/imported_patients/{}_patient_{}.csv".format(argv[4], argv[3])
	file = open(filename, 'w')
	writer = csv.writer(file, delimiter=";", quoting=csv.QUOTE_ALL)
	writer.writerow(firstLine)
	for line in convertedRows:
		newLine = list(line)
		writer.writerow(newLine)
	
	ssh.close()
# %%
