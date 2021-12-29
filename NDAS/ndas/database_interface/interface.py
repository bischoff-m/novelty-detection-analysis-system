from datacollector.databases import read_sql_query
import pandas as pd


def density_per_patient(num_patients, db_name):
	df = read_sql_query(f'select patientid, entriesTotal from SMITH_ASIC_SCHEME.asic_lookup_{db_name} order by entriesTotal desc limit {num_patients}')
	return df.astype('int64')

# display(startInterface(["interface", "db_asic_scheme.json", "dataDensity", "bestPatients", "entriesTotal", 10, 'mimic']))


def density_per_parameter(parameter_list, num_patients, db_name):
	df = read_sql_query(f'select patientid, {parameter_list} from (select *, ({parameter_list.replace(",","+")}) as numberOfEntries from SMITH_ASIC_SCHEME.asic_lookup_{db_name} order by numberOfEntries desc limit {num_patients}) as sub')
	return df

# display(startInterface(['interface', 'db_asic_scheme.json', 'dataDensity', 'bestPatients', 'pao2, fio2_gemessen', '10', 'mimic']))


def select_patient(patientid, db_name):
	df = read_sql_query(f'select * from SMITH_ASIC_SCHEME.{db_name} where patientid = {patientid}', index_col='patientid', parse_dates=['time'])
	
	# normalize time column and convert to timestamp format
	df['time'] = df['time'].apply(lambda x: x.timestamp())
	df['time'] = df['time'] - df['time'].min()

	# get parameter names which will be the column names in the output dataframe
	cols_units = ["", "mmHg", "mmHg", "%", "", "°C", "mmHg", "cmH2O", "/min", "/min", "mmol/L", "mmol/L", "µmol/L", "U/L", "mL/cmH2O", "mmHg", "%", "mmol/L", "", "µmol/L", "mmol/L", "10^3/µL", "ng/mL", "mmHg", "mmHg", "%", "mmHg", "", "s", "mmHg", "mL/kg", "U/L", "mmHg", "mmHg", "L/min/m2", "µmol/L", "L/min", "pmol/L", "dyn.s/cm-5/m2", "mmHg", "ng/mL", "dyn.s/cm-5/m2", "cmH2O", "mmHg", "%", "nmol/L", "L/min", "L/min/m2", "ml/m2", "/min", "L/min", "%", "µg/kg/min", "mg/h", "mL/h", "mg/h", "µg/kg/min", "IE/min", "µg/kg/min", "µg/kg/min", "mg/h", "µg/h", "mg", "mg", "mg/h", "mg/h", "mg/h", "µg/kg/min", "mg", "mg", "mg", "mg/h", "µg", "µg/kg/h", "mg", "%", "µg/L", "10^3/µL", "mL", "U/L", "mmol/L", "U/L", "U/L", "ppm", "cmH2O", "", "mL/m2", "mL/Tag", "/min", "%", "", "cmH2O", "mL/kg", "cmH2O", "cmH2O", "cmH2O"]
	cols_units += [None] * (len(df.columns) - len(cols_units))
	df.columns = [f'{base}({unit})' if unit is not None else base for base, unit in zip(df.columns, cols_units)]
	
	return df
	
# startInterface(["interface", "db_asic_scheme.json", "selectPatient", str(271162), 'asic_data_sepsis'])