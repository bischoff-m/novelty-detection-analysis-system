"""
*databases* - Fetch data from the SepsisDB database server.

The classes ``SepsisDB`` and ``MIMIC`` provide access to specific tables in the SMITH_SepsisDB database.

Before using for the first time: save passwords using ``save_passwords.py``
"""
if __name__ == '__main__':
    from generaltools import Timer, cache_path
else:
    from datacollector.generaltools import Timer, cache_path
import sshtunnel
import mysql.connector
import keyring
import pandas as pd
import os

datadir = cache_path()

ssh_ip = '137.226.78.84'
db_ip = '137.226.191.195'

ssh_user = keyring.get_password('ssh-i11', 'username')
ssh_pass = keyring.get_password('ssh-i11', 'password')
db_user = keyring.get_password('sepsisdb', 'username')
db_pass = keyring.get_password('sepsisdb', 'password')


def read_sql_query(query, print_time=True, **kwargs) -> pd.DataFrame:
    """
    Establishes a connection to the SMITH_SepsisDB database via SSH,
    runs a query and returns the retrieved data as a ``pandas.DataFrame``.
    :param query: The query string you want to send.
    :param kwargs: Additional parameters are passed to pandas.read_sql_query
    """
    if not (ssh_user and ssh_pass and db_user and db_pass):
        raise Exception('Failed to read credentials for ssh tunnel and database. Please save your credentials using the file "documentation/save_passwords.py"')
    
    tunnel = sshtunnel.SSHTunnelForwarder(
        (ssh_ip, 22),
        ssh_username=ssh_user,
        ssh_password=ssh_pass,
        remote_bind_address=(db_ip, 3306),
        local_bind_address=('127.0.0.1', 3306))
    tunnel.start()

    try:
        dbconn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            database='SMITH_SepsisDB',
            host='127.0.0.1',
            port=tunnel.local_bind_port)
    except Exception as exception:
        tunnel.close()
        raise exception
    timer = Timer()
    dataframe = pd.read_sql_query(query, dbconn, **kwargs)
    if print_time:
        print(f'SQL query done in {timer.elapsed()}')
    dbconn.close()
    tunnel.close()
    return dataframe


class SepsisDB:
    """
    This class provides static methods to get tables from the SMITH_SepsisDB database.
    Once a table is loaded from a file or fetched from the database, it is stored in the
    internal variable 'data' to provide faster access for future calls.
    To access the content of the database, always use the methods, not the variable.
    """
    data = {}
    
    @staticmethod
    def attributes():
        """
        returns the contents of the d_attribute table as a pandas.DataFrame;
        fetches data from d_attribute and saves query result in csv file for reuse;
        to fetch new data, even though a csv file exists, delete the file
        """
        d = SepsisDB.data
        filename = os.path.join(datadir, 'd_attribute.csv')

        if 'attributes' in d:
            return d['attributes']
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='attributeid')
        else:
            print(f'Fetching new attribute data file.')
            df = read_sql_query(f'SELECT * FROM SMITH_SepsisDB.d_attribute', index_col='attributeid')
            # query runs faster if column is dropped afterwards instead of not selecting it in the query
            df = df[df.conceptcode.notna()]
            df = df.filter(items=['shortlabel', 'longlabel', 'conceptlabel', 'conceptcode'])
            df.to_csv(filename)
        
        d['attributes'] = df
        return df

    @staticmethod
    def interventions():
        """
        returns the contents of the d_intervention table as a pandas.DataFrame;
        fetches data from d_intervention and saves query result in csv file for reuse;
        to fetch new data, even though a csv file exists, delete the file
        """
        d = SepsisDB.data
        filename = os.path.join(datadir, 'd_intervention.csv')

        if 'interventions' in d:
            return d['interventions']
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='interventionid')
        else:
            print(f'Fetching new intervention data file.')
            df = read_sql_query(f'SELECT * FROM SMITH_SepsisDB.d_intervention', index_col='interventionid')
            # query runs faster if columns are dropped afterwards instead of not selecting it in the query
            df = df.filter(items=['shortlabel', 'longlabel', 'type', 'conceptlabel', 'conceptcode'])
            df.to_csv(filename)
        
        d['interventions'] = df
        return df

    @staticmethod
    def encounters():
        """
        returns the contents of the d_encounter table as a pandas.DataFrame;
        fetches data from d_encounter and saves query result in csv file for reuse;
        to fetch new data, even though a csv file exists, delete the file
        """
        d = SepsisDB.data
        filename = os.path.join(datadir, 'd_encounter.csv')

        if 'encounters' in d:
            return d['encounters']
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='encounterid', parse_dates=['dateofbirth'])
        else:
            print(f'Fetching new encounter data file.')
            df = read_sql_query(f'SELECT * FROM SMITH_SepsisDB.d_encounter', index_col='encounterid', parse_dates=['dateofbirth'])
            # query runs faster if columns are dropped afterwards instead of not selecting it in the query
            df = df.filter(items=['patientid', 'dateofbirth', 'gender'])
            df.to_csv(filename)
        
        d['encounters'] = df
        return df
    
    @staticmethod
    def demographics(patientid):
        """
        returns all demographic data from ptdemographic table for a single patient as a pandas.DataFrame;
        fetches data from ptdemographic table and saves the query result in csv file for reuse;
        to fetch new data, even though a csv file exists, delete the file
        """
        d = SepsisDB.data
        filename = os.path.join(datadir, f'{patientid}_ptdemographic.csv')

        if 'demographics' not in d:
            d['demographics'] = {}
        
        if patientid in d['demographics']:
            return d['demographics'][patientid]
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='ptdemographicid', parse_dates=['charttime'])
        else:
            print(f'Fetching new demographic data for patientid {patientid}.')
            enc = SepsisDB.encounters()
            encounterids = tuple(enc[enc['patientid'] == patientid].index)
            if len(encounterids) == 0:
                raise Exception(f'No encounter found for patient with ID {patientid}')
            elif len(encounterids) == 1:
                query = f'SELECT * FROM SMITH_SepsisDB.ptdemographic WHERE encounterid = {encounterids[0]}'
            else:
                query = f'SELECT * FROM SMITH_SepsisDB.ptdemographic WHERE encounterid in {encounterids}'
            
            df = read_sql_query(query, index_col='ptdemographicid', parse_dates=['charttime'])
            # query runs faster if columns are dropped afterwards instead of not selecting it in the query
            df = df.filter(items=['interventionid', 'attributeid', 'charttime', 'terseform'])
            df.to_csv(filename)
        
        d['demographics'][patientid] = df
        return df
    
    @staticmethod
    def assessments(patientid, validatedonly=False):
        """
        returns all assessments from ptassessment table for a single patient as a pandas.DataFrame;
        fetches data from ptassessment table and joins it with data from d_validation_pta;
        saves the query result in csv file for reuse;
        to fetch new data, even though a csv file exists, delete the file
        """
        d = SepsisDB.data
        filename = os.path.join(datadir, f'{patientid}_ptassessment.csv')

        if 'assessments' not in d:
            d['assessments'] = {}
        
        if patientid in d['assessments']:
            df = d['assessments'][patientid]
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='ptassessmentid', parse_dates=['charttime'])
        else:
            print(f'Fetching new assessment data for patientid {patientid}.')
            enc = SepsisDB.encounters()
            encounterids = tuple(enc[enc['patientid'] == patientid].index)
            if len(encounterids) == 0:
                raise Exception(f'No encounter found for patient with ID {patientid}')
            elif len(encounterids) == 1:
                pta_query = f'SELECT * FROM SMITH_SepsisDB.ptassessment WHERE encounterid = {encounterids[0]}'
            else:
                pta_query = f'SELECT * FROM SMITH_SepsisDB.ptassessment WHERE encounterid in {encounterids}'
            
            # fetch assessment dataframe
            assessments = read_sql_query(pta_query, index_col='ptassessmentid', parse_dates=['charttime'])
            assessments = assessments.filter(items=['interventionid', 'attributeid', 'charttime', 'terseform'])
            
            # fetch validation dataframe
            val_query = f'SELECT * FROM SMITH_SepsisDB.d_validation_pta WHERE ptassessmentid in {tuple(assessments.index)}'
            validations = read_sql_query(val_query, index_col='validation_pta_id')

            # join both dataframes and save to file
            df = pd.merge(assessments, validations, how='left', on='ptassessmentid', validate='one_to_one')
            df.set_index('ptassessmentid', inplace=True)
            df.to_csv(filename)
        
        d['assessments'][patientid] = df
        if validatedonly:
            # drop all entries, where isunlikely == 1
            df = df[~(df['isunlikely'] == 1)]
            df.drop(columns=['isunlikely'], axis=1, inplace=True)
        return df
    
    @staticmethod
    def labresults(patientid, validatedonly=False):
        """
        returns all labresults from ptlabresult table for a single patient as a pandas.DataFrame;
        fetches data from ptlabresult table and joins it with data from d_validation_lab;
        saves the query result in csv file for reuse;
        to fetch new data, even though a csv file exists, delete the file
        """
        d = SepsisDB.data
        filename = os.path.join(datadir, f'{patientid}_ptlabresult.csv')

        if 'labresults' not in d:
            d['labresults'] = {}
        
        if patientid in d['labresults']:
            df = d['labresults'][patientid]
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='ptlabresultid', parse_dates=['charttime'])
        else:
            print(f'Fetching new labresult data for patientid {patientid}.')
            enc = SepsisDB.encounters()
            encounterids = tuple(enc[enc['patientid'] == patientid].index)
            if len(encounterids) == 0:
                raise Exception(f'No encounter found for patient with ID {patientid}')
            elif len(encounterids) == 1:
                lab_query = f'SELECT * FROM SMITH_SepsisDB.ptlabresult WHERE encounterid = {encounterids[0]}'
            else:
                lab_query = f'SELECT * FROM SMITH_SepsisDB.ptlabresult WHERE encounterid in {encounterids}'
            
            # fetch labresults dataframe
            labresults = read_sql_query(lab_query, index_col='ptlabresultid', parse_dates=['charttime'])
            labresults = labresults.filter(items=['interventionid', 'attributeid', 'charttime', 'terseform'])
            
            # fetch validation dataframe
            val_query = f'SELECT * FROM SMITH_SepsisDB.d_validation_lab WHERE ptlabresultid in {tuple(labresults.index)}'
            validations = read_sql_query(val_query, index_col='validation_lab_id')

            # join both dataframes and save to file
            df = pd.merge(labresults, validations, how='left', on='ptlabresultid', validate='one_to_one')
            df.set_index('ptlabresultid', inplace=True)
            df.to_csv(filename)
        
        d['labresults'][patientid] = df
        if validatedonly:
            # drop all entries, where isunlikely == 1
            df = df[~(df['isunlikely'] == 1)]
            df.drop(columns=['isunlikely'], axis=1, inplace=True)
        return df
    
    @staticmethod
    def medication(patientid):
        """
        returns all medications from ptmedication table for a single patient as a pandas.DataFrame;
        fetches data from ptmedication table and saves the query result in csv file for reuse;
        to fetch new data, even though a csv file exists, delete the file
        """
        d = SepsisDB.data
        filename = os.path.join(datadir, f'{patientid}_ptmedication.csv')

        if 'medication' not in d:
            d['medication'] = {}
        
        if patientid in d['medication']:
            return d['medication'][patientid]
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='ptmedicationid', parse_dates=['charttime'])
        else:
            print(f'Fetching new medication data for patientid {patientid}.')
            enc = SepsisDB.encounters()
            encounterids = tuple(enc[enc['patientid'] == patientid].index)
            if len(encounterids) == 0:
                raise Exception(f'No encounter found for patient with ID {patientid}')
            elif len(encounterids) == 1:
                query = f'SELECT * FROM SMITH_SepsisDB.ptmedication WHERE encounterid = {encounterids[0]}'
            else:
                query = f'SELECT * FROM SMITH_SepsisDB.ptmedication WHERE encounterid in {encounterids}'
            
            # fetch and save medication dataframe
            df = read_sql_query(query, index_col='ptmedicationid', parse_dates=['charttime'])
            df = df.filter(items=['interventionid', 'attributeid', 'charttime', 'terseform'])
            df.to_csv(filename)
        
        d['medication'][patientid] = df
        return df


class Mimic:
    """
    TODO: write documentation
    TODO: revise print statements
    TODO: revise caching
    """
    data = {}
    
    @staticmethod
    def patients():
        d = Mimic.data
        filename = os.path.join(datadir, 'mimic_patients.csv')

        if 'patients' in d:
            return d['patients']
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='row_id')
        else:
            print(f'Fetching new subject data file.')
            df = read_sql_query(f'SELECT * FROM `SMITH_MIMIC/3`.patients', index_col='row_id', parse_dates=['dob', 'dod'])
            df = df.filter(items=['row_id', 'subject_id', 'gender', 'dob', 'dod'])
            df.to_csv(filename)
        
        d['patients'] = df
        return df
    
    @staticmethod
    def chartevents(subject_id):
        d = Mimic.data
        filename = os.path.join(datadir, f'mimic_chartevents_{subject_id}.csv')
        
        if 'chartevents' not in d:
            d['chartevents'] = {}
        
        if subject_id in d['chartevents']:
            return d['chartevents'][subject_id]
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='row_id', parse_dates=['charttime'])
        else:
            print(f'Fetching new chartevents data for subject_id {subject_id}.')
            query = f'SELECT * FROM `SMITH_MIMIC/3`.chartevents WHERE subject_id = {subject_id}'
            df = read_sql_query(query, index_col='row_id', parse_dates=['charttime'])
            df = df.filter(items=['row_id', 'itemid', 'charttime', 'value', 'valuenum', 'valueuom'])
            df.to_csv(filename)
        
        d['chartevents'][subject_id] = df
        return df
    
    @staticmethod
    def labevents(subject_id):
        d = Mimic.data
        filename = os.path.join(datadir, f'mimic_labevents_{subject_id}.csv')
        
        if 'labevents' not in d:
            d['labevents'] = {}
        
        if subject_id in d['labevents']:
            return d['labevents'][subject_id]
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='row_id', parse_dates=['charttime'])
        else:
            print(f'Fetching new labevents data for subject_id {subject_id}.')
            query = f'SELECT * FROM `SMITH_MIMIC/3`.labevents WHERE subject_id = {subject_id}'
            df = read_sql_query(query, index_col='row_id', parse_dates=['charttime'])
            df = df.filter(items=['row_id', 'itemid', 'charttime', 'value', 'valuenum', 'valueuom'])
            df.to_csv(filename)
        
        d['labevents'][subject_id] = df
        return df
    
    @staticmethod
    def inputevents_mv():
        d = Mimic.data
        filename = os.path.join(datadir, 'mimic_inputevents_mv.csv')

        if 'inputevents_mv' in d:
            return d['inputevents_mv']
        elif os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='row_id')
        else:
            print(f'Fetching new inputevents_mv data file.')
            df = read_sql_query(f'SELECT * FROM `SMITH_MIMIC/3`.inputevents_mv', index_col='row_id', parse_dates=['starttime', 'endtime', 'storetime', 'comments_date'])
            df.to_csv(filename)
        
        d['inputevents_mv'] = df
        return df