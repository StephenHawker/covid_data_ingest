"""Database Access"""
import urllib
import pyodbc
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import logging
from HelperFunctions import HelperFunctions

class DbAccess:


    DRIVER = 'DSN=covid_data;' # DRIVER={ODBC Driver 17 for SQL Server};
    SERVER = 'SERVER=HAWK-PC\HAWK_SQL2017,1433;'
    DATABASE = 'DATABASE=covid_data;'
    USERNAME = r'UID=covid_data;'
    PASSWORD = r'PWD=covid_data'

    ############################################################
    # str
    ############################################################
    def __str__(self):

        return repr("Database Functions")

    ############################################################
    # Constructor
    ############################################################
    def __init__(self, dsn):

        #self.cnxn = pyodbc.connect(self.DRIVER + self.SERVER + self.DATABASE + self.USERNAME + self.PASSWORD)
        #self.cnxn = pyodbc.connect(self.DRIVER + self.SERVER + self.DATABASE)
        #self.dsnname = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=HAWK-PC\HAWK_SQL2017;DATABASE=covid_data;UID=covid_data;PWD=covid_data")
        self.dsnname = 'covid_data'
        self.dsn = 'DSN=' + self.dsnname + ';'
        self.cnxn = pyodbc.connect(self.dsn)
        self.cursor = self.cnxn.cursor()
        self.helper_f = HelperFunctions()
        self.LOGGER = logging.getLogger(__name__)

    ############################################################
    # on create
    ############################################################
    def __enter__(self):

       return self

    ############################################################
    # on destroy
    ############################################################
    def __exit__(self, exc_type, exc_value, traceback):
        print("__exit__")
        self.cursor.close()
        self.cnxn.close()

    ############################################################
    # execute
    ############################################################
    def execute_query(self, query, data_frame=True):
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        self.column_names = [i[0] for i in self.cursor.description]

        if not data_frame:
            return results
        else:
            return self._build_data_frame(results, self.column_names)

    ############################################################
    # Execute update
    ############################################################
    def executeupd(self, query, reclist):

        try:
            self.cursor.execute(query, reclist)
            self.cnxn.commit()


        except pyodbc.Error as ex:
            raise Exception("Error updating to database : " + str(ex.args))
        except Exception:
            raise Exception("Error updating to database.")
        finally:
            None

    ############################################################
    # Build data frame
    ############################################################
    def _build_data_frame(self, data, column_names):
        dictionary = {str(column): [] for column in column_names}
        for data_row in data:
            for i, data_point in enumerate(data_row):
                dictionary[column_names[i]].append(data_point)
        return pd.DataFrame.from_dict(dictionary)

    ############################################################
    # Parse Query
    ############################################################
    def _parse_query(self, query):
        parsed_string = query.split()
        column_names = []
        for string in parsed_string:
            string = string.strip().replace(',', '').replace('[', '').replace(']', '')
            if string.upper() == 'SELECT':
                pass
            elif 'FROM' in string.upper():
                if '*' in column_names:
                    raise Exception('cannot build dataframe with arbitrary column names...')
                return column_names
            else:
                # split removes table names from joined queries...
                column_names.append(string.split('.')[-1])

    ############################################################
    # Load data frame to table
    ############################################################
    def load_data_frame_to_table(self, p_df, p_table_name, p_dtypes, p_chunksize=200):

        self.LOGGER.debug("load_data_frame_to_table: ")
        quoted = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=HAWK-PC\HAWK_SQL2017;DATABASE=covid_data;UID=covid_data;PWD=covid_data")
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted), fast_executemany=True)

        self.LOGGER.debug("load_data_frame_to_table: loading to table %s", str(p_table_name))
        #, method='multi'
        p_df.to_sql(p_table_name, schema='dbo', con=engine, chunksize=p_chunksize, index=False, if_exists='replace', dtype=p_dtypes)

        result = engine.execute('SELECT COUNT(*) FROM [dbo].[' + p_table_name + ']')
        result.fetchall()
