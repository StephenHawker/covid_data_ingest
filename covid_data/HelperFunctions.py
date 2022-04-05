"""HELPER Functions"""
import logging
import urllib.parse as urlparse
import datetime
import codecs
import configparser
import pandas as pd
import sqlalchemy


class HelperFunctions:
    ############################################################
    # constructor
    ############################################################
    def __init__(self):

        self.name = "Helper Functions"
        self.LOGGER = logging.getLogger(__name__)

    ############################################################
    # str
    ############################################################
    def __str__(self):

        return repr("Helper Functions")

    ############################################################
    # Load Config File
    ############################################################
    def load_config(self, file):
        """Load config file

        Keyword arguments:
        file -- config file path
        config -- config array
        """
        config = {}

        config = config.copy()
        cp = configparser.ConfigParser()
        cp.read(file)
        for sec in cp.sections():
            name = sec.lower()  # string.lower(sec)
            for opt in cp.options(sec):
                config[name + "." + opt.lower()] = cp.get(sec, opt).strip()
        return config

    ############################################################
    # Get query string value from link
    ############################################################
    def get_qs_value(self, url, query_string):
        """get query string from passed url query string

        Keyword arguments:
        url -- href link
        query_string -- query string key to search for
        """
        try:

            parsed = urlparse.urlparse(url)
            qs_value = urlparse.parse_qs(parsed.query)[query_string]

            for k in qs_value:
                return str(k)

        except KeyError:
            return ""
        except Exception:
            raise Exception("Error in get_qs_value - %s %s ", url, query_string)

    ############################################################
    # Get EPOCH days ahead
    ############################################################
    def next_weekday(self, d, weekday):

        days_ahead = weekday - d.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7

        return d + datetime.timedelta(days_ahead)


    ############################################################
    # Read file into Pandas DF
    ############################################################
    def read_file_to_df(self, filename, sep):

        df = pd.read_csv(filename, sep=sep, quotechar='"')
        return df

    ############################################################
    # Get dictionary of name : type from list for Data
    ############################################################
    def create_dtype(self, p_columns):

        dtype_data = dict()

        # iterating the columns
        for col in p_columns:
            print(col)
            dtype_data[col]= self.set_column_dtype(col) #sqlalchemy.types.VARCHAR(length=50)

        return dtype_data

    ############################################################
    # Get dictionary of name : type from list for Data
    ############################################################
    def set_column_dtype(self, p_column_name):

        if p_column_name in ['index'] :
            return sqlalchemy.types.INTEGER()

        elif p_column_name in ['covidOccupiedMVBeds', 'hospitalCases','newAdmissions','newCasesByPublishDate','newCasesBySpecimenDate',
                             'newCasesLFDOnlyBySpecimenDate','newDeaths28DaysByDeathDate','newCasesBySpecimenDateAgeDemographics.cases',
        'vaccinationsAgeDemographics.VaccineRegisterPopulationByVaccinationDate', 'vaccinationsAgeDemographics.cumPeopleVaccinatedCompleteByVaccinationDate',
        'vaccinationsAgeDemographics.newPeopleVaccinatedCompleteByVaccinationDate', 'vaccinationsAgeDemographics.cumPeopleVaccinatedFirstDoseByVaccinationDate',
        'vaccinationsAgeDemographics.newPeopleVaccinatedFirstDoseByVaccinationDate', 'vaccinationsAgeDemographics.cumPeopleVaccinatedSecondDoseByVaccinationDate',
        'vaccinationsAgeDemographics.newPeopleVaccinatedSecondDoseByVaccinationDate', 'vaccinationsAgeDemographics.cumPeopleVaccinatedThirdInjectionByVaccinationDate',
        'vaccinationsAgeDemographics.newPeopleVaccinatedThirdInjectionByVaccinationDate', 'vaccinationsAgeDemographics.cumVaccinationFirstDoseUptakeByVaccinationDatePercentage',
        'vaccinationsAgeDemographics.cumVaccinationSecondDoseUptakeByVaccinationDatePercentage', 'vaccinationsAgeDemographics.cumVaccinationThirdInjectionUptakeByVaccinationDatePercentage',
        'vaccinationsAgeDemographics.cumVaccinationCompleteCoverageByVaccinationDatePercentage','TriageCount','Total'] :
            return sqlalchemy.types.Float(precision=3, asdecimal=True)

        elif p_column_name in ['Covid_unscaled', 'Covid_monthly', 'isPartial', 'scale', 'Covid_Trend', 'seven_day_ago_Covid_Trend',
         'Coronavirus','Omicron','Covid']:
            return sqlalchemy.types.Float(precision=3, asdecimal=True)

        #Google mobility index
        elif p_column_name in ['retail_and_recreation_percent_change_from_baseline', 'grocery_and_pharmacy_percent_change_from_baseline', 'parks_percent_change_from_baseline',
                               'transit_stations_percent_change_from_baseline', 'workplaces_percent_change_from_baseline', 'residential_percent_change_from_baseline']:
            return sqlalchemy.types.Float(precision=3, asdecimal=True)

        elif p_column_name in ['date','AsAtDate','WeekEndDate','Call Date','journeydate']:
            return sqlalchemy.DateTime()

        elif p_column_name in ['areaCode','areaType','newCasesBySpecimenDateAgeDemographics.age']:
            return sqlalchemy.types.VARCHAR(length=50)

        elif p_column_name in ['areaName','CCGName','ccgname']:
            return sqlalchemy.types.VARCHAR(length=100)

        elif p_column_name in ['newCasesBySpecimenDateAgeDemographics.rollingRate','newCasesBySpecimenDateAgeDemographics.rollingSum',
                               'uniqueCasePositivityBySpecimenDateRollingSum']:
            return sqlalchemy.types.Float(precision=3, asdecimal=True)

        else:
            return sqlalchemy.types.VARCHAR(length=100)

    ############################################################
    # untangle_utf8
    ############################################################
    def untangle_utf8(self, match):
        """unicode issues...

        Keyword arguments:
        match -- json string with unicode issues...
        """
        escaped = match.group(0)                   # '\\u00e2\\u0082\\u00ac'
        hexstr = escaped.replace(r'\u00', '')      # 'e282ac'
        buffer = codecs.decode(hexstr, "hex")      # b'\xe2\x82\xac'

        try:
            return buffer.decode('utf8')           # '€'

        except UnicodeDecodeError:
            self.LOGGER.error("Could not decode buffer: %s" % buffer)

    ############################################################
    # Flatten a nested json dataframe
    ############################################################
    def flatten_nested_json_df(self, df):

        df = df.reset_index()

        print(f"original shape: {df.shape}")
        print(f"original columns: {df.columns}")


        # search for columns to explode/flatten
        s = (df.applymap(type) == list).all()
        list_columns = s[s].index.tolist()

        s = (df.applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()

        print(f"lists: {list_columns}, dicts: {dict_columns}")
        while len(list_columns) > 0 or len(dict_columns) > 0:
            new_columns = []

            for col in dict_columns:
                print(f"flattening: {col}")
                # explode dictionaries horizontally, adding new columns
                horiz_exploded = pd.json_normalize(df[col]).add_prefix(f'{col}.')
                horiz_exploded.index = df.index
                df = pd.concat([df, horiz_exploded], axis=1).drop(columns=[col])
                new_columns.extend(horiz_exploded.columns) # inplace

            for col in list_columns:
                print(f"exploding: {col}")
                # explode lists vertically, adding new columns
                df = df.drop(columns=[col]).join(df[col].explode().to_frame())
                new_columns.append(col)

            # check if there are still dict o list fields to flatten
            s = (df[new_columns].applymap(type) == list).all()
            list_columns = s[s].index.tolist()

            s = (df[new_columns].applymap(type) == dict).all()
            dict_columns = s[s].index.tolist()

            print(f"lists: {list_columns}, dicts: {dict_columns}")

        print(f"final shape: {df.shape}")
        print(f"final columns: {df.columns}")
        return df