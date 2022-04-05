import os
import sys
import logging.config
import simplejson as json
import pandas
import glob
import sqlalchemy
from PageScraper import PageScraper
from WebFileDownload import WebFileDownload
from UKCov19 import UKCov19
from DbAccess import DbAccess
from APILookup import APILookup
from DistanceCalc import DistanceCalc
from HelperFunctions import HelperFunctions
from GetNearest import GetNearest
from collections import defaultdict
from datetime import date
import datetime
from GoogleTrends import GoogleTrends
from zipfile import ZipFile
"""Read Firestations web page data """
"""Project Classes"""
""" Other Imports"""


class DatabaseLoadError(Exception):
    """Database Exception

    Keyword arguments:
    Exception -- Exception
    """
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return repr(self.data)


class DataIngestError(Exception):
    """FirestationIngestError Exception

    Keyword arguments:
    Exception -- Exception
    """
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return repr(self.data)


############################################################
# Main
############################################################
def main():
    """Main process

    """
    try:

        LOGGER.info('Started run. main:')
        SCOT_VACCINE_DATA_URL = configImport["covid_data.url_scot_vaccine_supply"]
        ZOE_COVID_DATA_URL = configImport["covid_data.url_covid_zoe_data"]
        TEMP_FILE = configImport["covid_data.temp_file"]
        ZOE_COVID_HTML_FILE = configImport["covid_data.zoe_covid_html_file"]
        TABLE_CLASS = configImport["covid_data.key_marker"] ##"ds_accordion-item__body"

        uodf_regionaldemographiccases = pandas.DataFrame
        uodf_regionalhospdata = pandas.DataFrame
        uodf_ukdata = pandas.DataFrame
        uodf_engdata = pandas.DataFrame
        uodf_engreinfectiondata = pandas.DataFrame
        uodf_scotdata = pandas.DataFrame
        uodf_walesdata = pandas.DataFrame
        uodf_northern_ireland = pandas.DataFrame
        uodf_engcasedemodata = pandas.DataFrame
        uodf_regioncasedata = pandas.DataFrame
        uodf_utlademodata = pandas.DataFrame
        uodf_regionvaxdemodata = pandas.DataFrame
        uodf_nhstrusthospdata = pandas.DataFrame
        uodf = pandas.DataFrame
        uodf_googlemobindex = pandas.DataFrame
        uo_DBInsert = DbAccess('covid_data')
        ukapi = UKCov19()
        """firestation_nearest_json_with_travel = configImport["firestation.json_file_with_travel"]"""

        '''
        Get Google Trend Data
        '''
        get_Google_Trend_data()

        '''
        Get File Based Data 
        '''
        get_File_data()
        #exit(0)

        #Unzip google mobility regional data and place into subdirectory
        unzip_files('Region_Mobility_Report_CSVs.zip', 'GB_Region_Mobility_Report')
        directory = os.getcwd()
        path = directory + '\google_mob_data\\'
        #Get all unzipped mobility csv data
        all_files = glob.glob(path + "/*.csv")
        li = []

        #Loop through all mobility csvs, read into a dataframe and make a list of data frames.
        #These can then be concatenated together for loading to db
        for filename in all_files:
            df = UO_HELPER.read_file_to_df(filename, ',')
            li.append(df)

        uodf_googlemobindex = pandas.concat(li, axis=0, ignore_index=True)
        uodf_googlemobindex_dtype = UO_HELPER.create_dtype(uodf_googlemobindex.head())
        uo_DBInsert.load_data_frame_to_table(uodf_googlemobindex, 'uodf_googlemobindex', uodf_googlemobindex_dtype)


        LOGGER.debug("scot_vaccine_data_url  : %s ", SCOT_VACCINE_DATA_URL)
        # LOGGER.debug("Postcode api URL : %s ", POSTCODE_API_URL)
        LOGGER.debug("TEMP_FILE : %s ", TEMP_FILE)
        # LOGGER.debug("csv_file : %s ", CSV_FILE)
        # LOGGER.debug("number_closest : %s ", str(NUMBER_CLOSEST))

        fs_pagescraper = PageScraper(SCOT_VACCINE_DATA_URL)
        fs_zoe_pagescraper = PageScraper(ZOE_COVID_DATA_URL)
        html_data = fs_pagescraper.get_pagedata_ro('search')
        zoe_html_data = fs_zoe_pagescraper.get_pagedata_ro('search')

        """TODO Deal with changes and updates"""

        fs_pagescraper.write_table_to_temp(TEMP_FILE)
        fs_zoe_pagescraper.write_table_to_temp(ZOE_COVID_HTML_FILE)
        zoe_table = fs_zoe_pagescraper.get_table("summary_box")
        zoe_table = fs_zoe_pagescraper.html_table
        the_table = fs_pagescraper.get_table(TABLE_CLASS)
        the_table = fs_pagescraper.html_table


        ##fs_pagescraper.save_tab_as_list()


        #uodf_regionvaxdemodata = ukapi.get_vax_demo_data('region')
        #uodf_regionvaxdemodata.to_csv('uodf_regionvaxdemodata.csv')
        #uodf_regionvaxdemodata_dtype = UO_HELPER.create_dtype(uodf_regionvaxdemodata.head())
        #uo_DBInsert.load_data_frame_to_table(uodf_regionvaxdemodata, 'uodf_regionvaxdemodata',uodf_regionvaxdemodata_dtype)


        uodf_regionaldemographiccases = ukapi.get_case_demo_data('region')

        uodf_engcasedemodata = ukapi.get_case_demo_data('nation')
        uodf_regionalhospdata = ukapi.get_region_hosp_data('nhsRegion')
        uodf_nhstrusthospdata = ukapi.get_region_hosp_data('nhsTrust')
        uodf_ukdata = ukapi.get_data_uk()
        uodf_engdata = ukapi.get_data_country('England')
        uodf_scotdata = ukapi.get_data_country('Scotland')
        uodf_walesdata = ukapi.get_data_country('Wales')
        uodf_northern_ireland = ukapi.get_data_country('Northern Ireland')
        uodf_engreinfectiondata = ukapi.get_data_reinfectioncases('nation')
        uodf_regionalpositivitydata = ukapi.get_case_positivity_data('region')
        uodf_regioncasedata = ukapi.get_data_cases('region')
        uodf_regionvaxdemodata = ukapi.get_vax_demo_data('region')
        uodf_ltladata = ukapi.get_data_cases('ltla')
        #uodf_utlademodata= ukapi.get_case_demo_data('utla')


        uodf_regionaldemographiccases.to_csv('uodf_regionaldemographiccases.csv')
        uodf_regionalhospdata.to_csv('uodf_regionalhospdata.csv')
        uodf_nhstrusthospdata.to_csv('uodf_nhstrusthospdata.csv')
        uodf_engcasedemodata.to_csv('uodf_engcasedemodata.csv')
        uodf_ukdata.to_csv('uodf_ukdata.csv')
        uodf_engdata.to_csv('uodf_engdata.csv')
        uodf_scotdata.to_csv('uodf_scotland.csv')
        uodf_walesdata.to_csv('uodf_walesdata.csv')
        uodf_northern_ireland.to_csv('uodf_northern_ireland.csv')
        uodf_engreinfectiondata.to_csv('uodf_engreinfectiondata.csv')
        uodf_regionalpositivitydata.to_csv('uodf_regionalpositivitydata.csv')
        uodf_regioncasedata.to_csv('uodf_regioncasedata.csv')
        #uodf_utlademodata.to_csv('uodf_utlademodata.csv')
        uodf_regionvaxdemodata.to_csv('uodf_regionvaxdemodata.csv')
        uodf_ltladata.to_csv('uodf_ltladata.csv')

        uodf_regionaldemographiccases_dtype = UO_HELPER.create_dtype(uodf_regionaldemographiccases.head())
        uo_DBInsert.load_data_frame_to_table(uodf_regionaldemographiccases, 'uodf_regionaldemographiccases',uodf_regionaldemographiccases_dtype)

        uodf_engcasedemodata_dtype = UO_HELPER.create_dtype(uodf_engcasedemodata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_engcasedemodata, 'uodf_engcasedemodata', uodf_engcasedemodata_dtype)

        uodf_regionalhospdata_dtype = UO_HELPER.create_dtype(uodf_regionalhospdata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_regionalhospdata, 'uodf_regionalhospdata', uodf_regionalhospdata_dtype)

        uodf_nhstrusthospdata_dtype = UO_HELPER.create_dtype(uodf_nhstrusthospdata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_nhstrusthospdata, 'uodf_nhstrusthospdata', uodf_nhstrusthospdata_dtype)



        uodf_ukdata_dtype = UO_HELPER.create_dtype(uodf_ukdata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_ukdata, 'uodf_ukdata', uodf_ukdata_dtype)

        uodf_engdata_dtype = UO_HELPER.create_dtype(uodf_engdata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_engdata, 'uodf_engdata', uodf_engdata_dtype)

        uodf_scotdata_dtype = UO_HELPER.create_dtype(uodf_scotdata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_scotdata, 'uodf_scotdata', uodf_scotdata_dtype)

        uodf_walesdata_dtype = UO_HELPER.create_dtype(uodf_walesdata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_walesdata, 'uodf_walesdata', uodf_walesdata_dtype)

        uodf_engreinfectiondata_dtype = UO_HELPER.create_dtype(uodf_engreinfectiondata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_engreinfectiondata, 'uodf_engreinfectiondata', uodf_engreinfectiondata_dtype)

        uodf_northern_ireland_dtype = UO_HELPER.create_dtype(uodf_northern_ireland.head())
        uo_DBInsert.load_data_frame_to_table(uodf_northern_ireland, 'uodf_northern_ireland', uodf_northern_ireland_dtype)

        uodf_regionalpositivitydata_dtype = UO_HELPER.create_dtype(uodf_regionalpositivitydata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_regionalpositivitydata, 'uodf_regionalpositivitydata', uodf_regionalpositivitydata_dtype)

        uodf_regioncasedata_dtype = UO_HELPER.create_dtype(uodf_regioncasedata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_regioncasedata, 'uodf_regioncasedata', uodf_regioncasedata_dtype)
        #uo_DBInsert.load_data_frame_to_table(uodf_utlademodata, 'uodf_utlademodata')

        uodf_regionvaxdemodata_dtype = UO_HELPER.create_dtype(uodf_regionvaxdemodata.head())
        #uo_DBInsert.load_data_frame_to_table(uodf_regionvaxdemodata, 'uodf_regionvaxdemodata',uodf_regionvaxdemodata_dtype)

        uodf_ltladata_dtype = UO_HELPER.create_dtype(uodf_ltladata.head())
        uo_DBInsert.load_data_frame_to_table(uodf_ltladata, 'uodf_ltladata', uodf_ltladata_dtype)

        #5M rows LTLA demo data so do this last
        uodf_ltlademographiccases = ukapi.get_case_demo_data('ltla')
        uodf_ltlademographiccases_dtype = UO_HELPER.create_dtype(uodf_ltlademographiccases.head())
        uo_DBInsert.load_data_frame_to_table(uodf_ltlademographiccases, 'uodf_ltlademographiccases',uodf_ltlademographiccases_dtype)


        LOGGER.info('Completed run.')

    except DataIngestError as recex:
        LOGGER.error("An Exception occurred Covid Data Ingest  ")
        LOGGER.error(recex.data)
        raise DataIngestError(recex)

    except Exception:
        LOGGER.error("An Exception occurred Covid Data Ingest ")
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("!")



############################################################
# Get File based Web Data
############################################################
def get_File_data():
    """File Data

    """
    try:

        # Get 111 and NHS pathways spreadsheets
        uoWFD = WebFileDownload()
        uoWFDAppleMobility = WebFileDownload()
        uoWFDGoogleMobility = WebFileDownload()
        uoSangerData = WebFileDownload()
        uoONSData = WebFileDownload()
        sCOG_LINEAGES = "COG_LINEAGES.csv"
        sTableName = ''
        sLINEAGES_BY_LTLA_AND_WEEK = 'lineages_by_ltla_and_week.tsv'
        sCOVID19_APP_DATA = 'covid19_app_data_by_local_authority.csv'
        uo_DBInsertFiles = DbAccess('covid_data')

        uoWFD.get_files_from_url("https://www.england.nhs.uk","/statistics/statistical-work-areas/covid-19-hospital-activity/",".xlsx","")

        uoONSData.get_files_from_url("https://www.ons.gov.uk","/peoplepopulationandcommunity/healthandsocialcare/conditionsanddiseases/datasets/covid19infectionsurveytechnicaldata",".xlsx","")


        uoWFD.get_files_from_url("https://digital.nhs.uk","/data-and-information/publications/statistical/mi-potential-covid-19-symptoms-reported-through-nhs-pathways-and-111-online/latest",".xlsx","")
        #Take retrieved files and load to DB

        uodf_NHSPathways = pandas.DataFrame

        for fName in uoWFD.filenames:
            uodf_NHSPathways = pandas.read_excel(fName)
            if '111 Online Covid-19' in fName:
                sTableName = 'uodf_111_Online_Covid19'
            elif 'NHS Pathways Covid-19 data' in fName:
                sTableName = 'uodf_NHS_Pathways_Covid19_data'
            else:
                sTableName = ''

            if len(sTableName) > 0:
                uodf_NHSPathways_dtype = UO_HELPER.create_dtype(uodf_NHSPathways.head())
                uo_DBInsertFiles.load_data_frame_to_table(uodf_NHSPathways, sTableName,
                                                          uodf_NHSPathways_dtype)
                print(fName)


        #uoWFDAppleMobility.get_files_from_url("https://covid19.apple.com","/mobility",".csv")
        uoWFDGoogleMobility.get_file_from_url("https://www.gstatic.com","/covid19/mobility/",".zip","Region_Mobility_Report_CSVs.zip")

        unzip_files('Region_Mobility_Report_CSVs.zip', 'GB_Region_Mobility_Report')

        uoSangerData.get_file_from_url("https://covid-surveillance-data.cog.sanger.ac.uk","/download/",".tsv",sLINEAGES_BY_LTLA_AND_WEEK)
        uoWFD.get_file_from_url("https://stats.app.covid19.nhs.uk","/data/",".csv",sCOVID19_APP_DATA)
        uodf_Covid19AppData = UO_HELPER.read_file_to_df(sCOVID19_APP_DATA,',' )
        uodf_Covid19AppData.insert(0, 'AsAtDate', date.today())
        uodf_Covid19AppData_dtype = UO_HELPER.create_dtype(uodf_Covid19AppData.head())
        #uo_DBInsertFiles.load_data_frame_to_table(uodf_Covid19AppData, 'uodf_Covid19AppData',
        #                                          uodf_Covid19AppData_dtype)

        #uoSangerData.get_url_selenium("https://sars2.cvr.gla.ac.uk","/cog-uk/", "downloadTable3", ".csv",sCOG_LINEAGES)  ##lineages_by_ltla_and_week.tsv

        #uodf_cog_Lineages = UO_HELPER.read_file_to_df(sCOG_LINEAGES,',' )
        #Add a column for today's date
        #uodf_cog_Lineages.insert(0, 'AsAtDate', date.today())

        #uodf_cog_Lineages_dtype = UO_HELPER.create_dtype(uodf_cog_Lineages.head())
        #uo_DBInsertFiles.load_data_frame_to_table(uodf_cog_Lineages, 'uodf_cog_Lineages',uodf_cog_Lineages_dtype)

        uodf_Lineages_by_ltla = UO_HELPER.read_file_to_df(sLINEAGES_BY_LTLA_AND_WEEK,'\t' )
        #Add a column for today's date
        uodf_Lineages_by_ltla.insert(0, 'AsAtDate', date.today())

        uodf_Lineages_by_ltla_dtype = UO_HELPER.create_dtype(uodf_Lineages_by_ltla.head())
        uo_DBInsertFiles.load_data_frame_to_table(uodf_Lineages_by_ltla, 'uodf_Lineages_by_ltla',uodf_Lineages_by_ltla_dtype)

    except Exception:

        LOGGER.error("An Exception occurred Covid Data Ingest - get_File_data ")
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("!")


############################################################
# Get Google Trend Data
############################################################
def unzip_files(s_zipfile, s_filename_pattern):
    """unzip files

    """
    try:
        sExtractedFiles = []
        # Create a ZipFile Object and load sample.zip in it
        with ZipFile(s_zipfile, 'r') as zipObj:
            # Get a list of all archived file names from the zip
            listOfFileNames = zipObj.namelist()
            # Iterate over the file names
            for fileName in listOfFileNames:
                # Check filename endswith csv
                if fileName.endswith('.csv') and s_filename_pattern in fileName:
                    # Extract a single file from zip
                    zipObj.extract(fileName, 'google_mob_data')
                    sExtractedFiles.append(fileName)

    except Exception:

        LOGGER.error("An Exception occurred Covid Data Ingest - unzip_files for file %s, pattern %s",s_zipfile, s_filename_pattern)
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("!")
############################################################
# Get Google Trend Data
############################################################
def get_Google_Trend_data():
    """File Data

    """
    try:

        now = datetime.datetime.now()
        WORD_LIST = ['Coronavirus','Omicron','Covid','"Covid Symptoms"']

        uo_DBInsertGT = DbAccess('covid_data')
        uoTrend = GoogleTrends()
        #uoTrend.GetTrends(kf_list=WORD_LIST, geo='GB')

        uoTrend.get_daily_data(word='Covid',start_year=2020,start_mon=12, stop_year=now.year,stop_mon=now.month,geo='GB')

        #uodf_historicaldf_dtype = UO_HELPER.create_dtype(uoTrend.historicaldf.head())
        #uo_DBInsertGT.load_data_frame_to_table(uoTrend.historicaldf, 'uodf_historicaldf_dtype',uodf_historicaldf_dtype)

        uodf_GoogleTrend_dtype = UO_HELPER.create_dtype(uoTrend.complete.head())
        uo_DBInsertGT.load_data_frame_to_table(uoTrend.complete, 'uodf_GoogleTrend',uodf_GoogleTrend_dtype)

    except Exception:

        LOGGER.error("An Exception occurred Covid Data Ingest - get_Google_Trend_data ")
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("!")

############################################################
# Process the list of lookups and get the list of closest n
# firestations for each
############################################################
def process_lkp_list(df_lkp, firestations_df, top_n, json_file, b_travel):
    """Process the list of lookup addresses to get nearest n

    Keyword arguments:
    lkp_list -- base point (lat_value, lon_value)
    firestations_df -- data frame of firestation data
    """
    r_list = defaultdict(list)
    lkp_list = []

    for index, row in df_lkp.iterrows():

        nearest_list = []

        lat_value = row['latitude']
        lon_value = row['longitude']

        LOGGER.debug("process_lkp_list :lat_value: %s", str(lat_value))
        LOGGER.debug("process_lkp_list :lon_value: %s", str(lon_value))

        lkp_list = df_lkp.values[index].tolist()

        base_point = (lat_value, lon_value)  # (lat, lon)

        """Return sorted list of nearest by distance"""
        df_fs_dist_list = UO_GET_NEAREST.create_nearest_list(base_point,
                                              firestations_df=firestations_df)

        lkp_list.append(df_fs_dist_list)

        r_list["lkpaddress"].append(lkp_list)

    """save as json"""
    json.dump(r_list, open(json_file, "w"))

############################################################
# Run
############################################################

if __name__ == "__main__":

    try:

        dirname = os.path.dirname(__file__)
        filename_ini = os.path.join(dirname, 'covid_data.ini')
        UO_HELPER = HelperFunctions()

        configImport = UO_HELPER.load_config(filename_ini)

        LOG_PATH = configImport["logging.log_path"]
        LOG_FILE = configImport["logging.log_file"]
        THE_LOG = LOG_PATH + "\\" + LOG_FILE
        LOGGING_LEVEL = configImport["logging.logginglevel"]

        LEVELS = {'debug': logging.DEBUG,
                  'info': logging.INFO,
                  'warning': logging.WARNING,
                  'error': logging.ERROR,
                  'critical': logging.CRITICAL}

        # create LOGGER
        LOGGER = logging.getLogger('coviddata')
        LEVEL = LEVELS.get(LOGGING_LEVEL, logging.NOTSET)
        logging.basicConfig(level=LEVEL)

        HANDLER = logging.handlers.RotatingFileHandler(THE_LOG, maxBytes=1036288, backupCount=5)
        # create FORMATTER
        FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        HANDLER.setFormatter(FORMATTER)
        LOGGER.addHandler(HANDLER)
        # UO_DISTANCE_CALC = DistanceCalc(DISTANCE_MATRIX_API_KEY)
        # UO_API_LOOKUP = APILookup(POSTCODE_API_URL)
        # UO_GET_NEAREST = GetNearest(top_n=NUMBER_CLOSEST, bln_travel_times=B_TRAVEL, api_key=DISTANCE_MATRIX_API_KEY)

        main()

    except DataIngestError as exrec:
        LOGGER.error("Error in ingest - please check:" + str(exrec.data))
        raise Exception("Covid Data ingest failed - please check")

    except Exception:
        LOGGER.error("An Exception in : MAIN :" + __name__)
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("Covid Data ingest failed - please check")
