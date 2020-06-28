"""HELPER Functions"""
import logging
import urllib.parse as urlparse
import datetime
import codecs
import configparser


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
    def load_config(self, filename):
        """
        Load config file
        :param file: config file
        :retu
        """
        try:
            config = {}

            config = config.copy()
            cp = configparser.ConfigParser()
            cp.read(filename)
            for sec in cp.sections():
                name = sec.lower()  # string.lower(sec)
                for opt in cp.options(sec):
                    config[name + "." + opt.lower()] = cp.get(sec, opt).strip()
            return config

        except Exception as error:
            self.LOGGER.exception("Error in load_config : %s", repr(error))
            self.LOGGER.exception(("Error in load_config - filename - %s  ", filename))
            raise Exception("Error in load_config - filename - %s  ", filename)


    ############################################################
    # Write file from list
    ############################################################
    def write_file_from_list(self, contents_list, filename):
        """
        Write file from list
        :param contents_list: The list to write the file from.
        :param filename: The filename to write to.
        :retu
        """
        try:
            with open(filename, 'w') as f:
                for line in contents_list:
                    f.write("%s\n" % line)

        except Exception as error:
            self.LOGGER.exception("Error in read_and_replace : %s", repr(error))
            self.LOGGER.exception(("Error in read_and_replace - filename - %s  ", filename))
            raise Exception("Error in read_and_replace - filename - %s  ", filename)

    ############################################################
    # read the passed template and replace placeholders
    # with values in dictionary
    ############################################################
    def read_and_replace(self, contents_dic, filename):
        """
        Write file from list
        :param contents_dic: Dictionary items to replace placeholders.
        :param filename: The filename to read to get template.
        :return file_contents: Return replaced file contents
        """

        try:

            with open(filename, 'r') as f:
                file_contents = f.read()


            for key in contents_dic.keys():
                replaced_contents = file_contents.replace(key, contents_dic[key])
                file_contents = replaced_contents

            return file_contents

        except Exception as error:
            self.LOGGER.exception("Error in read_and_replace : %s", repr(error))
            self.LOGGER.exception(("Error in read_and_replace - filename - %s  ", filename))
            raise Exception("Error in read_and_replace - filename - %s  ", filename)


    ############################################################
    # write file with passed contents
    ############################################################
    def write_file(self, contents, filename):
        """
        Write file from list
        :param contents: contents to write to file
        :param filename: The filename to write to.
        """

        try:

            with open(filename, 'w') as f:
                f.writelines(contents)

        except Exception as error:
            self.LOGGER.exception("Error in write_file : %s", repr(error))
            self.LOGGER.exception(("Error in write_file - filename - %s  ", filename))
            raise Exception("Error in write_file - filename - %s ", filename)


    ############################################################
    # read the passed template and replace placeholders
    # with values in dictionary
    ############################################################
    def read_to_list(self, filename):
        """
        Write file from list
        :param filename: The filename to read.
        :return file_contents_list: Return file contents
        """

        try:

            with open(filename, 'r') as f:
                file_contents_list = f.readlines()

            return file_contents_list

        except Exception as error:
            self.LOGGER.exception("Error in read_to_list : %s", repr(error))
            self.LOGGER.exception(("Error in read_to_list - filename - %s  ", filename))
            raise Exception("Error in read_to_list - filename - %s  ", filename)
