"""HELPER Functions"""
import logging
import configparser


class HelperFunctions:
    """
    Various helper functions
    """
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
        :return config: return config
        """
        try:
            config = {}

            config = config.copy()
            conf_parse = configparser.ConfigParser()
            conf_parse.read(filename)
            for sec in conf_parse.sections():
                name = sec.lower()  # string.lower(sec)
                for opt in conf_parse.options(sec):
                    config[name + "." + opt.lower()] = conf_parse.get(sec, opt).strip()
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
        """
        try:
            with open(filename, 'w') as fl_nm:
                for line in contents_list:
                    fl_nm.write("%s\n" % line)

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

            with open(filename, 'r') as fl_nm:
                file_contents = fl_nm.read()


            for key in contents_dic.keys():
                replaced_contents = file_contents.replace(key, contents_dic[key])
                file_contents = replaced_contents

            return file_contents

        except Exception as error:

            self.LOGGER.exception("Error in read_and_replace : %s", repr(error))
            self.LOGGER.exception(("Error in read_and_replace - filename - %s  ", filename))
            raise Exception("Error in read_and_replace - filename - %s  ", filename)


    ############################################################
    # read file and pass contents back
    ############################################################
    def read_file_contents(self, filename):
        """
        Write file from list
        :param filename: The filename to read to get template.
        :return file_contents: Return replaced file contents
        """

        try:

            with open(filename, 'r') as fl_nm:
                file_contents = fl_nm.read()

            return file_contents

        except Exception as error:

            self.LOGGER.exception("Error in read_file_contents : %s", repr(error))
            self.LOGGER.exception(("Error in read_file_contents - filename - %s  ", filename))
            raise Exception("Error in read_file_contents - filename - %s  ", filename)


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

            with open(filename, 'w') as fl_nm:
                fl_nm.writelines(contents)

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

            with open(filename, 'r') as fl_nm:
                file_contents_list = fl_nm.readlines()

            return file_contents_list

        except Exception as error:
            self.LOGGER.exception("Error in read_to_list : %s", repr(error))
            self.LOGGER.exception(("Error in read_to_list - filename - %s  ", filename))
            raise Exception("Error in read_to_list - filename - %s  ", filename)
