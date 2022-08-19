import logging

"""
Log

Creates a logger that can be used for any file
Outputs the files into logs with the name where the log originated

Attributes
--------
current_filename: str
    Naming convention for the logs. Uses the name of the logger

logger: Logger
    Logger used to log the different aspects of the project

formatter: Formatter
    Formatter to handle the output of the logs

console_handler: StreamHandler
    Outputs debug messages to console

info_file_handler: FileHandler
    Outputs info logs to the info log file 

error_file_handler: FileHandler
    Outputs error logs to the error log file 

critical_file_handler: FileHandler
    Outputs critical logs to the critical log file 

Acknowledgement: https://medium.com/nerd-for-tech/logging-with-logging-in-python-d3d8eb9a155a
                https://www.programcreek.com/python/example/192/logging.Formatter
"""


class Log:
    def __init__(self, name):
        self.current_filename = "logger".rsplit('.', 1)[0]
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        self.formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')

        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(logging.DEBUG)
        self.console_handler.setFormatter(self.formatter)

        self.info_file_handler = logging.FileHandler(self.current_filename + '_info.log')
        self.info_file_handler.setLevel(logging.INFO)
        self.info_file_handler.setFormatter(self.formatter)

        self.error_file_handler = logging.FileHandler(self.current_filename + '_error.log')
        self.error_file_handler.setLevel(logging.ERROR)
        self.error_file_handler.setFormatter(self.formatter)

        self.critical_file_handler = logging.FileHandler(self.current_filename + '_critical.log')
        self.critical_file_handler.setLevel(logging.CRITICAL)
        self.critical_file_handler.setFormatter(self.formatter)

        self.logger.addHandler(self.console_handler)
        self.logger.addHandler(self.info_file_handler)
        self.logger.addHandler(self.error_file_handler)
        self.logger.addHandler(self.critical_file_handler)