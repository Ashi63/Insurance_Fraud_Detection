import json
import sys
import os
import shutil
sys.path.append("..")

from application_logging.logger import App_Logger

logger = App_Logger()




def valuesfromSchema():
    
    path = "D:\Fraud Detection\\"
    with open(path + "schema_training.json","r") as f:
        dict = json.load(f)

    pattern = dict['SampleFileName']
    LengthOfDateStampInFile = dict['LengthOfDateStampInFile']
    LengthOfTimeStampInFile = dict['LengthOfTimeStampInFile']
    NumberofColumns = dict['NumberofColumns']
    ColName = dict['ColName']

    path_logs_dir = os.path.join("D:\Fraud Detection\\","Training_Logs")
    path_log_file = os.path.join(path_logs_dir,"valuesfromSchemaValidationLog.txt")

    file = open(path_log_file,'a+')
    message = f"Length Of Date Stamp In File:: {LengthOfDateStampInFile}"+"\t\t"+f"Length Of Time Stamp In File:: {LengthOfTimeStampInFile}"+"\t\t"+ f"NumberofColumns:: {NumberofColumns}"+"\n"
    logger.log(file,message)
    file.close()


def manualRegexCreation():
    regex = "['fraudDetection']+['\_'']+[\d_]+[\d]+\.csv"
    return regex

def createDirectoryForGoodBadRawData():

    path_logs_dir = os.path.join("D:\Fraud Detection\\","Training_Logs")
    path_log_file = os.path.join(path_logs_dir,"GeneralLogs.txt")

    try:
        path = os.path.join("Training_Raw_files_validated/","Good_Raw/")
        if not os.path.isdir(path):
            os.makedirs(path)
            file = open (path_log_file,"a+")
            logger.log(file, "Good Raw Data Folder Created")
            file.close()
        else:
            file = open (path_log_file,"a+")
            logger.log(file, "Good Raw Data Folder Already Exits")
            file.close()
        path = os.path.join("Training_Raw_files_validated/","Bad_Raw")
        if not os.path.isdir(path):
            os.makedirs(path)
            file = open (path_log_file,"a+")
            logger.log(file,"Bad Raw Data Folder Created")
            file.close()
        else:
            file = open (path_log_file,"a+")
            logger.log(file, "Bad Raw Data Folder Already Exits")
            file.close()
    except OSError as ex:
        file = open (path_log_file,"a+")
        logger.log(file,"Error in creating Directory")
        file.close()
        raise OSError







