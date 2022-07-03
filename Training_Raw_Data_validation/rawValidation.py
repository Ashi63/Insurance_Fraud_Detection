import json
import sys
import os
import shutil
import re
import pandas as pd
from datetime import datetime
sys.path.append("..")

from application_logging.logger import App_Logger

logger = App_Logger()



def valuesfromSchema():
    path = "D:/Fraud Detection/"
    
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

    return LengthOfDateStampInFile,LengthOfTimeStampInFile,NumberofColumns,ColName


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


def deleteExistingGoodDataTrainingFolder():

    path = "Training_Raw_files_validated/"

    path_logs_dir = os.path.join("D:/Fraud Detection/","Training_Logs/")

    try:
        if os.path.isdir(path + "Good_Raw/"):
            shutil.rmtree(path+"Good_Raw/")
            file = open(path_logs_dir+"GeneralLogs.txt","a+")
            logger.log(file, "Good_Raw Folder Deleted")
            file.close()
    except OSError as s:
            file = open(path_logs_dir+"GeneralLogs.txt","a+")
            logger.log(file, f"Error Deleting Good Data Training Folder : {s}")
            file.close()
            raise OSError()


def deleteExistingBadDataTrainingFolder():

    path = "Training_Raw_files_validated/"

    path_logs_dir = os.path.join("D:/Fraud Detection/","Training_Logs/")

    try:
        if os.path.isdir(path + "Bad_Raw/"):
            shutil.rmtree(path+"Bad_Raw/")
            file = open(path_logs_dir+"GeneralLogs.txt","a+")
            logger.log(file, "Bad Raw Folder Deleted")
            file.close()
    except OSError as s:
            file = open(path_logs_dir+"GeneralLogs.txt","a+")
            logger.log(file, f"Error Deleting Bad Data Training Folder : {s}")
            file.close()
            raise OSError()


def moveBadFilesToArchiveBad():
    try:
        now = datetime.now()
        date = now.date()
        time= now.strftime("%H%M%S")
        path = "D:/Fraud Detection/"

        src_path = os.path.join(path + "Training_Raw_Data_validation/Training_Raw_files_validated/Bad_Raw/")

        if os.path.isdir(src_path):
            archive_path =path + "TrainingArchiveBadData/"
            if not os.path.isdir(archive_path):
                os.makedirs(archive_path)
            dst_path = archive_path + "BadData" + str(date) +"_"+ str(time)
            if not os.path.isdir(dst_path):
                os.makedirs(dst_path)

            src_files = os.listdir(src_path)
            dst_files = os.listdir(dst_path)
            
            for f in src_files:
                if f not in dst_files:
                    shutil.move(src_files + f, dst_files)
            file = open (path + "Training_Logs/GeneralLogs.txt","a+")
            logger.log(file, "Bad Files Moved to Archive Folder")
            path = path + "Training_Raw_Data_validation/Training_Raw_files_validated/"
            if os.path.isdir(path + "Bad_Raw/"):
                shutil.rmtree(path + "Bad_Raw/")
            logger.log(file, "Bad Raw Data Folder Deleted Successfully")
            file.close()
    except Exception as e:
            file = open(path + "Training_Logs/GeneralLog.txt", 'a+')
            logger.log(file, f"Error while moving bad files to archive:: {e}")
            file.close()
            raise e


def validatioFIleNameRaw():
    LengthOfDateStampInFile,LengthOfTimeStampInFile,NumberofColumns,ColName=valuesfromSchema()
    deleteExistingBadDataTrainingFolder()
    deleteExistingGoodDataTrainingFolder()
    regex = manualRegexCreation()
    path = "D:/Fraud Detection/"
    Training_Logs_nameValidationLog = "Training_Logs/nameValidationLog.txt"
    batch_files_raw = [f for f in os.listdir(path+"Shared_Batch_Files")]


    f = open(path + Training_Logs_nameValidationLog,"a+")

    for raw_file_name in batch_files_raw:
        if (re.match(regex, raw_file_name)):
            splitAtDot = raw_file_name.split(".csv")
            splitAtUnderscore = splitAtDot[0].split("_")
            if splitAtUnderscore[1] == LengthOfDateStampInFile:
                if splitAtUnderscore[2] == LengthOfTimeStampInFile:
                    shutil.copy(path+"Shared_Batch_Files/" + raw_file_name, "Training_Raw_files_validated/Good_Raw")
                    logger.log(f,f"File is valid..!!{raw_file_name}")
                else:
                    shutil.copy(path+"Shared_Batch_Files/" + raw_file_name, "Training_Raw_files_validated/Bad_Raw")
                    logger.log(f,f"File time length is not valid..!!{raw_file_name}")
                    
            else:
                shutil.copy(path+"Shared_Batch_Files/" + raw_file_name, "Training_Raw_files_validated/Bad_Raw")
                logger.log(f,f"File date length is not valid..!!{raw_file_name}")
        else:
            shutil.copy(path+"Shared_Batch_Files/" + raw_file_name, "Training_Raw_files_validated/Bad_Raw")
            logger.log(f,f"File name is not valid..!!{raw_file_name}")

    f.close()

def validateColumnLength():

    path ="D:/Fraud Detection/"
    Training_Logs_columnValidation = "Training_Logs/columnValidation.txt"
    good_raw_files_path = os.path.join(path + "Training_Raw_Data_validation/Training_Raw_files_validated/Good_Raw/")
    bad_raw_files_path = os.path.join(path + "Training_Raw_Data_validation/Training_Raw_files_validated/Bad_Raw/")
    LengthOfDateStampInFile,LengthOfTimeStampInFile,NumberofColumns,ColName = valuesfromSchema()
    good_raw_files = os.listdir(good_raw_files_path)

    try:    
        f = open(path + Training_Logs_columnValidation, "a+")
        logger.log(f, "Column Length Validation Started!!")
        for file in good_raw_files:
            csv = pd.read_csv(good_raw_files_path + file)
            if csv.shape[1] == NumberofColumns:
                pass
            else:
                shutil.move(good_raw_files_path + file,bad_raw_files_path )
                logger.log(f, f"Invalid Column length for the file.!! File moved to Bad Data Folder:: {file}")
        logger.log(f, "Column Length Validation Completed..!!")

    except Exception as OSError:
        f = open(path + Training_Logs_columnValidation, "a+")
        logger.log(f, f"Error Occured while moving the file :: {OSError}")
        f.close()
        raise OSError
    except Exception as e:
        f = open(path +Training_Logs_columnValidation, "a+")
        logger.log(f, f"Error Occured :: {e}")
        f.close()
        raise e
    f.close()

def validateMissingValuesInWholeColumn():
    try:
        path ="D:/Fraud Detection/"
        Training_Logs_columnValidation = "Training_Logs/columnValidation.txt"
        good_raw_files_path = os.path.join(path + "Training_Raw_Data_validation/Training_Raw_files_validated/Good_Raw/")
        bad_raw_files_path = os.path.join(path + "Training_Raw_Data_validation/Training_Raw_files_validated/Bad_Raw/")
        good_raw_files = os.listdir(good_raw_files_path)
        Training_Logs_missingValuesInColumn = "Training_Logs/missingValuesInColumn.txt"

        f = open(path+Training_Logs_missingValuesInColumn,"a+")
        logger.log(f,"Missing Values Validation Started..!!")
        for file in good_raw_files:
            csv = pd.read_csv(good_raw_files_path + file)
            count = 0
            for columns in csv:
                if (len(csv[columns])-csv[columns].count())== len(csv[columns]):
                    count +=1
                    shutil.move(good_raw_files_path + file, bad_raw_files_path)
                    logger.log(f, f"Invalid File Columns COntains Missing Values..!! File Moved to Bad Raw Folder {file}")
                    break
            if count == 0:
                csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)

    except OSError:
        f = open(path+Training_Logs_missingValuesInColumn, 'a+')
        self.logger.log(f, "Error Occured while moving the file :: {OSError}")
        f.close()
        raise OSError
    except Exception as e:
        f = open(path+Training_Logs_missingValuesInColumn, 'a+')
        self.logger.log(f, "Error Occured while moving the file :: {e}")
        f.close()
        raise e
    f.close()