import sys
import json
import datetime
import os
import time
import ftplib
import xml.etree.ElementTree as ElementTree
import csv
import requests

# global varaibles
logFileName = "EPG Run Logs/EPG_Ingest_Script_Log_"+datetime.date.today().strftime("%Y-%m-%d")
fileNameXML = ""         # hold xml file name
instanceName = "directv" # default to PROD
startJSONAt = -1         # default to all
whichSection = 0         # default to all
attempts = 0             # hasn't started
uploadSuccessful = False # default to fail
uploadMessage = ""       # default to blank
listingsSchedulesName = 'listings_schedules_s'  # Name of Listing Schedules file
listingsProgramsName = 'listings_programs_p'    # Name of Listing programs file
ChannelsName = 'channels_c'                     # Name of Channels file


if not os.path.exists("EPG Run Logs"): # if directory doesn't exist
    os.makedirs("EPG Run Logs") # create directory
logFile = open(logFileName, 'w') # open log file to write to throughout process.

def logInfo(message):   # function for logging info related to the process of the application
    logFile.write(str(message))  # write to log file
    print(str(message))  # print to console



def retry():
    global attempts
    global startJSONAt
    attempts += 1
    time.sleep(1) # sleeps for 60 seconds
    if (attempts < 10):
        if (whichSection == 0):
            main()  
        if (whichSection == 1):
            EPGXMLToJSON()
        if (whichSection == 2):
            JSONUploadToServiceNow(instanceName, startJSONAt)
    else:
        logInfo("\nFailed 10 times in a row, probably something needs to be checked on.")
        try:
            logFile.close()
            sendUploadStatus() # send error message
        except Exception as e:
            print(e)

def EPGFTPDownload(addressEPG: str, filePathEPG: str, usernameEPG: str, passwordEPG: str, fileNameEPG: str): # FTP server to get EPG XML file
    global uploadMessage  
    try:
        logInfo("Starting FTP connection at: " + addressEPG)
        EPGFTP = ftplib.FTP(addressEPG) # open FTP connection
        EPGFTP.login(usernameEPG, passwordEPG) # Login
        EPGFTP.cwd(filePathEPG) # move to file path
    except Exception as error: 
        logInfo("\nConnection failed: " + str(error) + ", Retrying in 1 minute")
        uploadMessage = "FTP connection failed: " + str(error)
        retry()
        return


    try:
        logInfo("\nConnection successful, attempting to download file " + fileNameEPG + " (This can take several minutes (5 usually)")
        EPGFTP.retrbinary("RETR " + fileNameEPG ,open(fileNameEPG, 'wb').write) # download file form FTP server and write it to file named fileNameEPG
        logInfo("\nDownload successful")
        EPGFTP.quit()
    except Exception as error: 
        logInfo("\nFile download failed: " + str(error))
        uploadMessage = "File download failed: " + str(error)
        retry()
        return

    EPGXMLToJSON() # convert downloaded EPG XML to JSON


def EPGXMLToJSON(): # Read EPG and convert to JSON
    global whichSection
    whichSection = 1

    def ElementTreeToCSV(elementTree: list, CSVHeaderKeys: list, CSVfileName: str): # first we convert to CSV... because its easier and already done.

        EPGTree = []
        for child in elementTree:             
            EPGTree.append(child.attrib)   # iterate through all nodes and append them to the list

        logInfo("\nWriting " + CSVfileName)
        with open(CSVfileName, 'w') as outputFile:
            CSVDictWriter = csv.DictWriter(outputFile, CSVHeaderKeys) # use keys for header line
            CSVDictWriter.writeheader()
            CSVDictWriter.writerows(EPGTree)


   
    def CSVToJSON(CSVFilePath: str, JSONFilePath: str):  # We convert from CSV to JSON because its fast and ServiceNow requires JSON
        JSONArray = []
        global uploadMessage

        with open(CSVFilePath, encoding='utf-8') as CSVf:   # read CSV file
            CSVReader = csv.DictReader(CSVf)                # load csv file data using csv library's dictionary reader

            if (CSVFilePath == (str(listingsSchedulesName) + '.csv')):
                stopDate = datetime.date.today() + datetime.timedelta(days=5)   # get 5 days from today 
                formattedDate = stopDate.strftime("%Y-%m-%d")                   # format date in expected format
                logInfo("\nCropping " + listingsSchedulesName + " date range to: " + formattedDate)
                
                for row in CSVReader: # convert each csv row into python dict
                    if str(row['s']).startswith(formattedDate): # stop adding if reached stop date
                        break
                    JSONArray.append(row)
            else:
                for row in CSVReader:   # convert each csv row into python dict
                    JSONArray.append(row)
        
        logInfo("\nWriting " + CSVFilePath)
        with open(JSONFilePath, 'w', encoding='utf-8') as JSONf:    # Convert python jsonArray to JSON String and write to file
            JSONString = json.dumps(JSONArray, indent=4)
            JSONf.write(JSONString)

    try:
        logInfo("\nStarting EPG transformation to CSV")
        global fileNameXML
        epgXMLroot = ElementTree.parse(fileNameXML).getroot()  # create element tree object and set root.

        # --Channels data--
        CSVHeaderKeys = ['id', 'c', 'l', 'd', 't', 'a', 'u', 'b', 'iso3166', 'iso639', 'tz'] # Channels header keys
        ElementTreeToCSV(epgXMLroot[2],CSVHeaderKeys,ChannelsName + '.csv') # create csv from xml element tree

        # --Schedules data--
        CSVHeaderKeys = ['s', 'd', 'p', 'c'] # Schedules header keys
        ElementTreeToCSV(epgXMLroot[0][0],CSVHeaderKeys,listingsSchedulesName + '.csv') # create csv from xml element tree

        # --Programs data--
        CSVHeaderKeys = ['id', 't', 'rt', 'et', 'd', 'rd', 'l'] # Programs header keys
        ElementTreeToCSV(epgXMLroot[0][1],CSVHeaderKeys,listingsProgramsName + '.csv') # create csv from xml element tree
        logInfo("\nEPG transformation to CSV successful")
    except Exception as error: 
        logInfo("\nCSV writing failed: " + str(error))
        uploadMessage = "CSV writing failed: " + str(error)
        retry()
        return

    try:
        logInfo("\nStarting CSV transformation to JSON")
        CSVToJSON(ChannelsName + ".csv", ChannelsName + ".json")
        CSVToJSON(listingsSchedulesName + ".csv", listingsSchedulesName + ".json")
        CSVToJSON(listingsProgramsName + ".csv", listingsProgramsName + ".json")
        logInfo("\nCSV transformation to JSON successful")
    except Exception as error: 
        logInfo("\nJSON writing failed: " + str(error))
        uploadMessage = "JSON writing failed: " + str(error)
        retry()
        return

    global instanceName
    JSONUploadToServiceNow(instanceName, 0) # upload created JSON's to ServiceNow
 

def JSONUploadToServiceNow(instanceName: str, startAt : int,): # Read JSON files, and upload them to SNOW
    global whichSection
    whichSection = 2

    def ServiceNowUpload(JSONData, url : str): # Upload data to ServiceNow URL
        # Login credentials for ServiceNow EPG ingestion agent
        user = ''
        pwd = ''

        headers = {"Content-Type":"application/json","Accept":"application/json"}   # Set proper headers

        JSONRecordsBrace = '{ "records": [ '    # used for sending 'records' as part of the json so that you can insert multiple records at once.
        JSONRecordsBraceEnd = ' ] }'            # used to close the appended 'records' braces

        # JSONData = '[ {} ]'   # Debugging data

        #Build the data string
        JSONString = JSONRecordsBrace + JSONData + JSONRecordsBraceEnd # eclose the data inside the records brace

        # print(JSONString) # Debug print string
        
        response = requests.post(url, auth=(user, pwd), headers=headers ,data=JSONString)   # Post data to SNOW

        if response.status_code != 200 and response.status_code != 201:     # Check for HTTP error codes other than 200 or 201
            logInfo('\nStatus:' + str(response.status_code) + '\nHeaders:' + str(response.headers) + '\nError Response:' + str(response.json()))
            exit()

        logInfo("\n" + str(response.text))    # Print responce from post operation
    
    def loadJSONandUpload(JSONFileName : str, url : str): # Read JSON files and sent them to ServiceNow
        
        JSONFile = open (JSONFileName, "r")         # Opening JSON file
        JSONData = json.loads(JSONFile.read())      # Reading JSON from file
        lengthOfJSON = len(JSONData)                # Setting length variable        
        previousKey = 0                             # Starting position of 0
        sendBlockSize = 10000                       # ServiceNow REST API has a time limit for uploading data, found that 10000 is optimal for uploading.
        
        logInfo("\nSending " + JSONFileName + ", operation generally takes: " + str((((lengthOfJSON - previousKey) / sendBlockSize))) + ' minutes' )    # Debug printing

        while(previousKey < lengthOfJSON):
            if (previousKey + sendBlockSize < lengthOfJSON):    # size of remaining data greather than sendBlockSize, so continue blocking up data to send.
                
                dataToSend = ''
                sendLength = previousKey + sendBlockSize            # Length is previously ended location + another block

                # for loop
                i = previousKey                                     # Start location (i) is previously ended location
                while i < sendLength:                               # Iterating using while loop, itterate till reached sendLength
                    dataToSend += json.dumps(JSONData[i]) + ', '    # Append data to send
                    i += 1                                          # Incriment i
                # end for loop
                logInfo("\nremaining parts: " + str((lengthOfJSON - previousKey) / sendBlockSize))
                ServiceNowUpload(dataToSend[:-2], url)              # Sending to function             
                previousKey += sendBlockSize                        # Setting previousKey to last location.
            else: # No more than sendBlockSize entries, just send it.
                
                dataToSend = ''
                
                # for loop
                i = previousKey                                     # Start location (i) is previously ended location
                while i < lengthOfJSON:                             # Iterating using while loop, itterate till reached end of JSON data
                    dataToSend += json.dumps(JSONData[i]) + ', '    # Append data to send
                    i += 1                                          # Incriment i
                # end for loop
                
                logInfo("\nSending final part")
                ServiceNowUpload(dataToSend[:-2], url)              # Sending to function             
                previousKey = lengthOfJSON                          # setting previousKey to length of JSON
        
        logInfo("\nAll parts sent")
        JSONFile.close()    # Closing file
    
    previouslySentJSON = 0
    try:
        global ChannelsName
        global listingsProgramsName
        global listingsSchedulesName
        if (int(startAt) <= 1):
            previouslySentJSON = 1
            loadJSONandUpload(listingsSchedulesName + '.json', 'https://' + instanceName + '.service-now.com/api/now/import/u_listing_s_test2_stage/insertMultiple')        # Loading Schedule data
        if (int(startAt) <= 2):
            previouslySentJSON = 2
            loadJSONandUpload(listingsProgramsName + '.json', 'https://' + instanceName + '.service-now.com/api/now/import/u_listings_programs_p_stage/insertMultiple')    # Loading Program data
        if (int(startAt) <= 3):
            previouslySentJSON = 3
            loadJSONandUpload(ChannelsName + '.json', 'https://' + instanceName + '.service-now.com/api/now/import/u_channels_c_stage/insertMultiple')                     # Loading Channel data
            
    except Exception as error: 
        logInfo("\nServiceNow Upload Error: " + str(error))
        global uploadMessage
        uploadMessage = "ServiceNow Upload Error: " + str(error)
        global startJSONAt
        startJSONAt = previouslySentJSON
        retry()
        return

    # send upload status
    global uploadSuccessful
    uploadSuccessful = True
    logInfo("\nServiceNow Upload Successful")
    try: 
        sendUploadStatus()
    except Exception as e:
        logInfo("\nServiceNow Upload Status Upload Error: " + str(error))
        logFile.close()


def sendUploadStatus(): # Send upload status to SNOW
    # create CSV of status + message
    global uploadSuccessful
    global uploadMessage

    uploadStatus = "Failure" # default to failure
    if uploadSuccessful:    # if successful
        uploadStatus = "Success"                            # change to success
        uploadMessage = "All parts uploaded successfully"   # send success message

    # Login credentials for ServiceNow EPG ingestion agent
    user = ''
    pwd = ''

    headers = {"Content-Type":"application/json","Accept":"application/json"}   # Set proper headers

    url = 'https://' + instanceName + '.service-now.com/api/now/import/u_u_verse_epg_upload_logs_import/insertMultiple'

    JSONRecordsBrace = '{ "records": [ '    # used for sending 'records' as part of the json so that you can insert multiple records at once.
    JSONRecordsBraceEnd = ' ] }'            # used to close the appended 'records' braces
    JSONData = '{ "Upload Status": "' + uploadStatus + '", "Log": "' + uploadMessage + '" }'  # build string

    #Build the data string
    JSONString = JSONRecordsBrace + JSONData + JSONRecordsBraceEnd # eclose the data inside the records brace

    # print(JSONString) # Debug print string
    
    response = requests.post(url, auth=(user, pwd), headers=headers ,data=JSONString)   # Post data to SNOW

    if response.status_code != 200 and response.status_code != 201:     # Check for HTTP error codes other than 200 or 201
        logInfo('\nStatus:' + str(response.status_code) + '\nHeaders:' + str(response.headers) + '\nError Response:' + str(response.json()))
        exit()

    logInfo("\n" + str(response.text))    # Print responce from post operation
    logInfo("\nUpload status upload completed")
    logFile.close()
    
    


def main():
    global fileNameXML

    addressXML = ''    # IP Address
    filePathXML = ''   # File path
    usernameXML = ""   # Username for FTP
    passwordXML = ""   # Password for FTP 
    fileNameXML = ""   # File name within file path

    global instanceName
    global whichSection
    global startJSONAt

    arguments = sys.argv[1:] # arguments should be instance, which part to run, which JSON to Upload (e.g: directv 0 0)
    if (len(arguments) > 0):
        startJSONAt = -1
        instanceName = "directv" # default to PROD
        whichSection = -1
        instanceName = arguments[0]
        whichSection = int(arguments[1])
        startJSONAt = int(arguments[2])

        print(str(instanceName) + " " + str(whichSection) + " " + str(startJSONAt))
        
        # Example: python3 EPGFTPDownloader.py directv 3 2 will make it run only the JSON Upload to servicenow, starting at listing programs.

        # which seciton: 0 = run all, 1 = run conversion again and resend, 2 = just resend data.
        # start JSON At: 1 = do all, 2 = do listing programs and channels, 3 = do channels only

        if (whichSection == 0):
            EPGFTPDownload(addressXML,filePathXML,usernameXML,passwordXML,fileNameXML)  # Download XML file from FTP server
        if (whichSection == 1):
            EPGXMLToJSON()
        if (whichSection == 2):
            JSONUploadToServiceNow(instanceName,startJSONAt)
            # start JSON At: 1 = do all, 2 = do listing Programs and listing schedules, 3 = do listing scheudles only
        if (whichSection == 3): # debug for testing email
            sendUploadStatus()
        
    else:
        EPGFTPDownload(addressXML,filePathXML,usernameXML,passwordXML,fileNameXML) # Download EPG file from FTP server
    

if __name__ == "__main__":
    main()