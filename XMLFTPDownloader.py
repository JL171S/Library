import json
import datetime
from multiprocessing.connection import wait

logFile = open("Ingest_Script_Log_"+datetime.date.today().strftime("%Y-%m-%d"), 'w') # open log file to write to throughout process.

listingsSchedulesName = 'listings_schedules_s'  # Name of Listing Schedules file
listingsProgramsName = 'listings_programs_p'    # Name of Listing programs file
ChannelssName = 'channels_c'                    # Name of Channels file

def logInfo(message):   # function for logging info related to the process of the application
    logFile.write(str(message))  # write to log file
    print(str(message))  # print to console

retriedCount = 0

def retry():
    import time
    retriedCount += 1
    time.sleep(60) # sleeps for 60 seconds
    if (retriedCount != 10):
        main()
    else:
        logInfo("Failed 10 times in a row, probably something needs to be checked on.")

def EPGFTPDownload(IPAddress: str, filePath: str, username: str, password: str, fileName: str): # FTP server to get fileName from filePath
    import ftplib     

    try:
        logInfo("Starting FTP connection at: " + IPAddress)
        EPGFTP = ftplib.FTP(IPAddress) # open FTP connection
        EPGFTP.login(username, password) # Login
        EPGFTP.cwd(filePath) # move to file path
    except Exception as error: 
        logInfo("\nConnection failed: " + str(error) + ", Retring in 1 minute")
        retry()
        return
    try:
        logInfo("\nConnection successful, attempting to download file " + fileName + " (This can take several minutes (5 usually)")
        EPGFTP.retrbinary("RETR " + fileName ,open(fileName, 'wb').write) # download file form FTP server and write it to file named fileName
        logInfo("\nDownload successful")
        EPGFTP.quit()
    except Exception as error: 
        logInfo("\nDownload failed: " + str(error))
        return


def EPGXMLToJSON(): # Read EPG and convert to JSON
    import xml.etree.ElementTree as ElementTree
    import csv
    
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

    try:    # extract XML data from root and pass to CSV maker.
        logInfo("\nStarting EPG transformation to CSV")
        epgXMLroot = ElementTree.parse('epgpub.xml').getroot()  # create element tree object and set root.

        # --Channels data--
        CSVHeaderKeys = ['id', 'c', 'l', 'd', 't', 'a', 'u', 'b', 'iso3166', 'iso639', 'tz'] # Channels header keys
        ElementTreeToCSV(epgXMLroot[2],CSVHeaderKeys,ChannelssName + '.csv') # create csv from xml element tree

        # --Schedules data--
        CSVHeaderKeys = ['s', 'd', 'p', 'c'] # Schedules header keys
        ElementTreeToCSV(epgXMLroot[0][0],CSVHeaderKeys,listingsSchedulesName + '.csv') # create csv from xml element tree

        # --Programs data--
        CSVHeaderKeys = ['id', 't', 'rt', 'et', 'd', 'rd', 'l'] # Programs header keys
        ElementTreeToCSV(epgXMLroot[0][1],CSVHeaderKeys,listingsProgramsName + '.csv') # create csv from xml element tree
        logInfo("\nEPG transformation to CSV successful")
    except Exception as error: 
        logInfo("\nCSV writing failed: " + str(error))
        return

    try:
        logInfo("\nStarting CSV transformation to JSON")
        CSVToJSON(ChannelssName + ".csv", ChannelssName + ".json")
        CSVToJSON(listingsSchedulesName + ".csv", listingsSchedulesName + ".json")
        CSVToJSON(listingsProgramsName + ".csv", listingsProgramsName + ".json")
        logInfo("\nCSV transformation to JSON successful")
    except Exception as error: 
        logInfo("\nJSON writing failed: " + str(error))
        return
 

def JSONUploadToServiceNow(): # Read JSON files, and upload them to SNOW
    
    def ServiceNowUpload(JSONData, url : str): # Upload data to ServiceNow URL
        import requests

        # Login credentials for ServiceNow ingestion agent
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
            logInfo('\nStatus:', response.status_code, '\nHeaders:', response.headers, '\nError Response:',response.json())
            exit()

        logInfo("\m" + str(response.text))    # Print responce from post operation
    
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
       
        JSONFile.close()    # Closing file
    
    try:
        loadJSONandUpload(ChannelssName + '.json', 'https://attegdev.service-now.com/api/now/import/stagingTable/insertMultiple')               # Loading stagingTable data
        loadJSONandUpload(listingsProgramsName + '.json', 'https://attegdev.service-now.com/api/now/import/stagingTable/insertMultiple')        # Loading stagingTable data
        loadJSONandUpload(listingsSchedulesName + '.json', 'https://attegdev.service-now.com/api/now/import/stagingTable/insertMultiple')       # Loading stagingTable data
    except Exception as error: 
        logInfo("\nServiceNow Upload Error: " + str(error))
        return


def main():
    IPAddress = ''                  # IP Address
    filePath = ''                   # File path
    username = ""                   # Username for FTP
    password = ""                   # Password for FTP 
    fileName = ""                   # File name within file path

    EPGFTPDownload(IPAddress,filePath,username,password,fileName) # Download EPG file from FTP server
    EPGXMLToJSON() # convert downloaded EPG XML to JSON
    JSONUploadToServiceNow() # upload created JSON's to ServiceNow
    logFile.close()
    

if __name__ == "__main__":
    main()