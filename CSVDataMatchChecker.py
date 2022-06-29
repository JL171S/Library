import csv

def logInfo(message):   # function for logging info related to the process of the application
    print(str(message))  # logInfo to console

def CSVComparison(firstCSVPath : str, firstCSVKey : str, secondCSVPath : str, secondCSVKey : str): # Read two CSV files and compare two keys for matches
    foundMatches = []
    itterationsCount = 0
    with open(firstCSVPath, encoding='utf-8') as CSV1:      # read CSV file
        firstCSVReader = csv.DictReader(CSV1)               # load csv file data using csv library's dictionary reader

        for firstRow in firstCSVReader:                     # itterate through first CSV rows
            # logInfo(firstRow)                             # Debug log
            with open(secondCSVPath, encoding='utf-8') as CSV2:     # read CSV file
                secondCSVReader = csv.DictReader(CSV2)              # load csv file data using csv library's dictionary reader
                for secondRow in secondCSVReader:                   # itterate through second CSV rows
                    # logInfo(secondRow)                            # Debug log
                    if (str(firstRow[firstCSVKey]) == str(secondRow[secondCSVKey])):    # compare first CSV's row with second CSV's row for any direct matches
                        foundMatches.append(str(firstRow[firstCSVKey])) # append matches
                        # logInfo("found match")                          # print
                        break
                    else:
                        if (str(secondRow[secondCSVKey]) == '' or str(firstRow[firstCSVKey]) == ''):
                            # logInfo(str(firstRow[firstCSVKey])  + " | " + str(secondRow[secondCSVKey]))     # Debug log if empty string
                            break
                    itterationsCount += 1
    
        logInfo("Matches found: " + str(len(foundMatches)))
        logInfo("records itterated over: " + str(itterationsCount))        
        # for match in foundMatches:
        #     logInfo(match)
        return foundMatches
 
def main(): 
    firstCSVToCompare = ""                  # CSV filename.csv
    firstCSVFieldToCompareFrom = ''         # CSV header key

    secondCSVToCompare = ""                 # CSV filename.csv
    secondCSVieldToCompareFrom = ''         # CSV header key


    resultsFound = CSVComparison(secondCSVToCompare, secondCSVieldToCompareFrom, firstCSVToCompare, firstCSVFieldToCompareFrom)
    
    unfoundMatches = []

    with open(firstCSVToCompare, encoding='utf-8') as CSV1:         # read CSV file
        firstCSVReader = csv.DictReader(CSV1)                       # load csv file data using csv library's dictionary reader

        for firstRow in firstCSVReader:                             # itterate through first CSV rows
            found = False
            for match in resultsFound:
                if (str(firstRow[firstCSVFieldToCompareFrom]) == match):
                    found = True
                    break
            if found == False:
                unfoundMatches.append(str(firstRow[firstCSVFieldToCompareFrom]))
        logInfo("Total records not found: "  + len(unfoundMatches))


if __name__ == "__main__":
    main()
