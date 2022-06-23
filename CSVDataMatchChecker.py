def logInfo(message):   # function for logging info related to the process of the application
    print(str(message))  # print to console

def CSVComparison(firstCSVPath : str, firstCSVKey : str, secondCSVPath : str, secondCSVKey : str): # Read two CSV files and compare two keys for matches
    import csv
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
                        # logInfo(str(firstRow[firstCSVKey])  + " | " + str(secondRow[secondCSVKey]))     # Debug log
                        if (str(secondRow[secondCSVKey]) == '' or str(firstRow[firstCSVKey]) == ''):
                            break
                    itterationsCount += 1
    
        logInfo("Matches found: " + str(len(foundMatches)))
        logInfo("records itterated over: " + str(itterationsCount))
 
def main(): 
    firstCSVToCompare = ""               # CSV filename.csv
    firstCSVFieldToCompareFrom = ''      # CSV header key

    secondCSVToCompare = ""              # CSV filename.csv
    secondSVFieldToCompareFrom = ''      # CSV header key

    CSVComparison(firstCSVToCompare, firstCSVFieldToCompareFrom, secondCSVToCompare, secondSVFieldToCompareFrom)


if __name__ == "__main__":
    main()