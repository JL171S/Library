def XLSToXLSX(XLSFile: str):
    import os
    try:
        os.rename(XLSFile, XLSFile + "x") # Rename to xlsx
    except Exception as e:
        print("File doesn't exist, try checking your input.")
        return

def XLSXRowDateFormat(XLSXFile: str, sheetName: str, formatCol: int):    # format date
    from openpyxl import Workbook, load_workbook
    from datetime import datetime

    try:
        wb = load_workbook(XLSXFile)    # load xlsx
        source = wb[sheetName]          # get page
        for cell in source['B']:        # column 'B' is Date in the expected XLSX file
            if cell.value == "Date":    # if on first row, skip.
                continue
            splitCell = str(cell.value).split(' ')                              # split date
            dateFormatted = datetime.strptime(str(splitCell[0]), '%Y-%m-%d')    # strip date and format
            cell.value = dateFormatted.strftime('%d-%b-%Y')                     # apply formatting

        wb.save(XLSXFile)               # save xlsx
    except Exception as e:
        if (str(e).startswith("'Worksheet")):
            print("File already converted or doesn't exist, please check file name.")
        else:
            print(e)
            return


def XLSXRemoveDuplicateColumns(XLSXFile: str):     # remove duplicate columns
    import pandas as pd
    try:
        dataFile = pd.read_excel(XLSXFile)          # load xlsx file
        writer = pd.ExcelWriter(XLSXFile)           # prep to write new xlsx file
        dataFile.to_excel(writer, index = False)    # write new xlsx file
        writer.save()                               # save it... this really removes duplicates by renaming duplicates to duplicateName 1 ... 2 ... 3 .. so on
    except Exception as e:
        print(e)
        return

def convertXLSXtoCSV(XLSXFile: str): # convert to CSV
    import pandas as pd
    
    try:
        # Read and store content
        # of an excel file 
        read_file = pd.read_excel (XLSXFile)
        
        # Write the dataframe object
        # into csv file
        read_file.to_csv (XLSXFile.split('.')[0] + ".csv", index = None, header=True)
            
        # read csv file and convert 
        # into a dataframe object
        dataFile = pd.DataFrame(pd.read_csv(XLSXFile.split('.')[0] + ".csv"))
        
        # show the dataframe
        dataFile
    except Exception as e:
        print(e)
        return

def main():
    import sys

    filename = str(sys.argv[1])  # get argument 1 from command line, expected input is filename.xls or .xlsx (e.g: "DS_MASTER_7_13_2022.xls")
    try:
        if str(filename).endswith('x') != True: # if this isn't an XLSX
            XLSToXLSX(filename)         # format to xlsx
            filename = filename + 'x'   # rename file name to .xlsx
    except: # it isn't an xls, is already an xlsx... I hope lol
        filename = filename + 'x'
        pass

    XLSXRowDateFormat(filename, 'Create', 1)    # format date
    XLSXRemoveDuplicateColumns(filename)        # remove duplicate columns
    convertXLSXtoCSV(filename)                  # convert to CSV
    

if __name__ == "__main__":
    main()