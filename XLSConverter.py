def XLSToXLSX(XLSFile: str):
    import os
    os.rename(XLSFile, XLSFile + "x") # Rename to xlsx

def XLSXRowDateFormat(XLSXFile: str, sheetName: str, formatCol: int):
    # format date
    from openpyxl import Workbook, load_workbook
    from datetime import datetime
    import pandas as pd

    wb = load_workbook(XLSXFile)
    source = wb[sheetName]
    for cell in source['B']:
        if cell.value == "Date":
            continue
        splitCell = str(cell.value).split(' ')        
        dateFormatted = datetime.strptime(str(splitCell[0]), '%Y-%m-%d')
        cell.value = dateFormatted.strftime('%d-%b-%Y')

    wb.save(XLSXFile)


def XLSXRemoveDuplicateColumns(XLSXFile: str):
    # delete columns

    import pandas as pd

    dataFile = pd.read_excel(XLSXFile)

    writer = pd.ExcelWriter(XLSXFile)

    dataFile.to_excel(writer, index = False)
    writer.save()

# convert to CSV
def convertXLSXtoCSV(XLSXFile: str):
    #importing pandas as pd
    import pandas as pd
    
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

def main():
    filename = "DS_MASTER_7_13_2022.xls"
    try:
        if str(filename).endswith('x') != True:
            XLSToXLSX(filename)
            filename = filename + 'x'
    except: # it isn't an xls, is already an xlsx
        filename = filename + 'x'
        pass

    XLSXRowDateFormat(filename, 'Create', 1)
    XLSXRemoveDuplicateColumns(filename)
    convertXLSXtoCSV(filename)
    

if __name__ == "__main__":
    main()