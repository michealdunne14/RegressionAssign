import pandas as pd
import numpy as np
import pymysql
import pymysql.cursors

#Step 1
#Import CSV File
def importCSVFile(csvFile):
	#Reads CSV File
	return csvFile

#Step 2
#Clean Information Date 
def cleanseInformationDate(csvFile,csvFileyear):
	#sort by date of the CSV file
	# print(cleaningdate.unique())
	
	# Q1
	# Not all the data comes with the same date format. 
	# Q2
	# These are the three formats that are used yyyy-mm-dd,dd-mm-yyyy and dd/mm
	# One with a slash divider and no year and one a hiphan divider.
	# There is 169 of the format dd/mm and yyyy-mm-dd equals 8787 and dd-mm-yyyy is equals 169

	# Q3
	# There are 230 in total 
	print(len(csvFile))
	# df1.merge(csvFile[csvFile.str.contains("\d{2}/\d{2}", regex=True)])
	print(csvFile)
	# print("length")
	# print(len(csvFile[csvFile.str.contains("\d{2}/\d{2}", regex=True)]))
	# print(len(csvFile[csvFile.str.contains("\d{4}-\d{2}-\d{2}", regex=True)]))
	# print(len(csvFile[csvFile.str.contains("\d{2}-\d{2}-\d{4}", regex=True)]))
	

	csvFile = pd.to_datetime(csvFile,"coerce")
	#Drop all the null values
	csvFile = csvFile.dropna()
	#Print out the CSV files
	# cleaningdate = len(cleaningdate.unique())
	# print(cleaningdate)
	return csvFile

#Step 3
def cleanseInFormationType(csvFile):
	# Q1
	# There are 2 genuine categories with 'conventional' and  'organic' with 'Org.' being an odd value.

	# Q2
	# There is one category called 'Org.' which means organic. So this can be changed to organic. 

	# Q3 
	# The amount of entries that have errors are 169.
	# print(cleaningtype.unique())

	#Gets the amount of values that begin with Org. which is 169
	invalidValues = len(csvFile[csvFile == "Org."])
	# print(invalidValues)
	#Updates Org. to now be organic 
	csvFile[csvFile.str.contains("Org.", regex=True)]  = "organic"
	# print(csvFile.unique())
	countingOrg = len(csvFile[csvFile == "Org."])
	#When printing out this value it is 0 which means that it updated. 
	#print(countingOrg)
	return csvFile

#Step 4
def cleanseAveragePrice(csvFile):
	# Q1 
	# There are 20 values with missing values.

	# Q2
	# There are 30 entries that have erroneous string-based representation 

	# expectedValuenan = csvFile.isnull().sum()
	# print(expectedValuenan)
	#Drops all Null values 
	csvFile = csvFile.dropna()
	#Counts all values where a string contains a comma 
	countcommas = len(csvFile[csvFile.str.contains(",",regex=True)])
	# countcommas = cleaningprice[cleaningprice.str.contains(",", regex=True)]
	# print(countcommas)

	#Updates the comma to be a full stop 
	csvFile = csvFile.str.replace(",",".")
	pd.to_numeric(csvFile);
	csvFile = csvFile.dropna()
	# print(csvFile.unique())
	return csvFile

#Step 6
def ImportMySQL():
	connection = pymysql.connect(host="localhost",user="username",password="password",db="BSCY4")
	frame = pd.read_sql("select * from AVOCADO",connection)
	return frame

#Step 7
def cleanseRegion(SQLImport):
	# Q1
	# All the regions are represented in one word.

	# Q2 
	# There are 57 different regions. 

	# Q3 
	# The total number of Invalid regions is 149

	#Check which regions are unique
	differentregions = SQLImport.unique()
	# print(differentregions)
	#check the length of the different regions 
	#result is 57
	# print(len(differentregions))
	#Updates value where the region contains a "-" which the count is 80 
	countinvalidregion = len(SQLImport[SQLImport.str.contains("-",regex=True)])
	# print(countinvalidregion)
	SQLImport = SQLImport.str.replace("-"," ")
	#  Removes spaces before regions and after regions which is 69
	countinvalidregion = len(SQLImport[SQLImport.str.contains(" ",regex=True)])
	# print(countinvalidregion)
	SQLImport = SQLImport.str.replace(" ", "")
	return SQLImport

#Step 8
def cleanseYear(SQLImport):
	# Q1
	# There are fours years represented with 2018 been used twice

	# Q2 
	# Some of the years dont have the 20 before the year. e.g So the year is represented like this 17.

	# Q3
	# The amount of rows that are affected is 3208
	# Gets every unique year 
	uniqueyears = SQLImport.unique()
	# Prints out each unique year and gets the length of it which is 5 
	# print(uniqueyears)
	# Replaces the year 17 with 2017 and replaces the year 18 with 2018 
	replaceyears = SQLImport.replace({17: 2017, 18: 2018})
	# print(replaceyears.unique())
	# Counting the total number of times 17 and 18 occurs which is 3208
	count17 = len(SQLImport[SQLImport == 17])
	count18 = len(SQLImport[SQLImport == 18])
	total = count17 + count18
	# print(total)

#Step 9
def cleanseType(SQLImport):
	# Q1
	# The type of avocado that is represented is Conventional

	# Q2 
	# Some of the conventional have uppercase.

	# Q3 
	# There is 169 rows affected with Uppercase for the first letter.

	# Gets the unique types which is 2 
	uniquetype = SQLImport.unique()
	# print(uniquetype)

	# Some of the Conventional text is uppercase. I changed it to all be lowercase
	SQLImport = SQLImport.replace({"Conventional": "conventional"})
	# print(SQLImport.unique())


	# The total number of types with caps on is 169
	# count = len(SQLImport[SQLImport == "Conventional"])
	# print(count)
	return SQLImport

#Step 10 
def visualInspection(importCSV,importSQL):
	# Q1
	# The two data frames are suitable for consolidation.

	# Q2
	# There is a column called Unnamed in CSV that does not exist in SQL.
	# The Column TotalValue in CSV is called Total Volume in the SQL.
	# The Columns 4046,4770 and 4225 in the CSV is called c4046,c4770 and c4225 in the SQL.
	# The columns Small Bags, Large Bags, XLarge Bags all have spaces before the word Bags

	# Q3
	importCSV = importCSV.drop("Unnamed: 0",axis=1)
	# print(importCSV.columns)

	test = importCSV.rename(columns={'Total Volume':'TotalValue','4046':'c4046','4225':'c4225','4770':'c4770','Small Bags':'SmallBags','Large Bags':'LargeBags','XLarge Bags':'XLargeBags','Total Bags':'TotalBags'}, errors="raise")
	# print(test)
	# print(importSQL)

	# Step 11	
	# Q1
	# Outer is what we need. This keeps only the common values in both the left and right dataframes for the merged data.

	# Q2 
	mergeData = pd.concat([test,importSQL], axis=0, join='outer',sort=True)
	# print(len(mergeData.columns))
	return mergeData

#Main 
def main():
	BSCY4 = pd.read_csv("BSCY4.csv")
	importCSV = importCSVFile(BSCY4)

	importCSV["Date"] = cleanseInformationDate(importCSV["Date"],importCSV["year"])
	# print(cleanDate)
	importCSV["type"] = cleanseInFormationType(importCSV["type"])
	# print(importCSV["type"])
	importCSV["AveragePrice"] = cleanseAveragePrice(importCSV["AveragePrice"])

	importSQL = ImportMySQL()

	importSQL["region"] = cleanseRegion(importSQL["region"])
	importSQL["year"]  = cleanseYear(importSQL["year"])
	importSQL["type"] = cleanseType(importSQL["type"])
	inspection = visualInspection(importCSV,importSQL)
	print(inspection)
	# print(importSQL)
	# print("------------------------")
	# print(importCSV)
	# print(inspection)
	# print(cleanDate)
	# print(cleanType)
	# print(cleanPrice)

main()