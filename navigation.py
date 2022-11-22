import sys
from pyspark import SparkContext
import time

# Finds out the index of "name" in the array firstLine 
# returns -1 if it cannot find it
def findCol(firstLine, name):
	if name in firstLine:
		return firstLine.index(name)
	else:
		return -1


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("./data/CLIWOC15.csv")

# The first line of the file defines the name of each column in the cvs file
# We store it as an array in the driver program
firstLine = wholeFile.filter(lambda x: "RecID" in x).collect()[0].replace('"','').split(',')
# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not ("RecID" in x))
# split each line into an array of items
entries = entries.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()
entries.count()

##### Create an RDD that contains all nationalities observed in the
##### different entries

# Information about the nationality is provided in the column named
# "Nationality"
# First find the index of the column corresponding to the "Nationality"
column_index=findCol(firstLine, "Nationality")
# Use 'map' to create a RDD with all nationalities and 'distinct' to remove duplicates 
print("{} corresponds to column {}".format("Nationality", column_index))

nationalities = entries.map(lambda x:x[column_index]).distinct()

# Display the 5 first nationalities
print("A few examples of nationalities:")
for elem in nationalities.sortBy(lambda x: x).take(5):
	print(elem)
#2
print(entries.count())
#3
column_index_year=findCol(firstLine, "Year")
print(column_index_year)
#4
years = entries.map(lambda x:x[column_index_year]).distinct()
years.count()
# Display the odelset
print("oldest years:")
oldest=years.sortBy(lambda x: x).collect()[0]
print(oldest)
print("newest years:")
newest=years.sortBy(lambda x: x).collect()[-1]
print(newest)
#5 Display the years with the minimum and the maximum number of observations (and the corresponding number of observations)
l=years.collect()
print(l)
indexO=l.index(oldest)
print((oldest,indexO))
indexN=l.index(newest)
print((newest,indexN))
#6.Count the distinct departure places (column "VoyageFrom") using two methods 
#(i.e., usingthe function distinct() or reduceByKey()) and compare the execution time.
column_index_VoyageFrom=findCol(firstLine, "VoyageFrom")
#distinct
start=time.time()
voyageFrom = entries.map(lambda x:x[column_index_VoyageFrom]).distinct()
end=time.time()
print(end-start)
#reduceByKey
start=time.time()
voyageFrom = entries.reduceByKey(lambda x:x[column_index_VoyageFrom])
print(voyageFrom.collect())
end=time.time()
print(end-start)
