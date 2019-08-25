# read the main file and shwo the 1st record
data = sc.textFile("/home/sandipan/Documents/Learning_PySpark/VS14MORT.txt.gz")
data.take(1)
#take records count
data.count()
#filter the data
data_filter = data.filter(lambda x: "I64" in x)
data_filter.take(1)
data_filter.count()
#split the data with reguler expression
import re
#one or more spaces
pattern = re.compile(" +")
data_parse = data.map(lambda v: pattern.split(v))


#data_parse = data.map(_.split("\\t")) # why error? AttributeError: 'list' object has no attribute 'split'

# check the data 
data_parse.take(1)

#print the few lines
for element in data_parse.take(2):
    print(element)
    print(element[1])
# notice that not all the lines have same number of columns or data point

# lets print the number of elements in a row
for element in data_parse.take(10):
    print(len(element))


#------------------------------------------------------------------------------
# i split the file in 6, as I have 6 core processer in local mode
# GZ has no split option so I am using BZ2


# try using snappy -- local snappy is not working 
# using uncompressed file format
data = sc.textFile("/home/sandipan/Documents/Learning_PySpark/VS14MORT.txt", 6)
data.take(1)
#take records count
data.count()
import re
#one or more spaces
pattern = re.compile(" +")
data_parse = data.map(lambda v: pattern.split(v))
data_parse.first()
data_len = import re
#one or more spaces
pattern = re.compile(" +")
# add new length column
data_col_length = data_parse.map(lambda v: (v,len(v)))
# how to filter the maximum length?
data_col_length.filter(