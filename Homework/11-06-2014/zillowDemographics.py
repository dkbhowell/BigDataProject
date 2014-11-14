import urllib2
import xml 
import os
import xml.etree.ElementTree as etree

zips = file('zipcodes.txt', 'r')
url = 'http://www.zillow.com/webservice/GetDemographics.htm?zws-id=X1-ZWz1b260i9k4jv_7j3qs&zip='
outputName = 'output.txt'
# os.remove(output.txt)
output = open(outputName, 'w')
output.write('')

for line in zips:
	file = urllib2.urlopen(url + line)
	tree = etree.parse(file)
	notags = etree.tostring(tree, encoding='utf8', method='text')
	print(notags)
	# for line in file:
	# 	#data = file.readline()
	# 	print line
	# 	output.write(line)
	output.write(notags)
	output.write("\n")
file.close()
output.close()
neighborhoods.close()
