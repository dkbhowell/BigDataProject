import urllib2
import xml 
import os
import xml.etree.ElementTree as etree

zips = file('zipcodes.txt', 'r')
url = 'http://www.zillow.com/webservice/GetDemographics.htm?zws-id=X1-ZWz1b260i9k4jv_7j3qs&zip='
outputName = 'output.txt'
os.remove(outputName)
output = open(outputName, 'w')
output.write('')

for line in zips:
	file = urllib2.urlopen(url + line)
	# for l in file:
	data = file.read()
	print data
	output.write(data)
	# output.write("\n")
	# output.write("\n")
file.close()
output.close()
zips.close()
