import urllib2
import xml 
import os
import xml.etree.ElementTree as etree

#get zipcode data 

# zips = file('APIinput/zipcodes.txt', 'r')
# url = 'http://www.zillow.com/webservice/GetDemographics.htm?zws-id=X1-ZWz1b260i9k4jv_7j3qs&zip='
# outputName = 'output_zips.txt'
# # os.remove(output.txt)
# output_zips = open(outputName, 'w')
# output_zips.write('')

# for line in zips:
# 	file = urllib2.urlopen(url + line)
# 	#for line in file:
# 	data = file.read()
# 	print data
# 	output_zips.write(data)
# 	# output_zips.write(notags)
# 	output_zips.write("\n")
# file.close()
# output_zips.close()

#get neighborhood data 

hoods = file('APIinput/hoodsnospace.txt', 'r')
url = 'http://www.zillow.com/webservice/GetDemographics.htm?zws-id=X1-ZWz1b260i9k4jv_7j3qs&state=NY&city=NewYork&neighborhood='
outputName = 'output_hoods.txt'
# os.remove(output.txt)
output_hoods = open(outputName, 'w')
output_hoods.write('')

for line in hoods:
	file = urllib2.urlopen(url + line)
	#for line in file:
	data = file.read()
	print line
	output_hoods.write(data)
	# output_hoods.write(notags)
	output_hoods.write("\n")
file.close()
output_hoods.close()
