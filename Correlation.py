#!/usr/bin/env python
import couchdb
import math

# Database
database = ''
databaseIP = 'http://127.0.0.1:5984'

allData = []

# get command line arguements
if len(sys.argv) != 2:
    print 'python Correlation.py <database_name>'
    exit()
else:
    # Database name
    database = str(sys.argv[1])


def getCorrelation():

    n = 1500;
    totalx = 0
    totaly = 0
    totalMul = 0
    totalXSqSum = 0
    totalYSqSum = 0

    for data in allData:
        totalx += data[1]
        totaly += data[2]
        totalMul += data[1] * data[2]

        totalXSqSum += (data[1] * data[1])
        totalYSqSum += (data[2] * data[2])

    top = (n * totalMul) - (totalx * totaly)
    #bottom = math.sqrt((n * totalXSqSum) - (totalx * totalx)) * math.sqrt((n * totalYSqSum) - (totaly * totaly))

    bottom = math.sqrt(((n * totalXSqSum) - (totalx * totalx)) * ((n * totalYSqSum) - (totaly * totaly)))

    print "Correlation: ", (top / bottom)



if __name__ == "__main__":
    
    # database IP    
    couch = couchdb.Server(databaseIP)
    
    # set database to query
    db = couch[database]
    
    map_fun = '''function(doc) {
    emit(doc.tweet_id,null); }'''
    
    results = db.iterview('_design/getratings/_view/expert',2000)
    
    for tweet in results:

        allData.append((tweet.key, tweet.value, tweet.value))

    getCorrelation()
