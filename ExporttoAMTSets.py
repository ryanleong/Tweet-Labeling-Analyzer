import couchdb
import json
import re
import sys

# Database name
database = ''
databaseIP = ''
filename = ''

# get command line arguements
if len(sys.argv) != 2:
    print 'python ExporttoAMTSets.py <database_ip>'
    exit()
else:
    # Database IP
    databaseIP = str(sys.argv[1])


def writeCSVHeader() :
    header = ""
    hitSetNum = 0
    
    # form header string
    while hitSetNum < 50:
        
        if hitSetNum == 0:
            header = "\"docID" + str(hitSetNum) + "\",\"tweet" + str(hitSetNum) + "\""
        else:
            header += ",\"docID" + str(hitSetNum) + "\",\"tweet" + str(hitSetNum) + "\""
            
        hitSetNum += 1
        
    # write header of csv to file
    with open(filename, "w") as myfile:
        myfile.write(header + "\n")

def exportToCSV():
    
    # database IP
    couch = couchdb.Server(databaseIP)
    
    # set database to query
    db = couch[database]
    
    # map function
    map_function = '''function(doc) { emit(doc.set,doc); }'''
    
    # write csv header
    writeCSVHeader()
    
    hitSetNum = 0
    
    f = open(filename, "a")
    
    while hitSetNum < 30:
    
        # query from db
        results = db.query(map_function, key = hitSetNum)
        
        setString = ""
        firstElement = True
        
        #print "Size of set", hitSetNum , ":", len(results)
        
        for tweetData in results:
            tweet = json.loads(json.dumps(tweetData.value))
            
            if firstElement:
                setString = "\"" + str(tweet['tweet_id']) + "\",\"" + re.sub(r"\"", " ", tweet['value']) + "\""
                firstElement = False
            else:
                setString += ",\"" + str(tweet['tweet_id']) + "\",\"" + re.sub(r"\"", " ", tweet['value']) + "\""
        
        
        # write to file
        f.write(setString + "\n")
            
        hitSetNum += 1
    
if __name__ == "__main__":

    print 'Creating NBN HIT sets..'

    # Settings for NBN
    database = 'nbn_tweets'
    filename = 'sets/' + database + '_sets.csv'

    # export to CSV
    exportToCSV()

    print 'Done!'
    print 'Creating Flu HIT sets..'

    # Settings for NBN
    database = 'flu_tweets'
    filename = 'sets/' + database + '_sets.csv'

    # export to CSV
    exportToCSV()
    print 'Done!'
