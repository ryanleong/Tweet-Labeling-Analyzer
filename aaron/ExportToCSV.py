import couchdb
import json
import re
import sys

databaseIP = ''     #'http://127.0.0.1:5984'
database = 'all_tweets'

# get command line arguements
if len(sys.argv) != 2:
    print 'python ExportToCSV.py <database_ip>'
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
    with open("sets.csv", "w") as myfile:
        myfile.write(header + "\n")

def exportToCSV():
    
    # database IP
    couch = couchdb.Server(databaseIP)
    
    # set database to query
    db = couch[database]
    
    # map function
    map_function = '''function(doc) { emit(null,doc); }'''
    
    f = open("results.dat", "w")
    
    # query from db
    results = db.query(map_function)

    
    for tweetData in results:
        tweet = json.loads(json.dumps(tweetData.value))

        del(tweet['_rev'])
        
        setString = json.dumps(tweet)
    
        # write to file
        f.write(setString + "\n")
        

if __name__ == "__main__":
    # export to CSV
    exportToCSV()