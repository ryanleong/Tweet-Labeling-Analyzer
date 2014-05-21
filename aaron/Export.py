import couchdb
import json
import re
import sys

databaseIP = ''     #'http://127.0.0.1:5984'
database = 'nbn'
database2 = 'flu'

filename = 'results_nbn.dat'
filename2 = 'results_flu.dat'

# get command line arguements
if len(sys.argv) != 2:
    print 'python Export.py <database_ip>'
    exit()
else:
    # Database IP
    databaseIP = str(sys.argv[1])


def export():
    
    # database IP
    couch = couchdb.Server(databaseIP)
    
    # set database to query
    db = couch[database]
    
    # map function
    map_function = '''function(doc) { emit(null,doc); }'''
    
    f = open(filename, "w")
    
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
    export()

    # change settings
    database = database2
    filename = filename2

    export()