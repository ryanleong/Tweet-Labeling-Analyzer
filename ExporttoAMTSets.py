import couchdb
import json
import re

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
    couch = couchdb.Server('http://127.0.0.1:5984')
    
    # set database to query
    db = couch['twitter_data']
    
    # map function
    map_function = '''function(doc) { emit(doc.set,doc); }'''
    
    # write csv header
    writeCSVHeader()
    
    hitSetNum = 0
    
    f = open("sets.csv", "a")
    
    while hitSetNum < 30:
    
        # query from db
        results = db.query(map_function, key = hitSetNum)
        
        setString = ""
        firstElement = True
        
        for tweetData in results:
            tweet = json.loads(json.dumps(tweetData.value))
            
            if firstElement:
                setString = "\"" + str(tweet['_id']) + "\",\"" + re.sub(r"\"", " ", tweet['value']) + "\""
            else:
                setString += ",\"" + str(tweet['_id']) + "\",\"" + re.sub(r"\"", " ", tweet['value']) + "\""
        
        
        # write to file
        f.write(setString + "\n")
            
        hitSetNum += 1
    
if __name__ == "__main__":
    # export to CSV
    exportToCSV()