#!/usr/bin/env python
# encoding: utf-8

import re
import couchdb
import sys

databaseIP = ''     #'http://127.0.0.1:5984'
database = 'all_tweets'

# get command line arguements
if len(sys.argv) != 2:
    print 'python ImportFromDataFile.py <database_ip>'
    exit()
else:
    # Database IP
    databaseIP = str(sys.argv[1])


def inDB(tweetID, tweet, db):
    inDatabase = False
    
    map_fun = '''function(doc) {
    emit(doc.tweet_id,null); }'''

    results = db.query(map_fun,key=tweetID)
    
    if len(results) != 0:
        inDatabase = True
    else:
        
        map_fun = '''function(doc) {
        emit(doc.value,null); }'''
    
        results = db.query(map_fun,key=tweet)
        
        if len(results) != 0:
            inDatabase = True
    
    return inDatabase

def importFromFile():
    
    print "Starting import.."
    count = 0
    
    try:
        # read from file
        with open('Tweets2000.dat', 'r') as fileContent:
            content = fileContent.readlines()
    except:
        print "No such file in directory."
        exit()

    try:
        # database IP    
        couch = couchdb.Server(databaseIP)
    except:
        print "Could not connect to database IP."
        exit()

    try:
        db = couch.create(database)
        print "database created"
    except:
        # set database to query
        db = couch[database]
        print "database set"

    tweetsPerSet = 0
    hitSetNum = 0
    
    for line in content:
        
        if count >= 1500:
            break
        
        # keep track of sets
        if tweetsPerSet >= 50:
            hitSetNum += 1
            tweetsPerSet = 0
        
        # split line by space
        lineParts = line.split()
        
        # form up twitter data after split
        tweet = "";
        for index in range(2, len(lineParts)):
            
            if index == 2:
                tweet = tweet + lineParts[index]
            else:
                tweet = tweet + " " + lineParts[index]
            
            tweet = re.sub(r"(?:\@|https?\:)\S+", "", tweet)
            
            tweet = re.sub(r"&gt;", ">", tweet)
            
            tweet = re.sub(r"&lt;", "<", tweet)
            
            tweet = re.sub(r"&amp;", "&", tweet)

        if inDB(lineParts[1], tweet, db) == False:
            doc = {'expert_ratings': [], 'tweet_id' : lineParts[1], 
                   'value': tweet}
            
            db.save(doc)
            
            tweetsPerSet += 1
            
            count += 1
        else:
            #print "Skipped", lineParts[1]
            pass
    
    db.cleanup()
    db.compact()
    
    # print completion
    print "Import complete."

if __name__ == "__main__":
    # Run import
    importFromFile()
