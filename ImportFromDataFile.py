#!/usr/bin/env python
# encoding: utf-8

import re
import couchdb
import sys

databaseIP = ''
database = ''
inputFile = ''

skipDuplicates = True

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
    
    count = 0
    
    try:
        # read from file
        with open(inputFile, 'r') as fileContent:
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
        # try to create database
        db = couch.create(database)
    except:
        # set database to query
        print 'Database', database, 'already exist. Delete database before importing.'
        return

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
            
            # Remove URLs, Mentions and other symbols
            tweet = re.sub(r"(?:\@|https?\:)\S+", "", tweet)
            tweet = re.sub(r"&gt;", ">", tweet)
            tweet = re.sub(r"&lt;", "<", tweet)
            tweet = re.sub(r"&amp;", "&", tweet)

        if skipDuplicates == True:
            if inDB(lineParts[1], tweet, db) == False:
                doc = {'expert_rating': int(lineParts[0]), 'tweet_id' : lineParts[1], 
                       'value': tweet, 'set': hitSetNum, 'doc_type': 'tweet', 'worker_ratings': [], 'expert_ratings' : []}
                
                db.save(doc)
                
                tweetsPerSet += 1
                
                count += 1
            # else:
            #     print "Skipped", lineParts[1]

        else:
            doc = {'expert_rating': int(lineParts[0]), 'tweet_id' : lineParts[1], 
                   'value': tweet, 'set': hitSetNum, 'doc_type': 'tweet', 'worker_ratings': [], 'expert_ratings' : []}
            
            db.save(doc)
            
            tweetsPerSet += 1
            
            count += 1
    
    # print completion
    print "Import complete."


if __name__ == "__main__":

    print "Starting NBN import.."

    # Database name
    database = 'nbn_tweets'

    # Input file name
    inputFile = 'data/Tweets2000.dat'

    # Run import
    importFromFile()

    print "Starting Flu import.."

    # Database name
    database = 'flu_tweets'

    # Input file name
    inputFile = 'data/Flu1500.dat'

    skipDuplicates = False

    # Run import
    importFromFile()
    