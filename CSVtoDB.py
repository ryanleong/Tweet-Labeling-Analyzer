import re
import couchdb

if __name__ == "__main__":
    # read from file
    with open('Tweets1500.dat', 'r') as fileContent:
        content = fileContent.readlines()
    
    # database IP    
    couch = couchdb.Server('http://127.0.0.1:5984')
    
    # set database to query
    db = couch['test']

    tweetsPerSet = 0
    hitSetNum = 0
    
    for line in content:
        
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
        
        # create and insert doc to db
        doc = {'expertRating': lineParts[0], 'tweetID' : lineParts[1], 
               'value': tweet, 'set': hitSetNum}
        db.save(doc)
        
        #debug
        print doc['set']
        
        tweetsPerSet += 1