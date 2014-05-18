import csv
import couchdb
import json

def importResults(db):
    database = []
    columnRef = {}
    isHeader = True
    columnNumber = 0

    labelColumn = -1
    workerIDColumn = -1

    # couchdb map function
    map_fun = '''function(doc) {
    emit(doc.tweet_id,doc); }'''

    reader = csv.reader(open('results.csv', 'rb'))

    for row in reader:
        # rest column count
        columnNumber = 0

        # append header
        if isHeader == True:

            for element in row:
                # find answer column
                if "Answer" in element:
                    # store tweet IDs
                    columnRef[columnNumber] = int(element[7:])

                    # store label column
                    if labelColumn == -1:
                        labelColumn = columnNumber
                
                # find workerid column
                if "WorkerId" in element:
                    
                    # store worker id column
                    if workerIDColumn == -1:
                        workerIDColumn = columnNumber

                # increment column number
                columnNumber += 1

            isHeader = False

        # if not header
        else:
            workerid = "";

            for element in row:

                # get woker ID
                if columnNumber == workerIDColumn:
                    workerid = element

                # get labelling
                if columnNumber >= labelColumn:
                    
                    if element != "":

                        results = db.query(map_fun,key=str(columnRef[columnNumber]))

                        for tweet in results:
                            tweet = json.loads(json.dumps(tweet.value))

                            inDB = False

                            # check if rating exist
                            for rating in tweet['worker_ratings']:
                                if rating['worker_id'] == workerid:
                                    inDB = True
                                    break

                            if inDB == False:
                                # store data
                                temp = json.loads('{"worker_id" : "%s", "rating": %d}' % (workerid, int(element)))
                                tweet['worker_ratings'].append(temp)

                                # store to db
                                db.save(tweet)
                            else:
                                print element, "already in DB."


                # increment column number
                columnNumber += 1



if __name__ == "__main__":

    # database IP    
    couch = couchdb.Server('http://127.0.0.1:5984')
    
    # set database to query
    db = couch['test_final']

    importResults(db)
