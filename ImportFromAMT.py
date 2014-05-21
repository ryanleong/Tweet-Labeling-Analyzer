import csv
import couchdb
import json

filename = 'Batch_1540632_batch_results.csv'
databaseIP = 'http://127.0.0.1:5984'
database = 'results/nbn_test'

def importResults(db):
    database = []
    columnRef = {}
    isHeader = True
    columnNumber = 0
    entries = 0

    labelColumn = -1
    workerIDColumn = -1

    # couchdb map function
    map_fun = '''function(doc) { if(doc.doc_type == "tweet"){
    emit(doc.tweet_id, doc); }}'''

    reader = csv.reader(open(filename, 'rb'))

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
            workerid = ""
            assignmentid = ""

            for element in row:

                # get woker ID
                if columnNumber == workerIDColumn:
                    workerid = element

                # get assignment ID
                if columnNumber == 15:
                    assignmentid == element

                # get labelling
                if columnNumber >= labelColumn:
                    
                    # check if column is empty
                    if element != "":
                        entries += 1
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
                                #temp = json.loads('{"worker_id" : "%s", "rating": %d, "assignment_id": "%s"}' % (workerid, int(element), assignmentid))
                                temp = json.loads('{"worker_id" : "%s", "rating": %d}' % (workerid, int(element)))
                                
                                tweet['worker_ratings'].append(temp)

                                # store to db
                                db.save(tweet)
                            else:
                                print element, "already in DB."
                                pass


                # increment column number
                columnNumber += 1
            print "Number of answered questions for:", entries
            entries = 0



if __name__ == "__main__":

    # database IP    
    couch = couchdb.Server(databaseIP)
    
    # set database to query
    db = couch[database]

    importResults(db)
