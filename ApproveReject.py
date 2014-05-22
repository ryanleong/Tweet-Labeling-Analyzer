import csv
import couchdb
import json

filename = 'results/nbn_no_au.csv'
databaseIP = 'http://127.0.0.1:5984'
database = 'nbn_test'

def importResults():
    database = []
    columnRef = {}
    isHeader = True
    columnNumber = 0
    entries = 0

    labelColumn = -1
    workerIDColumn = -1
    assignmentidColumn = -1
    rejectColumn = -1

    # couchdb map function
    map_fun = '''function(doc) { if(doc.doc_type == "tweet"){
    emit(doc.tweet_id, doc); }}'''

    reader = csv.reader(open(filename, 'rb'))

    f = open('results/nbn_no_au_upload.csv','w')



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

                # find assignmentID column
                if "AssignmentId" in element:
                    # store assignment id column
                    if assignmentidColumn == -1:
                        assignmentidColumn = columnNumber

                out = ""
                if "Reject" in element:
                    rejectColumn = columnNumber
                    out = '"%s"' % (element)
                else:
                    out = '"%s",' % (element)

                # increment column number
                columnNumber += 1

                f.write(out)

            f.write("\n")
            isHeader = False

        # if not header
        else:
            workerid = ""
            assignmentid = ""

            for element in row:
                out = ""

                # get woker ID
                if columnNumber == workerIDColumn:
                    workerid = element

                # get assignment ID
                if columnNumber == assignmentidColumn:
                    assignmentid = element

                # get labeling
                if columnNumber >= labelColumn:

                    # check if column is empty
                    if element != "":
                        entries += 1

                if columnNumber == rejectColumn -1:
                    break
                else:
                    out = '"%s",' % (element)

                # increment column number
                columnNumber += 1

            # out = ""
            # if columnNumber == rejectColumn:
            #   rejectColumn = columnNumber
            #   out = '"%s"' % (element)
            # else:
            #   out = '"%s",' % (element)

                f.write(out)

            if entries < 25:
                print "Assignment", assignmentid, "has only", entries, "entries"
            entries = 0

            f.write('\n')

    f.close() # you can omit in most cases as the destructor will call if


if __name__ == "__main__":

    importResults()
