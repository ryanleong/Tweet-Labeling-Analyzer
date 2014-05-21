INSTRUCTIONS
=================

1. Install CouchDB

2. Run ImportFromDataFile.py <IP_of_couchdb>
(This will create a DB and store the tweets for both nbn and flu)

3. Deploy Tomcat WAR file
(The 2 pages are nbn.jsp, flu.jsp)

4. Run "Export.py <IP_of_couchdb>" when results completed

Results will be exported as a .dat file