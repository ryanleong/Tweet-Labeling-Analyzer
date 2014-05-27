#!/bin/bash

python ImportFromDataFile.py http://localhost:5984
python ExporttoAMTSets.py http://localhost:5984
python ImportFromAMT.py http://localhost:5984
python Analyzer.py http://localhost:5984
python ExportForGraphs.py http://localhost:5984
