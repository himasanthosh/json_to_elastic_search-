# -*- coding: utf-8 -*-
"""
Created on Sun May 24 06:59:53 2020

@author: hima
"""
import requests 
from elasticsearch import Elasticsearch,helpers 
import os, uuid
import json
import glob
import io
import tkinter as tk
from tkinter import *
from tkinter import filedialog
from dateutil import parser
root = Tk()

Tk().withdraw() # we don't want a full GUI, so keep the root window from appearing
filename=filedialog.askdirectory()
path= os.path.abspath("./merged_file.json")
path

result = []

for f in glob.glob(filename+'/*.json'):
    with open(f, "r") as infile:
        h=json.load(infile)
        stri=str(json.dumps(h[0]))
        result.append(json.loads(stri))


with open(path, "w") as outfile:
     json.dump(result, outfile)
with io.open(path, 'r', encoding='utf-8-sig') as outfile:  
  JSON_file=json.load(outfile)
print(JSON_file)

with open(path, 'w') as outfile:
    for entry in JSON_file:
        json.dump(entry, outfile)
        outfile.write('\n')


def _decode(o):
    # Note the "unicode" part is only for python2
    if isinstance(o, str):
        try:
            return float(o)
        except ValueError:
            return o
    elif isinstance(o, dict):
        return {k: _decode(v) for k, v in o.items()}
    elif isinstance(o, list):
        return [_decode(v) for v in o]
    else:
        return o
filepath = os.path.abspath("./lyf.json")
print(filepath)
m=[]
with open(path, mode="r",encoding="utf-8") as my_file:
    for line in my_file:
        jsonDict =json.loads(line)
         # converting a dictionary object to json String
        jsonString = json.dumps(jsonDict)
        # converting a json string to json object
        jsonObj = json.loads(jsonString)
        # replacing the "published" value with date only
        l=[]
        l.append(jsonObj)
        for i in jsonDict.keys():
            if str(i) == "invoicedate":
                
                jsonObj["invoicedate"] = parser.parse(str(jsonDict[str(i)])).strftime('%Y-%m-%d')
                #print(jsonObj["invoicedate"])
                # outputs 2018-08-15
                
                # converting back to json string to print
                jsonString = json.dumps(jsonObj)
        
        m.append(json.loads(jsonString))
with open (path,'w') as n:
    json.dump(m, n)
with io.open(path, 'r', encoding='utf-8-sig') as outfile:  
     JSON_file=json.load(outfile)
with open(filepath, 'w') as outfile:
    for entry in JSON_file:
        json.dump(entry, outfile)
        outfile.write('\n')
                
# Then you can do:

l=[]        
metadata='{ "index": { "_index": "invoice", "_type": "doc" }}'
l=[]        
with open(filepath, mode="r",encoding="utf-8") as my_file:
    for line in my_file:
        l.append(metadata)
        a=line.rstrip()
        l.append(a)
with open(filepath, 'w') as outfile:
    for entry in JSON_file:
        json.dump(entry, outfile)
        outfile.write('\n')        
a='\n'.join(map(str, l))   
print(a)
with open(path, "w") as outfile:
    outfile.write(a) 

res = requests.get('http://localhost:9200') 
print(res.content)
elastic = Elasticsearch()

########ELS########
file_name=path
def script_path():
    path = os.path.dirname(os.path.realpath(file_name))
    if os.name == 'posix': # posix is for macOS or Linux
        path = path + "/"
    else:
        path = path + chr(92) # backslash is for Windows
    return path

def get_data_from_file(file_name):
    if "/" in file_name or chr(92) in file_name:
        file = open(file_name, encoding="utf8", errors='ignore')
    else:
        # use the script_path() function to get path if none is passed
        file = open(script_path() + str(file_name), encoding="utf8", errors='ignore')
        
        data = [line.strip() for line in file]
        file.close()
        return data
    

def bulk_json_data(file_name, _index, doc_type):
    json_list = get_data_from_file(file_name)
    for doc in json_list:
        # use a `yield` generator so that the data
        # isn't loaded into memory

        if '{"index"' not in doc:
            yield {
                "_index": _index,
                "_type": doc_type,
                "_id": uuid.uuid4(),
                "_source": doc
            }
try:
    # make the bulk call, and get a response
    response = helpers.bulk(elastic, bulk_json_data("merged_file.json", "final", "_doc"))
    print ("\nbulk_json_data() RESPONSE:", response)
except Exception as e:
    print("\nERROR:", e)