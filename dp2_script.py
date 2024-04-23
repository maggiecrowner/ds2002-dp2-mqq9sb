#!/bin/env python

from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
db = client.mqq9sb
collection = db.project

path = "/workspace/ds2002-dp2-mqq9sb/data"

def insert_data(data):
    try:
        if isinstance(file_data, list):
            collection.insert_many(file_data)
    except Exception:
        print(f"Error - {f}")  
    else:
        try:
            collection.insert_one(file_data)
        except Exception:
            print(f"Error - {f}")


for (root, dirs, file) in os.walk(path):
    for f in file:
        with open(path+'/'+f, 'r') as dat:
            file_data = json.load(dat)
            insert_data(file_data)
            continue