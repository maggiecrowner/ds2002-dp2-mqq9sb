#!/bin/env python

from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json
from json.decoder import JSONDecodeError

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
db = client.mqq9sb
collection = db.project

path = "/workspace/ds2002-dp2-mqq9sb/data"

for (root, dirs, file) in os.walk(path):
    for f in file:
        with open(path+'/'+f, 'r') as dat:
            try:
                file_data = json.load(dat)
                if isinstance(file_data, list):
                    collection.insert_many(file_data)
                elif isinstance(file_data, dict):
                    collection.insert_one(file_data)
            except json.JSONDecodeError:
                print(f"Corrupted - {f}")
        continue
