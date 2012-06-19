#!/usr/bin/env python
""""""

import sys
import os
import couchdb
import hashlib
import time

def  main():
        couch   = couchdb.Server("http://maggie.scilifelab.se:5984")
        qc      = couch['qc']
	obj = eval(open("test.json").read())
        save_obj(qc, obj)

def save_obj(db, obj):
    dbobj = db.get(obj['_id'])
    if dbobj is None:
        obj["creation_time"] = time.strftime("%x %X")
        obj["modification_time"] = time.strftime("%x %X")
        db.save(obj)
    else:
        obj["_rev"] = dbobj.get("_rev")
        if obj != dbobj:
            obj["creation_time"] = dbobj["creation_time"]
            obj["modification_time"] = time.strftime("%x %X")
            db.save(obj)
    return True



if __name__ == '__main__':
        main()
