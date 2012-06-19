#!/usr/bin/env python

import sys
import couchdb


def fun(list):
	couch = couchdb.Server("http://maggie.scilifelab.se:5984")
	qc = couch['qc']

	try:
		list=list.split(' ')
	except:
		pass
	for key in list:
	     	obj = qc.get(key)
		try:
			print obj['Project_id']
			for key in  obj['samples']:
				print obj['samples'][key].keys()
		except:
			pass

if __name__ == "__main__":
    fun(sys.argv)
