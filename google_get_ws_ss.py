#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import bcbio.google
import bcbio.google.spreadsheet

ssheet_title=' '.join(sys.argv[1:len(sys.argv)])
print ssheet_title

credentials_file = '/bubo/home/h24/mayabr/config/gdocs_credentials'
credentials = bcbio.google.get_credentials({'gdocs_upload': {'gdocs_credentials': credentials_file}})
client  = bcbio.google.spreadsheet.get_client(credentials)
print client
#ssheet  = bcbio.google.spreadsheet.get_spreadsheet(client,ssheet_title)
feed = bcbio.google.spreadsheet.get_spreadsheets_feed(client,ssheet_title, False)
print len(feed.entry)
if len(feed.entry) == 0:
    ssheet=None
else:
    #ssheet=feed.entry
    for ssheet in feed.entry:
	print ssheet.title.text.split('_20132')[0].lstrip().rstrip().rstrip('_')
assert ssheet is not None, "Could not find spreadsheet '%s'" % ssheet_title

