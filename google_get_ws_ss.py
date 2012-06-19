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
ssheet  = bcbio.google.spreadsheet.get_spreadsheet(client,ssheet_title)
assert ssheet is not None, "Could not find spreadsheet '%s'" % ssheet_title

#wsheet_title='Sheet1'
#        wsheet  = bcbio.google.spreadsheet.get_worksheet(client,ssheet,wsheet_title)
#        assert wsheet is not None, "Could not find worksheet %s within spreadsheet %s" % (wsheet_title,ssheet_title)
#        content = bcbio.google.spreadsheet.get_cell_content(client,ssheet,wsheet)
#        ss_key  = bcbio.google.spreadsheet.get_key(ssheet)
#        ws_key  = bcbio.google.spreadsheet.get_key(wsheet)


