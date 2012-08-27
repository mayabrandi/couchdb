#!/usr/bin/env python
"""google_docs_get_run_info.py
Created by Maya Brandi on 2012-06-05.
"""
import bcbio.google
import bcbio.google.spreadsheet
import sys
import os
import hashlib
import couchdb
import time
from scilifelab.scripts.bcbb_helpers.process_run_info import _replace_ascii
import bcbio.scilifelab.google.project_metadata as pm
import bcbio.pipeline.config_loader as cl
import logging
from bcbio.google import _to_unicode

def get_proj_inf(project_name_swe,qc,credentials_file,config_file):
	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename='ProjectSummary.log',level=logging.INFO)#,datefmt='%Y-%m-%d'
	project_name	 = _replace_ascii(_to_unicode(project_name_swe))
	key  		 = hashlib.md5(project_name).hexdigest()
	print key
        obj={   'Application':'',
		'Min_M_reads_per_sample_ordered':'',
		'No_of_samples':'',
                'Entity_type': 'ProjectSummary',
                'Uppnex_id': '',                 
		'Samples': {},
                'Project_id': project_name, 
                'Entity_version': 0.1,
                '_id': key}

	logging.warning(str('Proj '+project_name+' '+ key))

	### Get minimal #M reads and uppnexid from Genomics Project list
	print '\nGetting minimal #M reads and uppnexid from Genomics Project list for project ' + project_name_swe
	config = cl.load_config(config_file)
	p = pm.ProjectMetaData(project_name,config)
	if p.project_name==None:
		p = pm.ProjectMetaData(project_name_swe,config)
	if p.project_name==None:
		print project_name+' not found in genomics project list'
		logging.warning(str('Google Document Genomics Project list: '+project_name+' not found'))
	else:
                obj['Min_M_reads_per_sample_ordered'] = p.min_reads_per_sample
                obj['Uppnex_id']                      = p.uppnex_id
                obj['No_of_samples']                  = p.no_samples
		obj['Application']		      = p.application



	### Get costumer and Scilife Sample name from _20132_0X_Table for Sample Summary and Reception Control
	print '\nTrying to find Scilife Sample names from '+project_name_swe+'_20132_0X_Table for Sample Summary and Reception Control'

       	versions = { 	"01":["Data",'Sample name Scilife (Index included)'],
			"02":["Sheet1",'Sample name Scilife'],
			"04":["Reception control",'SciLifeLab ID'],
			"05":["Reception control",'SciLifeLab ID']}

	# Load google document
	client	= make_client(credentials_file)
	feed 	= bcbio.google.spreadsheet.get_spreadsheets_feed(client,project_name_swe+'_20132', False) #FIXA: Hantera mistakes
	if len(feed.entry) == 0:
    		ssheet=None
		logging.warning("Google Document %s: Could not find spreadsheet" % str(project_name_swe+'_20132_XXX'))
		print "Could not find spreadsheet" 
	else:
    		ssheet	= feed.entry[0].title.text
  		version	= ssheet.split('_20132_')[1].split(' ')[0].split('_')[0]
		wsheet 	= versions[version][0]
		header 	= versions[version][1]
		content, ws_key, ss_key = get_google_document(ssheet, wsheet, credentials_file)
	
	# Get Scilife Sample names
	try:    
	   	dummy, customer_names_colindex 	= get_column(content,'Sample name from customer')
		row_ind, scilife_names_colindex = get_column(content, header)
		info={}
                for j,row in enumerate(content):
			if (j > row_ind):
				try:
					info[str(row[scilife_names_colindex]).strip().replace('-','_')] = str(row[customer_names_colindex]).strip()
				except:
					pass
		print 'Names found'
		for scilife_name in info:
			try:
				obj['Samples'][scilife_name] = {'customer_name': info[scilife_name], 'scilife_name':scilife_name}
			except:
				pass
        except:
		print 'Names not found'
                pass

	### Get Sample Status from _20158_01_Table for QA HiSeq2000 sequencing results for samples
	print '\nGetting Sample Status from '+project_name_swe+'_20158_0X_Table for QA HiSeq2000 sequencing results for samples'

        versions = {    "01":['Sample name Scilife',"Total reads per sample","Passed=P/ not passed=NP*"],
                        "02":["Sample name (SciLifeLab)","Total number of reads (Millions)","Based on total number of reads"],
                        "03":["Sample name (SciLifeLab)","Total number of reads (Millions)","Based on total number of reads"]}
        # Load google document
	mistakes = ["_"," _"," ",""]
	found='FALSE'
	for m in mistakes:
		feed    = bcbio.google.spreadsheet.get_spreadsheets_feed(client,project_name_swe + m + '20158', False)
        	if len(feed.entry) == 0:
                	ssheet=None
                	print "Could not find spreadsheet"
		else:
			ssheet  = feed.entry[0].title.text
			version = ssheet.split(str(m+'20158_'))[1].split(' ')[0].split('_')[0]	
	        	wsheet = "Sheet1"
			try:
				content, ws_key, ss_key = get_google_document(ssheet,wsheet,credentials_file)
				found='TRUE'
				break
                	except:
				pass
	if found=='TRUE':
		print 'Google document found!'
	else:
		print 'Google document NOT found!'
		logging.warning("Google Document %s: Could not find spreadsheet" % str(project_name_swe+'_20158_XXX'))
	# Get status etc from loaded document
	try:
		dummy, P_NP_colindex 			= get_column(content,versions[version][2])
		dummy, No_reads_sequenced_colindex 	= get_column(content,versions[version][1])
        	row_ind, scilife_names_colindex 	= get_column(content,versions[version][0])
		info={}
                for j,row in enumerate(content):
			if ( j > row_ind ):
				try:
					info[str(row[scilife_names_colindex]).strip()]=[str(row[P_NP_colindex]).strip(),str(row[No_reads_sequenced_colindex]).strip()]
				except:
					pass
		scilife_names 	= strip_scilife_name_prep(info.keys())
		duplicates	= find_duplicates(scilife_names.values())
		print duplicates
		for key in scilife_names:
			striped_scilife_name = scilife_names[key]
			print striped_scilife_name
			try:
				if striped_scilife_name in duplicates:
					obj['Samples'][striped_scilife_name] = {'status':'inconsistent','M_reads_sequenced':'inconsistent'}
                		elif obj['Samples'].has_key(striped_scilife_name):
                        		obj['Samples'][striped_scilife_name]['status']            = info[key][0]
                        	        obj['Samples'][striped_scilife_name]['M_reads_sequenced'] = info[key][1]
			except:
				pass
        except:
		print 'Status and M reads sequenced not found in '+project_name_swe+'_20158_0X_Table for QA HiSeq2000 sequencing results for samples'
		logging.warning("Google Document %s: Status and M reads sequenced not found" % str(project_name_swe+'_20158_XXX'))
                pass


	### Get _id for SampleQCMetrics and bcbb names  
	#	use couchdb views instead.... To be fixed...
	print '\nGetting _id for SampleQCMetrics'
	info={}
        for key in qc:
                try:
                        SampQC = qc.get(key)
                        if (SampQC["entity_type"] == "SampleQCMetrics") & SampQC.has_key("sample_prj"):
                                if SampQC["sample_prj"] == project_name:
					try:
						info[str(SampQC["barcode_name"]).strip()]=[SampQC["_id"],SampQC["name"]]
                                        except:
						pass

		except:
			pass
    	scilife_names = strip_scilife_name_prep(info.keys())
	if len(info.keys())>0:
		print 'SampleQCMetrics found on couchdb for the folowing samples:'
		print info.keys()
		print scilife_names
	else:
		print 'no SampleQCMetrics found on couchdb for project '+ project_name
		logging.warning(str('CouchDB: No SampleQCMetrics found for project '+ project_name))
        for key in scilife_names:
        	scilife_name=scilife_names[key]
                if obj['Samples'].has_key(scilife_name):
        		if obj['Samples'][scilife_name].has_key("SampleQCMetrics"):
				print info[key][0]
				print "SampleQCMetricsSampleQCMetricsSampleQCMetrics"
                		obj['Samples'][scilife_name]["SampleQCMetrics"].append(info[key][0])
                        else:
                              	obj['Samples'][scilife_name]["SampleQCMetrics"] = [info[key][0]]
                if obj['Samples'].has_key(scilife_name):
                        if obj['Samples'][scilife_name].has_key("bcbb_names"):
                                obj['Samples'][scilife_name]["bcbb_names"].append(info[key][1])
                        else:
                                obj['Samples'][scilife_name]["bcbb_names"] = [info[key][1]]

	return obj

#		GOOGLE DOCS
def get_google_document(ssheet_title,wsheet_title,credentials_file):
	""""""
	client  = make_client(credentials_file)
	ssheet 	= bcbio.google.spreadsheet.get_spreadsheet(client,ssheet_title)
	wsheet 	= bcbio.google.spreadsheet.get_worksheet(client,ssheet,wsheet_title)
	content = bcbio.google.spreadsheet.get_cell_content(client,ssheet,wsheet)
	ss_key 	= bcbio.google.spreadsheet.get_key(ssheet)
	ws_key 	= bcbio.google.spreadsheet.get_key(wsheet)
	return content, ws_key, ss_key

def make_client(credentials_file):
        credentials = bcbio.google.get_credentials({'gdocs_upload': {'gdocs_credentials': credentials_file}})
        client  = bcbio.google.spreadsheet.get_client(credentials)
        return client

def get_column(ssheet_content,header):
	""""""
	colindex=''
	for j,row in enumerate(ssheet_content):
                if colindex == '':
			try:
                        	for i,col in enumerate(row):
					try:
                                		if str(col).strip() == header:
                                        		colindex = i
					except:
						pass
			except:
				pass
		else:
			rowindex = j-1
			return rowindex, colindex

#		COUCHDB
def save_couchdb_obj(db, obj):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename='ProjectSummary.log',level=logging.INFO)#,datefmt='%Y-%m-%d'
    dbobj = db.get(obj['_id'])
    if dbobj is None:
        obj["creation_time"] = time.strftime("%x %X")
        obj["modification_time"] = time.strftime("%x %X")
        db.save(obj)
	logging.info('CouchDB: '+obj['_id'] + ' ' + obj['Project_id'] + ' Created ')
    else:
        obj["_rev"] = dbobj.get("_rev")
	del dbobj["modification_time"]
	obj["creation_time"] = dbobj["creation_time"]
        if comp_obj(obj,dbobj)==False:    
	    print "uppdating couchdb"
            obj["modification_time"] = time.strftime("%x %X")
            db.save(obj)
	    logging.info('CouchDB: '+obj['_id'] + ' ' + obj['Project_id'] + ' Uppdated ')

def comp_obj(obj,dbobj):
	for key in dbobj:
		if (obj.has_key(key)):
			if (obj[key]!=dbobj[key]):
	                     return False
	     	else:
			return False
	return True


#		NAME HANDELING
def strip_scilife_name_index(names):
	N={}
        for name_init in names:
                name  = name_init.replace('-','_').split(' ')[0].split("_index")[0].strip()
                if name !='':
                	N[name_init]=name
       	return N

def find_duplicates(list):
	dup=[]
        shown=[]
        for name in list:
        	if name in shown and name not in dup:
                	dup.append(name)
                shown.append(name)
	return dup

def strip_scilife_name_prep(names):
	N={}
        preps = '_BCDE'
        for name_init in names:
		name = name_init.replace('-','_').split(' ')[0].split("_index")[0].strip()
		if name !='':
			while name[-1] in preps:
				name=name[0:-1]
			if name !='':
                        	N[name_init]=name
	return N


def  main(credentials_file,config_file, proj_ID = None):
	client	= make_client(credentials_file)
        couch   = couchdb.Server("http://maggie.scilifelab.se:5984")
        qc      = couch['qc']
	if proj_ID==None:
	        feed = bcbio.google.spreadsheet.get_spreadsheets_feed(client,'20132', False)
	        try:
        	        for ssheet in feed.entry:
                	        proj_ID = ssheet.title.text.split('_20132')[0].lstrip().rstrip().rstrip('_')
                        	if (proj_ID !=''):
                                	try:
                                        	obj = get_proj_inf(proj_ID ,qc ,credentials_file, config_file )
                                        	if obj['Samples'].keys()!=[]:
                                        	        save_couchdb_obj(qc, obj)
                                	except:
                                        	pass
        	except:
	               	pass
	else:
	        obj = get_proj_inf(proj_ID ,qc ,credentials_file, config_file )
        	if obj['Samples'].keys()!=[]:
                	save_couchdb_obj(qc, obj)


if __name__ == '__main__':
	credentials_file = '/bubo/home/h24/mayabr/config/gdocs_credentials'
	config_file='/bubo/home/h24/mayabr/config/post_process.yaml'
	if len(sys.argv)>1:
		main(credentials_file,config_file,sys.argv[1])
	else:
		main(credentials_file,config_file)



