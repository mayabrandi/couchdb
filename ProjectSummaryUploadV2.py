#!/usr/bin/env python
"""
ProjectSummaryUploadV2
Created by Maya Brandi on 2012-09-00.
"""
from uuid import uuid4
import bcbio.google
import bcbio.google.spreadsheet
import sys
import os
import hashlib
import couchdb
import time
from  datetime  import  datetime
from scilifelab.scripts.bcbb_helpers.process_run_info import _replace_ascii
import bcbio.scilifelab.google.project_metadata as pm
import bcbio.pipeline.config_loader as cl
import logging
from bcbio.google import _to_unicode

def get_proj_inf(project_name_swe,samp_db,proj_db,credentials_file,config_file):
	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename='proj_coucdb.log',level=logging.INFO)
	project_name	 = _replace_ascii(_to_unicode(project_name_swe))
	key		= find_proj_from_view(proj_db,project_name)
	if not key:
		key = uuid4().hex

        logging.info(str('Handling proj '+project_name+' '+ key))
        print key

        obj={   'application':'',
		'customer_reference':'',
		'min_m_reads_per_sample_ordered':'',
		'no_of_samples':'',
                'entity_type': 'project_summary',
                'uppnex_id': '',                 
		'samples': {},
                'project_id': project_name, 
                '_id': key}


	### Get minimal #M reads and uppnexid from Genomics Project list
	print '\nGetting minimal #M reads and uppnexid from Genomics Project list for project ' + project_name_swe

	config = cl.load_config(config_file)
	p = pm.ProjectMetaData(project_name,config)
	if p.project_name == None:
		p = pm.ProjectMetaData(project_name_swe,config)
	if p.project_name == None:
		print project_name+' not found in genomics project list'
		logging.warning(str('Google Document Genomics Project list: '+project_name+' not found')) 
	else:
                obj['min_m_reads_per_sample_ordered'] = float(p.min_reads_per_sample)
                obj['uppnex_id']                      = p.uppnex_id
                obj['no_of_samples']                  = int(p.no_samples)
		obj['application']		      = p.application
		obj['customer_reference']             = p.customer_reference


	### Get costumer and Scilife Sample name from _20132_0X_Table for Sample Summary and Reception Control
	print '\nTrying to find Scilife Sample names from '+project_name_swe+'_20132_0X_Table for Sample Summary and Reception Control'

       	versions = { 	"01":["Data",'Sample name Scilife (Index included)'],
			"02":["Sheet1",'Sample name Scilife'],
			"04":["Reception control",'Complete sample name'],
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
					cust_name = str(row[customer_names_colindex]).strip()
					sci_name  = str(row[scilife_names_colindex]).strip().replace('-','_')
					if cust_name != '':
						info[sci_name] = cust_name
				except:
					pass
		print 'Names found'
		for scilife_name in info:
			try:
				obj['samples'][scilife_name] = {'customer_name': info[scilife_name], 'scilife_name':scilife_name}
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
                                        sci_name=str(row[scilife_names_colindex]).strip()
                                        no_reads=str(row[No_reads_sequenced_colindex]).strip()
                                        if sci_name[-1]=='F':
                                                status='P'
                                        else:
                                                status=str(row[P_NP_colindex]).strip()
                                        info[sci_name]=[status,no_reads]
				except:
					pass
		scilife_names 	= strip_scilife_name(info.keys())
		duplicates	= find_duplicates(scilife_names.values())
		for key in scilife_names:
			striped_scilife_name = scilife_names[key]
			try:
				if striped_scilife_name in duplicates:
					obj['samples'][striped_scilife_name] = {'status':'inconsistent','m_reads_sequenced':'inconsistent'}
                		elif obj['samples'].has_key(striped_scilife_name):
                        		obj['samples'][striped_scilife_name]['status']            = info[key][0]
                        	        obj['samples'][striped_scilife_name]['m_reads_sequenced'] = info[key][1]
			except:
				pass
        except:
		print 'Status and M reads sequenced not found in '+project_name_swe+'_20158_0X_Table for QA HiSeq2000 sequencing results for samples'
                pass


	### Get _id for SampleQCMetrics and bcbb names -- use couchdb views instead.... To be fixed...
	print '\nGetting _id for SampleQCMetrics'

	info={}
        for key in samp_db:
		SampQC = samp_db.get(key)
                if SampQC.has_key("entity_type"):
                        if (SampQC["entity_type"] == "SampleQCMetrics") & SampQC.has_key("sample_prj"):
                                if SampQC["sample_prj"] == project_name:
					info[SampQC["_id"]]=[str(SampQC["name"]).strip(),SampQC["barcode_name"]]

	if len(info.keys())>0:
		print 'SampleQCMetrics found on couchdb for the folowing samples:'
		print info.values()
	else:
		print 'no SampleQCMetrics found on couchdb for project '+ project_name
		logging.warning(str('CouchDB: No SampleQCMetrics found for project '+ project_name))

        for key in info:
        	scilife_name=strip_scilife_name([info[key][1]])[info[key][1]]
                if obj['samples'].has_key(scilife_name):
        		if obj['samples'][scilife_name].has_key("sample_run_metrics"):
                		obj['samples'][scilife_name]["sample_run_metrics"][info[key][0]]=key
                        else:
                              	obj['samples'][scilife_name]["sample_run_metrics"] = {info[key][0]:key}
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
    dbobj	= db.get(obj['_id'])
    time_log 	= datetime.utcnow().isoformat() + "Z"
    if dbobj is None:
        obj["creation_time"] 	 = time_log 
        obj["modification_time"] = time_log 
        db.save(obj)
	return 'Created'
    else:
        obj["_rev"] = dbobj.get("_rev")
	del dbobj["modification_time"]
	obj["creation_time"] = dbobj["creation_time"]
        if comp_obj(obj,dbobj)==False:    
            obj["modification_time"] = time_log 
            db.save(obj)
	    return 'Uppdated'
    return None 

def comp_obj(obj,dbobj):
	for key in dbobj:
		if (obj.has_key(key)):
			if (obj[key]!=dbobj[key]):
	                     return False
	     	else:
			return False
	return True

def find_proj_from_view(proj_db,proj_id):
	view = proj_db.view('project/project_id')
	for proj in view:
		if proj.key==proj_id:
			return proj.value
	return None

#		NAME HANDELING
def find_duplicates(list):
	dup=[]
        shown=[]
        for name in list:
        	if name in shown and name not in dup:
                	dup.append(name)
                shown.append(name)
	return dup

def strip_scilife_name(names):
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
#       samp_db = couch['samples']
        proj_db = couch['projects']
        qc      = couch['qc']
	if proj_ID == None:
	        feed = bcbio.google.spreadsheet.get_spreadsheets_feed(client,'20132', False)
	        try:
        	        for ssheet in feed.entry:
                	        proj_ID = ssheet.title.text.split('_20132')[0].lstrip().rstrip().rstrip('_')
                        	if (proj_ID !=''):
                                	try:
                                        	obj = get_proj_inf(proj_ID ,qc,proj_db ,credentials_file, config_file )
                                        	if obj['samples'].keys()!=[]:
                                        		info =	save_couchdb_obj(proj_db, obj)
                                	except:
                                        	pass
        	except:
	               	pass
	else:
	        obj = get_proj_inf(proj_ID ,qc ,proj_db ,credentials_file, config_file )
        	if obj['samples'].keys()!=[]:
                	info = save_couchdb_obj(proj_db, obj)
	if info:
		print 'couchdb '+info
        	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename='proj_coucdb.log',level=logging.INFO)
		logging.info('CouchDB: '+obj['_id'] + ' ' + obj['project_id'] + ' ' +info)


if __name__ == '__main__':
	credentials_file = '/bubo/home/h24/mayabr/config/gdocs_credentials'
	config_file='/bubo/home/h24/mayabr/config/post_process.yaml'
	if len(sys.argv)>1:
		main(credentials_file,config_file,sys.argv[1])
	else:
		main(credentials_file,config_file)



