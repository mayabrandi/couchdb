#!/usr/bin/env python
# -*- coding: utf-8 -*-
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



def get_proj_inf(project_name,qc):
         
	credentials_file = '/bubo/home/h24/mayabr/config/gdocs_credentials'
	key  		 = hashlib.md5(project_name).hexdigest()

        obj={   'min_M_reads_per_sample_ordered':'',
		'No_of_samples':'',
                'entity_type': 'ProjectSummary',
                'Uppnex_id': '',                 
		'samples': {},
                'Project_id': project_name, 
                'entity_version': 0.1,
                '_id': key}

	mistakes = ["_"," _"," ",""]

	# get minimal #M reads and uppnexid from Genomics Project list
	print 'get minimal #M reads and uppnexid from Genomics Project list'
	try:
		ssheet  = "Genomics Project list"
		wsheets = ["Finished 2011","Finished 2012","Ongoing"]
		for wsheet in wsheets:
			try:
				content, ws_key, ss_key		= get_google_document(ssheet,wsheet,credentials_file)		
				dummy, No_samps_colindex 	= get_column(content,'No of samples')
				dummy, uppnex_ID_colindex 	= get_column(content,'Uppnex ID')
				dummy, project_name_colindex 	= get_column(content,'Project name')
				dummy, min_reads_colindex 	= get_column(content,'minimal M read pairs/sample (passed filter)')
        			for j,row in enumerate(content):
					try:
						name = str(row[project_name_colindex]).strip()
						if name == project_name:
                					uppnexid  = str(row[uppnex_ID_colindex]).strip()
                	        			min_reads = str(row[min_reads_colindex]).strip()
							No_samps  = str(row[No_samps_colindex]).strip()
							obj['min_M_reads_per_sample_ordered'] 	= min_reads
							obj['Uppnex_id']    			= uppnexid
							obj['No_of_samples'] 			= No_samps
  							print 'yes'
	      	               				break
					except:
						pass
			except:
				pass
	except:
		pass


	# get costumer and Scilife Sample name from _20132_0*_Table for Sample Summary and Reception Control
	print 'Trying to find Scilife Sample names from _20132_0*_Table for Sample Summary and Reception Control'
	t  	= "_Table for Sample Summary and Reception Control"
	t2	= " QC table for sample summary and reception control"
	t3      = "_Table for Sample Summery and Reception Control"
	versions=["01","02","03","04","05"]
	for m in mistakes:
		for v in versions:
		 	if v == "05":
				ssheet = str(project_name+m+"20132_"+v+t2)
				wsheet = "Reception control"
				header = 'SciLifeLab ID' 
			elif v == "04":
				ssheet = project_name+m+"20132_"+v+t2
                                wsheet = "Reception control"
				header = 'SciLifeLab ID'
			elif v == "02":
                                ssheet = project_name+m+"20132_"+v+t
                                wsheet = "Sheet1" 
				header = 'Sample name Scilife'
			else:
				
				ssheet = project_name+m+"20132_"+v+t
				wsheet = "Data"
				header = 'Sample name Scilife (Index included)'
                                try: #	special case
                                        content, ws_key, ss_key = get_google_document(project_name+m+"20132_"+v+t3, wsheet, credentials_file)
					check=1
					break
                                except:
                                        pass
			try:
                		content, ws_key, ss_key = get_google_document(ssheet, wsheet, credentials_file)
				check=1
				break
        		except:   
				check=0
				pass 

		if check == 1:
			break
	if check == 1:
		print 'Google document found!'
	else:
		print 'Google document NOT found'
	try:    
	   	dummy, customer_names_colindex 	= get_column(content,'Sample name from customer')
		row_ind, scilife_names_colindex = get_column(content, header)
		info={}
                for j,row in enumerate(content):
			if (j > row_ind):
				try:
					info[str(row[scilife_names_colindex]).strip()] = str(row[customer_names_colindex]).strip()
				except:
					pass
		scilife_names = strip_scilife_name(info.keys())
		print 'Names found'
		for key in scilife_names:
			try:
				print 'name '+key+' was striped to '+ scilife_names[key]
				obj['samples'][scilife_names[key]] = {'customer_name': info[key], 'scilife_name':scilife_names[key]}
			except:
				pass
        except:
		print 'Names not found'
                pass

	# get Sample Status from _20158_01_Table for QA HiSeq2000 sequencing results for samples
	print 'Getting Sample Status from _20158_0*_Table for QA HiSeq2000 sequencing results for samples'
	for m in mistakes:
		ssheet = project_name + m + "20158_01_Table for QA HiSeq2000 sequencing results for samples"
	        wsheet = "Sheet1"
		try:
			content, ws_key, ss_key = get_google_document(ssheet,wsheet,credentials_file)
			print 'Google document found!'
			break
                except:
			print 'Google document NOT found'
			pass
	try:
	       	dummy, P_NP_colindex 			= get_column(content,'Passed=P/ not passed=NP*')
		dummy, No_reads_sequenced_colindex 	= get_column(content,'Total reads per sample')
        	row_ind, scilife_names_colindex 	= get_column(content,'Sample name Scilife')
		info={}
                for j,row in enumerate(content):
			if ( j > row_ind ):
				try:
					info[str(row[scilife_names_colindex]).strip()]=[str(row[P_NP_colindex]).strip(),str(row[No_reads_sequenced_colindex]).strip()]
				except:
					pass
		scilife_names = strip_scilife_name(info.keys())
		for key in scilife_names:
			try:
				scilife_name=scilife_names[key]
                		if obj['samples'].has_key(scilife_name):
                        		obj['samples'][scilife_name]['status']            = info[key][0]
                        	        obj['samples'][scilife_name]['M_reads_sequenced'] = info[key][1]
                        	else:
                                	obj['samples'][scilife_name] = {'status':info[key][0],'M_reads_sequenced':info[key][1]}
				print key+' '+info[key][0]+' '+info[key][1]
			except:
				pass
        except:
		print 'Status and M_reads_sequenced not found'
                pass


	# get _id for SampleQCMetrics   
	#	use couchdb views instead.... To be fixed...
	print 'get _id for SampleQCMetrics'
	info={}
        for key in qc:
                try:
                        SampQC = qc.get(key)
                        if (SampQC["entity_type"] == "SampleQCMetrics") & SampQC.has_key("sample_prj"):
                                if SampQC["sample_prj"] == project_name:
					try:
						info[str(SampQC["barcode_name"]).strip()]=SampQC["_id"]
                                        except:
						pass

		except:
			pass
    	scilife_names = strip_scilife_name(info.keys())
	if len(info.keys())>0:
		print 'SampleQCMetrics found on couchdb for the folowing samples:'
        for key in scilife_names:
        	scilife_name=scilife_names[key]
                if obj['samples'].has_key(scilife_name):
        		if obj['samples'][scilife_name].has_key("SampleQCMetrics"):
                		obj['samples'][scilife_name]["SampleQCMetrics"].append(info[key])
                        else:
                              	obj['samples'][scilife_name]["SampleQCMetrics"] = [info[key]]
             	else:
                     	obj['samples'][scilife_name]={"SampleQCMetrics":[info[key]]}

		print key+' '+info[key]
	print obj
	return obj


def get_google_document(ssheet_title,wsheet_title,credentials_file):
	""""""
	credentials = bcbio.google.get_credentials({'gdocs_upload': {'gdocs_credentials': credentials_file}})
	client 	= bcbio.google.spreadsheet.get_client(credentials)
	ssheet 	= bcbio.google.spreadsheet.get_spreadsheet(client,ssheet_title)
        if ssheet is None:
		f=open('fail_to_find_on_google_docs.txt','a')
                f.write(ssheet_title+"\n")
		f.close()
	assert ssheet is not None, "Could not find spreadsheet '%s'" % ssheet_title
	wsheet 	= bcbio.google.spreadsheet.get_worksheet(client,ssheet,wsheet_title)
	assert wsheet is not None, "Could not find worksheet %s within spreadsheet %s" % (wsheet_title,ssheet_title)
	content = bcbio.google.spreadsheet.get_cell_content(client,ssheet,wsheet)
	ss_key 	= bcbio.google.spreadsheet.get_key(ssheet)
	ws_key 	= bcbio.google.spreadsheet.get_key(wsheet)
	return content, ws_key, ss_key


def get_column(ssheet_content,header):
	""""""
	colindex=''
	for j,row in enumerate(ssheet_content):
                if colindex == '':
                        for i,col in enumerate(row):
				try:
                                	if str(col).strip() == header:
                                        			colindex = i
				except:
					pass
		else:
			rowindex = j-1
			return rowindex, colindex


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


def strip_scilife_name(names):
        preps = ['_B','B','_C','C','_D','D','_E','E']
	N1={}
	N2={}
	N3={}
	for name_init in names:
		name  = name_init.split(' ')[0].split("_index")[0].strip()
		if name !='':
	        	for prep in preps:
        	        	name = name.rstrip(prep)
        		try:
                        	name  = name.replace('-','_').split('_')
                        	N1[name_init]=name[0]
                        	if len(name)>1:
                        		N2[name_init]=name[1]
                               		N3[name_init]=name[0]+'_'+name[1]
        		except:
                		print 'trouble handeling scilife sample name '+ name_init
	
	if len(N1.values()) == len(set(N1.values())):
		return N1
	elif len(N2.values()) == len(set(N2.values())):
		return N2
	else:
		return N3


def  main2():
        credentials_file = '/bubo/home/h24/mayabr/config/gdocs_credentials'
        ssheet  = "Genomics Project list"
        wsheet  = "Ongoing"
        couch   = couchdb.Server("http://maggie.scilifelab.se:5984")
        qc      = couch['qc']

        f1=open('log1.txt','w')
        f2=open('log2.txt','w')

        content, ws_key, ss_key = get_google_document(ssheet,wsheet,credentials_file)
        project_name_rowindex, project_name_colindex = get_column(content,'Project name')        
        for j,row in enumerate(content):
      		try:
	        	name = str(row[project_name_colindex]).strip().split(' ')[0]
			if (name !='') & (j>project_name_rowindex):
				print 'Project: '+name 
                       		obj     = get_proj_inf(name,qc)
        			if obj['samples'].keys()!=[]:
                			save_obj(qc, obj)
                			#import simple_couch
                			#simple_couch.fun(obj['_id'])
                			print >> f1,obj['_id']+' '+name
					print 'couchdb uppdated'
        			else:
					print 'couchdb NOT uppdated with this project since no data was found'
                			print >> f2,name
		except:
			pass
	f1.close()
	f2.close()

def main(proj_ID):
        couch   = couchdb.Server("http://maggie.scilifelab.se:5984")
        qc      = couch['qc']
	obj     = get_proj_inf(proj_ID,qc)
        if obj['samples'].keys()!=[]:
                save_obj(qc, obj)
		#import simple_couch
		#simple_couch.fun(obj['_id'])
                return proj_ID+' '+obj['_id']
        else:
		return proj_ID



if __name__ == '__main__':
	if len(sys.argv)>1:
		print main(sys.argv[1])
	else:
		main2()
