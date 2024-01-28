import pathlib
import csv
import os
import sys
import re
import getopt
import pandas as pd
import numpy as np
import argparse
import sqlite3 as sql
import pyodbc #for sqlserver
from xml.etree import ElementTree as ET
import collections
import sqlalchemy
import urllib 
from pathlib import Path
import shutil
import time

##################manipulate command line inputs data#####################
def manipulate_args(input):

    list_files = []
    files = []

    input_file = pathlib.Path(input["i"]) if os.path.isfile(input["i"]) is True else None
    input_path = pathlib.Path(input["i"]) if os.path.isdir(input["i"]) is True else None

    #Validate when input is a directory path or an XML file	
    if input_path is not None:
        for filename in os.listdir(input_path): 
            if filename.endswith('.xml'):
                file = os.path.join(input_path, filename)			
                list_files.append(Path(file))
                files.append(filename)					
        print("\n This is the list of XML files contained in the directory path: \n\n", files)
        #print("\n This is the list of XML files contained in the directory path: \n\n", list_files)		
    #check when is an input file
    if input_file is not None:
        if str(input_file).endswith('.xml'):
            print("The input file is an XML file")
            list_files.append(input_file)
            print(list_files)			
        else:	
            print("The input file is NOT an XML file")
            quit()			
    if input_file is None and input_path is None:	
        print("XML/Path input not found!!!!")
        quit()
		
    #output directory path
    if os.path.exists(input['dir']):
        output_base_dir = str(input['dir']) + "/"
    else:
	    output_base_dir = ""
    input['dir'] = output_base_dir
	
    #definir o path para projecto
    if str(input["ProjName"]) == 'None':
        output_dir_CSVs = output_base_dir + 'Output_MOs/'
    else: 
        output_dir_CSVs = output_base_dir + str(input["ProjName"]) + "/"

#    if not os.path.exists(output_dir_CSVs):
#        os.makedirs(output_dir_CSVs)
#    input['ProjName'] = output_dir_CSVs
	#1.if dir exists, then delete it. 2.Create the path.
    if os.path.exists(output_dir_CSVs):
        shutil.rmtree(output_dir_CSVs)
    os.makedirs(output_dir_CSVs)	
    input['ProjName'] = output_dir_CSVs		

    #definir Trace Analysis as default
    if input["Reporting"] not in ["1","2","3"]:
        input["Reporting"] = 1
    else:
        input["Reporting"] = int(input["Reporting"])

	#definir MDT as not default
    if str(input["MDT"]).capitalize() == "True":
       input["MDT"] = True
    else:
       input["MDT"] = False	

	#definir MRs default as 10
    if str(input["MRs"]) == "5":
       input["MRs"] = "5"
    elif str(input["MRs"]) == "1":
       input["MRs"] = "1"	   
    else:
       input["MRs"] = "10"	   
	   
	#definir PCMD as default for 5G Traces
    if str(input["PCMD"]).capitalize() == "False":
       input["PCMD"] = False
    else:
       input["PCMD"] = True	    

	#definir ENDC Feature available to check with LTE Traces  	   
    if str(input["ENDC"]).capitalize() == "True":
       input["ENDC"] = True
    else:
       input["ENDC"] = False

	#definir NSETAP interface available to check with LTE Traces  	   
    if str(input["NSETAP"]).capitalize() == "True":
       input["NSETAP"] = True
    else:
       input["NSETAP"] = False	   
	
   #definir MOs as not default
    if str(input["MOs"]).capitalize() == "True":
       input["MOs"] = True
    else:
       input["MOs"] = False		   

   #definir Trace Analysis as default	
    if input["AoI"]:
        if os.path.isfile(input["AoI"]):	
            AoI_file = pathlib.Path(input["AoI"])
            try:
                AoI_list = pd.read_csv(AoI_file, header = None, names = ["eUtranCellId"])
            except:
                AoI_list = pd.DataFrame()
                print("AoI file not readable!")	
        else:
            AoI_list = pd.DataFrame()
            print("AoI file not found!")				
    else:
        AoI_list = pd.DataFrame()
        print("AoI file not used!")

   #definir DB_SQLite as not default
    if str(input["DB_SQLite"]).capitalize() == "True":
       input["DB_SQLite"] = True
    else:
       input["DB_SQLite"] = False

   #definir DB_Postgres as not default
    if str(input["DB_Postgres"]).capitalize() == "True":
       input["DB_Postgres"] = True
    else:
       input["DB_Postgres"] = False	   

   #definir DB_SQLServer as not default
    if str(input["DB_SQLServer"]).capitalize() == "True":
       input["DB_SQLServer"] = True
    else:
       input["DB_SQLServer"] = False		   
	
    return(list_files, AoI_list)
	
#######################list of possible command line options#####################

def main():
    com = dict()
    parser = argparse.ArgumentParser(description = "Description for the command line inputs/outputs")
    parser.add_argument('-i', help = "<input: XML File/Folder to be parsed>", required = True)
    parser.add_argument('-AoI', help = "<input: File with AoI list>", required = False, default = None)
    parser.add_argument('-val', help = "<input: Validation Report Input>")
    parser.add_argument('-MOs', help = "Parsing for all the MOs? True/False", default = False)
    parser.add_argument('-DB_SQLite', help = "Create an SQLite DataBase True/False", required = False, default = False)
    parser.add_argument('-DB_Postgres', help = "Create an Postgres DataBase True/False", required = False, default = False)	
    parser.add_argument('-DB_SQLServer', help = "Create an SQL Server/TSQL DataBase True/False", required = False, default = False)		
    parser.add_argument('-ProjName', help = "output: <Project Name>", required = False)	
    parser.add_argument('-dir', help = "output directory: <Project Name>", required = False, default="")
    parser.add_argument('-MDT', help = "Checking MDT Feature Active? True/False", required = False, default = False)
    parser.add_argument('-PCMD', help = "Checking PCMD Feature Active? True/False", required = False, default = True)	
    parser.add_argument('-Thr', help = "Checking Throughout Feature Active? True/False", required = False, default = True)
    parser.add_argument('-ENDC', help = "Checking EN/DC Feature Active? True/False", required = False, default = False)
    parser.add_argument('-NSETAP', help = "Checking NSETAP Interface Active? True/False", required = False, default = False)
    parser.add_argument('-MRs', help = "Change MR periodicity new value? 1/5/10 (default= 10seg)", required = False, default = "10")
    parser.add_argument('-Reporting', help = "Trace Analysis:1, Network Analysis: 2, Both:3 ", required = False, default = 1)	
    args = parser.parse_args()
    com['i'] = args.i
    com['AoI'] = args.AoI
    com['val'] = args.val
    com['MOs'] = args.MOs
    com['DB_SQLite'] = args.DB_SQLite
    com['DB_Postgres'] = args.DB_Postgres
    com['DB_SQLServer'] = args.DB_SQLServer	
    com['dir'] = args.dir
    com['ProjName'] = args.ProjName
    com['MDT'] = args.MDT
    com['Thr'] = args.Thr
    com['MRs'] = args.MRs
    com['PCMD'] = args.PCMD
    com['ENDC'] = args.ENDC
    com['NSETAP'] = args.NSETAP	
    com['Reporting'] = args.Reporting

    file, AoI_list = manipulate_args(com) 	
    return com, file, AoI_list

##########################Parser including all vendors############################	

def parser (files, input):

    flag_Nokia = False
    flag_Ericsson = False
    flag_Huawei = False	
    items_Nokia = []
    items_Ericsson = []	
    items_Huawei = []	
    MOs_struct_Nokia = {}
    MOs_struct_Ericsson = {}
    MOs_struct_Huawei = {}	
	
    for file in files:
        print("File being parsed:",file)	
        tree = ET.parse(file)
        root = tree.getroot()
        if re.match("^\{raml[0-9]{2}\.xsd\}raml", root.tag):
            vendor = "Nokia"
            flag_Nokia = True
            items = Nokia_Collector(file, input['MOs'])			
            items_Nokia += items		
            MOs_struct_Nokia = check_MOs_structures(MOs_struct_Nokia, vendor, items)			
            #print(MOs_struct_Nokia)			
        elif (root.tag == "{configData.xsd}bulkCmConfigDataFile"):
            vendor = "Ericsson"
            flag_Ericsson = True			
            ##print("Ericsson")			
            items = Ericsson_Collector(file, input['MOs'])
            items_Ericsson += items			
            MOs_struct_Ericsson = check_MOs_structures(MOs_struct_Ericsson, vendor, items)					
            ##print(MOs_struct_Ericsson)
        elif root.tag == "{http://www.huawei.com/specs/SRAN}cmconfigdatafile" or root.tag == "{http://www.huawei.com/specs/SOM}cmconfigdatafile":
            vendor = "Huawei"
            flag_Huawei = True
            #print(vendor)
            items = Huawei_Collector(file, input['MOs'])
            #print(items)
            items_Huawei += items			
            MOs_struct_Huawei = check_MOs_structures(MOs_struct_Huawei, vendor, items)				
        else:
            print("Unknown vendor for the file:" + str(file), "Please check the file!!!")
            print(root.tag)
            quit()			

	#copy MOs to the different Outputs
    if flag_Nokia:	
        MOs_to_CSV_SQL("Nokia", items_Nokia, MOs_struct_Nokia, input)       
    if flag_Ericsson:	
        MOs_to_CSV_SQL("Ericsson", items_Ericsson, MOs_struct_Ericsson, input)
    if flag_Huawei:	 		
        MOs_to_CSV_SQL("Huawei", items_Huawei, MOs_struct_Huawei, input)

    return MOs_struct_Nokia, items_Nokia, flag_Nokia, MOs_struct_Ericsson, items_Ericsson, flag_Ericsson, MOs_struct_Huawei, items_Huawei, flag_Huawei

#############################Huawei 4G Parser####################################

#cmconfigdatafile
#|-->subsession
#####|-->NE
#########|-->module
#############|-->moi
################|-->attributes
#|-->fileheader

def Huawei_Collector(file, MOs_Flag):

    items = []
    header_dict = {}
    attr_dict = {}
    tree = ET.parse(file.open(encoding='utf-8'))
    root = tree.getroot()
    #print(root.tag)
    if root.tag == "{http://www.huawei.com/specs/SRAN}cmconfigdatafile" or root.tag == "{http://www.huawei.com/specs/SOM}cmconfigdatafile":
        huw_version = str(root.tag)[:-16]
        #print("cenas:", huw_version)		
		#começar abaixo do root
        for node in root.findall('./' + huw_version + 'fileheader'):
            header_dict['filetype'] =  str(dict(node.attrib).get('filetype'))		
        for node in root.findall('./' + huw_version + 'filefooter'):
            header_dict['file'] = str(file)
            header_dict['datetime'] = str(dict(node.attrib).get('datetime'))
 		#subsession level	
        for node in root.findall('./' + huw_version + 'subsession'):
            #NE level		
            for child1 in node.findall('./' + huw_version + 'NE'):
                NE_type = str(dict(child1.attrib).get('{http://www.w3.org/2001/XMLSchema-instance}type')) 				
                header_dict['netype'] = str(dict(child1.attrib).get('netype'))				
                header_dict['neversion'] = str(dict(child1.attrib).get('neversion'))			
                header_dict['neid'] = str(dict(child1.attrib).get('neid'))

                #module level			   
                for child2 in child1.findall('./' + huw_version + 'module'):
                    Module_type = str(dict(child2.attrib).get('{http://www.w3.org/2001/XMLSchema-instance}type'))
                    header_dict['module_type'] = Module_type				
                    #print(str(dict(child2.attrib).get('{http://www.w3.org/2001/XMLSchema-instance}type')) )
                    
					#field here depends if it is a controller or a Node					
                    if str(dict(child1.attrib).get('{http://www.w3.org/2001/XMLSchema-instance}type')) =='UMTS_RNC' or str(dict(child1.attrib).get('{http://www.w3.org/2001/XMLSchema-instance}type')) =='GSM_BSC':
                        header_dict['remark'] = str(dict(child2.attrib).get('remark'))
                        #print(str(dict(child2.attrib).get('productversion')))						
                    else:  
                        header_dict['productversion'] = str(dict(child2.attrib).get('productversion'))		
                        #print(str(dict(child2.attrib).get('productversion')))						
                    #moi level
                    for child3 in child2.findall('./' + huw_version + 'moi'):	
                        MO = str(dict(child3.attrib).get('{http://www.w3.org/2001/XMLSchema-instance}type'))
                        MO = NE_type + "_" + MO
                        #print("MO:",NE_type,"_",Module_type,"_",MO)						
                        if MO_filter_list("Huawei", MO, MOs_Flag) is True: 						
                            attr_dict['MO'] = MO				
                            #attributes level
                            for child4 in child3.findall('./' + huw_version + 'attributes'):
                                for child5 in child4:						
                                    attr_dict[(re.sub('{[^>]+}', '', str(child5.tag)))] = str(child5.text)
                                    if str(child5.text) is None:
                                        for child6 in child5:
                                            print("NEW layer of attributes ADDED!!!:", str(child6.tag), str(child6.text))	
										    
                        #if MO_filter_list("Huawei", MO, MOs_Flag) is True:                        
                            items.append({**header_dict,  **attr_dict})					
                            attr_dict = {}
    return items;							

#############################Ericsson 4G Parser####################################

#bulkCmConfigDataFile
#|-->SubNetwork
#####|-->SubNetwork
#########|-->MEContext
#############|-->vsDataContainer
#################|-->attributes
#....
#############|-->ManagedElement
#################|-->VsDataContainer
#####################|-->attributes
#####################|-->VsDataContainer
#########################|-->attributes
#########################|-->VsDataContainer
#############################|-->attributes
#....

##5G new format -> apparently they removed SubNetwork
#bulkCmConfigDataFile
#|-->SubNetwork
#########|-->MEContext
#############|-->vsDataContainer
#################|-->attributes
#....
#############|-->ManagedElement
#################|-->VsDataContainer
#####################|-->attributes
#####################|-->VsDataContainer
#########################|-->attributes
#########################|-->VsDataContainer
#############################|-->attributes
#....

def Ericsson_Collector(file, MOs_Flag):

    items = []
    header_dict = {}
    MEId_dict = {}
    attr_dict = {}
    tree = ET.parse(file.open(encoding='utf-8'))
    root = tree.getroot()
    if(root.tag == "{configData.xsd}bulkCmConfigDataFile"):      
		#começar abaixo do root
        for node in root.findall('./{configData.xsd}fileFooter'):
            header_dict['FileName'] = str(file) 		
            header_dict['dateTime'] = str(dict(node.attrib).get('dateTime'))             		
        for node in root.findall('./{configData.xsd}configData'):
            header_dict['configData_dnPrefix'] = str(dict(node.attrib).get('dnPrefix'))          			
            #SubNetwork_1			
            for child1 in node:
                header_dict['SubNetwork_Id_1'] = str(dict(child1.attrib).get('id'))				

            #SubNetwork_2			
                for child2 in child1.findall('./{genericNrm.xsd}SubNetwork'):
                    header_dict['SubNetwork_Id_2'] = str(dict(child2.attrib).get('id'))					
			#MeContext	
                    for child3 in child2.findall("./{genericNrm.xsd}MeContext"):
                        header_dict['MeContext_Id'] = str(dict(child3.attrib).get('id'))				
			    #VsDataContainer 	
                        for child4 in child3.findall('./{genericNrm.xsd}VsDataContainer'):
                            vsDataContainer_Id = str(dict(child4.attrib).get('id'))							
                            for child5 in child4:
                                for child6 in child5:								
                                    if child6.tag == '{genericNrm.xsd}vsDataType':
                                        MO = str(child6.text).replace('{EricssonSpecificAttributes.xsd}','')[6:]
                                        attr_dict['vsDataType'] = MO
                                        attr_dict["vs" + MO + "_Id"] = vsDataContainer_Id 									    
                                    else:
                                        for child7 in child6:									
                                            attr_dict[str(child7.tag).replace('{EricssonSpecificAttributes.xsd}','')] = str(child7.text) if isinstance(child7.text, str) is True else ""
                        if MO_filter_list("Ericsson", MO, MOs_Flag) is True:                        
                            items.append({**header_dict,  **attr_dict})					
                        attr_dict = {}						
                #ManagedElement      
                        for child4 in child3.findall('./{genericNrm.xsd}ManagedElement'):
                            MEId_dict['ManagedElement_Id'] = str(dict(child4.attrib).get('id'))
                            #attributes - userLabel
                            for child5 in child4.findall('./{genericNrm.xsd}attributes/{genericNrm.xsd}userLabel'):
                                 MEId_dict['UserLabel'] = str(child5.text) if isinstance(child5.text, str) is True else ""								 
                            #VsDataContainer									
                            for child5 in child4.findall('./{genericNrm.xsd}VsDataContainer'):
                                vsDataContainer_Id_0 = str(dict(child5.attrib).get('id'))
								
                                #MO level - eNodeB level								
                                for child6 in child5.findall('./{genericNrm.xsd}attributes'):							
                                    #MO
                                    for child7 in child6.findall('./{genericNrm.xsd}vsDataType'):
                                        MO_0 = str(child7.text)[6:]									
                                        attr_dict['vsDataType'] = MO_0
                                        attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0										
                                    #Parametros
                                    if MO_filter_list("Ericsson", MO_0, MOs_Flag) is True:									
                                        for child7 in child6:
                                            for child8 in child7:
                                                tag8 = str(child8.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                nr_child = 0
                                                if child8.text is None:
                                                    str8=""	
                                                if child8.text is not None: 
                                                    #print("MO_0:", MO_0 + ";")
                                                    if re.search('[a-zA-Z0-9_]+', child8.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                        str8 = ""
                                                    else:
                                                        str8= str(child8.text)
                                                attr_dict[tag8] = str8 if check_Key(attr_dict, tag8) is False else ("" if attr_dict[tag8] is None else attr_dict[tag8]+ ";")  + str8#corrigi a parte de concatenação													                                         											   
                                                for child9 in child8:
                                                    nr_child += 1 												
                                                    tag9 = str(child9.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                    #print("MO_0:", MO_0  +"; "+ tag8 + ":" +tag9 )                                                   
                                                    if re.search('^[;]+$', attr_dict[tag8]):#aqui vou limpar os casos de str8 que vao agregndo ;;; quando o que interessa e o valor em no parametro da lyer abaixo
                                                        #print("MO_0:" + MO_0 +";"+ str(child8.tag) +":"+ str8)														
                                                        attr_dict[tag8] =""														
                                                        #print(";;;;;")
                                                    if child9.text:														
                                                        attr_dict[tag8 + '_' + tag9] =  str(child9.text) if check_Key(attr_dict, tag8 + '_' + tag9) is False else attr_dict[tag8 + '_' + tag9] + ";" + str(child9.text)
                                                    else:
                                                        attr_dict[tag8 + '_' + tag9] =  "" if check_Key(attr_dict, tag8 + '_' + tag9) is False else attr_dict[tag8 + '_' + tag9] + ";" + ""			     															
                                                    for child10 in child9:
                                                        print("NEW layer of attributes ADDED - Layer 0" + str(child10.tag) + ":" + str(child10.text))
                                                if nr_child == 0 and tag8 not in attr_dict.keys():
                                                    attr_dict[tag8] = None
                                    if MO_filter_list("Ericsson", MO_0, MOs_Flag) is True:      												
                                        items.append({**header_dict,**MEId_dict,**attr_dict})					
                                    attr_dict = {}
                                #MO level - 1 level below 								
                                for child6 in child5.findall('./{genericNrm.xsd}VsDataContainer'):								
                                    vsDataContainer_Id_1 = str(dict(child6.attrib).get('id'))								
                                    for child7 in child6.findall('./{genericNrm.xsd}attributes'):									
                                        #MO
                                        for child8 in child7.findall('./{genericNrm.xsd}vsDataType'):
                                            MO_1 = str(child8.text)[6:]									
                                            attr_dict['vsDataType'] = MO_1
                                            attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                            attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                        #Parametros
                                        if MO_filter_list("Ericsson", MO_1, MOs_Flag) is True:											
                                            for child8 in child7:
                                                for child9 in child8:
                                                    tag9 = str(child9.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                    nr_child = 0
                                                    if child9.text is None:
                                                            str9 = ""													
                                                    if child9.text is not None:
                                                        #print("MO_1:", MO_1)
                                                        if re.search('[a-zA-Z0-9_]+', child9.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                            str9 = ""
                                                        else:
                                                            str9 = str(child9.text)
                                                    attr_dict[tag9] = str9 if check_Key(attr_dict, tag9) is False else ("" if attr_dict[tag9] is None else attr_dict[tag9]+ ";")  + str9#corrigi a parte de concatenação	
                                                    for child10 in child9:  												
                                                        nr_child += 1 												
                                                        tag10 = str(child10.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                        if re.search('^[;]+$', attr_dict[tag9]):#aqui vou limpar os casos de str9 que vao agregndo ;;; quando o que interessa e o valor em no parametro da lyer abaixo
                                                            #print("MO_1:" + MO_1 +";"+ str(child9.tag) +":"+ str9)														
                                                            attr_dict[tag9] =""														
                                                            #print(";;;;;")
                                                        if child10.text:														
                                                            attr_dict[tag9 + '_' + tag10] =  str(child10.text) if check_Key(attr_dict, tag9 + '_' + tag10) is False else attr_dict[tag9 + '_' + tag10] + ";" + str(child10.text)
                                                        else:
                                                            attr_dict[tag9 + '_' + tag10] =  "" if check_Key(attr_dict, tag9 + '_' + tag10) is False else attr_dict[tag9 + '_' + tag10] + ";" + ""			    															
                                                        for child11 in child10:
                                                            print("NEW layer of attributes ADDED - Layer 1" + str(child11.tag) + ":" + str(child11.text))
                                                    if nr_child == 0 and tag9 not in attr_dict.keys():
                                                        attr_dict[tag9] = None
                                        if MO_filter_list("Ericsson", MO_1, MOs_Flag) is True: 													
                                            items.append({**header_dict,**MEId_dict,**attr_dict})					
                                        attr_dict = {}									
                                    #MO level - 2 level below 								
                                    for child7 in child6.findall('./{genericNrm.xsd}VsDataContainer'):								
                                        vsDataContainer_Id_2 = str(dict(child7.attrib).get('id'))								
                                        for child8 in child7.findall('./{genericNrm.xsd}attributes'):									
                                            #MO
                                            for child9 in child8.findall('./{genericNrm.xsd}vsDataType'):
                                                MO_2 = str(child9.text)[6:]	
                                                #print(str(child9.text))												
                                                attr_dict['vsDataType'] = MO_2
                                                attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2											
                                            #Parametros
                                            if MO_filter_list("Ericsson", MO_2, MOs_Flag) is True:											
                                                for child9 in child8:
                                                    for child10 in child9:
                                                        tag10 = str(child10.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                        nr_child = 0
                                                        if child10.text is None:														
                                                            str10 = ""														
                                                        if child10.text is not None:														
                                                            #print("MO_2:", MO_2)
                                                            if re.search('[a-zA-Z0-9_]+', child10.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                str10 = ""
                                                            else:
                                                                str10 = str(child10.text)
                                                        attr_dict[tag10] = str10 if check_Key(attr_dict, tag10) is False else ("" if attr_dict[tag10] is None else attr_dict[tag10]+ ";")  + str10#corrigi a parte de concatenação																								
                                                        for child11 in child10:
                                                            nr_child += 1 												
                                                            tag11 = str(child11.tag).replace('{EricssonSpecificAttributes.xsd}','')	
                                                            if re.search('^[;]+$', attr_dict[tag10]):#aqui vou limpar os casos de str10 que vao agregndo ;;; quando o que interessa e o valor em no parametro da lyer abaixo
                                                                #print("MO_2:" + MO_2 +";"+ str(child10.tag) +":"+ str10)														
                                                                attr_dict[tag10] =""														
                                                                #print(";;;;;")
                                                            if child11.text:														
                                                                attr_dict[tag10 + '_' + tag11] =  str(child11.text) if check_Key(attr_dict, tag10 + '_' + tag11) is False else attr_dict[tag10 + '_' + tag11] + ";" + str(child11.text)
                                                            else:
                                                                attr_dict[tag10 + '_' + tag11] =  "" if check_Key(attr_dict, tag10 + '_' + tag11) is False else attr_dict[tag10 + '_' + tag11] + ";" + ""		
                                                            for child12 in child11:
                                                                print("NEW layer of attributes ADDED - Layer 2" + str(child12.tag) + ":" + str(child12.text))
                                                        if nr_child == 0 and tag10 not in attr_dict.keys():
                                                            attr_dict[tag10] = None
                                            if MO_filter_list("Ericsson", MO_2, MOs_Flag) is True: 														
                                                items.append({**header_dict,**MEId_dict,**attr_dict})					
                                            attr_dict = {}													
                                        #MO level - 3 level below 								
                                        for child8 in child7.findall('./{genericNrm.xsd}VsDataContainer'):								
                                            vsDataContainer_Id_3 = str(dict(child8.attrib).get('id'))								
                                            for child9 in child8.findall('./{genericNrm.xsd}attributes'):									
                                                #MO
                                                for child10 in child9.findall('./{genericNrm.xsd}vsDataType'):
                                                    MO_3 = str(child10.text)[6:]									
                                                    attr_dict['vsDataType'] = MO_3
                                                    attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                    attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                    attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2
                                                    attr_dict["vs" + MO_3 + "_Id"] = vsDataContainer_Id_3										
                                                #Parametros
                                                if MO_filter_list("Ericsson", MO_3, MOs_Flag) is True:     												
                                                    for child10 in child9:
                                                        for child11 in child10:
                                                            nr_child = 0 											
                                                            tag11 = str(child11.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                            if child11.text is None:															
                                                                str11 = ""														
                                                            if child11.text is not None:															
                                                                #print("MO_3:", MO_3)
                                                                if re.search('[a-zA-Z0-9_]+', child11.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                    str11 = ""
                                                                else:
                                                                    str11 = str(child11.text)
                                                            attr_dict[tag11] = str11 if check_Key(attr_dict, tag11) is False else ("" if attr_dict[tag11] is None else attr_dict[tag11]+ ";")  + str11#corrigi a parte de concatenação																									
                                                            for child12 in child11:
                                                                nr_child += 1 												
                                                                tag12 = str(child12.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                                if re.search('^[;]+$', attr_dict[tag11]):#aqui vou limpar os casos de str11 que vao agregndo ;;; quando o que interessa e o valor em no parametro da lyer abaixo
                                                                    #print("MO_3:" + MO_3 +";"+ str(child11.tag) +":"+ str11)														
                                                                    attr_dict[tag11] =""														
                                                                    #print(";;;;;")
                                                                if child12.text:														
                                                                    attr_dict[tag11 + '_' + tag12] =  str(child12.text) if check_Key(attr_dict, tag11 + '_' + tag12) is False else attr_dict[tag11 + '_' + tag12] + ";" + str(child12.text)
                                                                else:
                                                                    attr_dict[tag11 + '_' + tag12] =  "" if check_Key(attr_dict, tag11 + '_' + tag12) is False else attr_dict[tag11 + '_' + tag12] + ";" + ""	  															
                                                                for child13 in child12:
                                                                    print("NEW layer of attributes ADDED - Layer 3" + str(child13.tag) + ":" + str(child13.text))
                                                            if nr_child == 0 and tag11 not in attr_dict.keys():
                                                                attr_dict[tag11] = None
                                                if MO_filter_list("Ericsson", MO_3, MOs_Flag) is True:															
                                                    items.append({**header_dict,**MEId_dict,**attr_dict})					
                                                attr_dict = {}												
                                            #MO level - 4 level below 								
                                            for child9 in child8.findall('./{genericNrm.xsd}VsDataContainer'):								
                                                vsDataContainer_Id_4 = str(dict(child9.attrib).get('id'))								
                                                for child10 in child9.findall('./{genericNrm.xsd}attributes'):									
                                                    #MO
                                                    for child11 in child10.findall('./{genericNrm.xsd}vsDataType'):
                                                        MO_4 = str(child11.text)[6:]									
                                                        attr_dict['vsDataType'] = MO_4
                                                        attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                        attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                        attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2
                                                        attr_dict["vs" + MO_3 + "_Id"] = vsDataContainer_Id_3
                                                        attr_dict["vs" + MO_4 + "_Id"] = vsDataContainer_Id_4										
                                                    #Parametros
                                                    if MO_filter_list("Ericsson", MO_4, MOs_Flag) is True:													
                                                        for child11 in child10:
                                                            for child12 in child11:
                                                                nr_child = 0 											
                                                                tag12 = str(child12.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                                if child12.text is None:																
                                                                    str12 = ""																
                                                                if child12.text is not None:																
                                                                    #print("MO_4:", MO_4)
                                                                    if re.search('[a-zA-Z0-9_]+', child12.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                        str12 = ""
                                                                    else:
                                                                        str12 = str(child12.text)
                                                                attr_dict[tag12] = str12 if check_Key(attr_dict, tag12) is False else ("" if attr_dict[tag12] is None else attr_dict[tag12]+ ";")  + str12#corrigi a parte de concatenação											
                                                                for child13 in child12:
                                                                    nr_child += 1 												
                                                                    tag13 = str(child13.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                                    if re.search('^[;]+$', attr_dict[tag12]):#aqui vou limpar os casos de str12 que vao agregndo ;;; quando o que interessa e o valor em no parametro da lyer abaixo
                                                                        #print("MO_4:" + MO_4 +";"+ str(child12.tag) +":"+ str12)														
                                                                        attr_dict[tag12] =""														
                                                                        #print(";;;;;")
                                                                    if child13.text:														
                                                                        attr_dict[tag12 + '_' + tag13] =  str(child13.text) if check_Key(attr_dict, tag12 + '_' + tag13) is False else attr_dict[tag12 + '_' + tag13] + ";" + str(child13.text)
                                                                    else:
                                                                        attr_dict[tag12 + '_' + tag13] =  "" if check_Key(attr_dict, tag12 + '_' + tag13) is False else attr_dict[tag12 + '_' + tag13] + ";" + ""	    															
                                                                    for child14 in child13:
                                                                        print("NEW layer of attributes ADDED - Layer 4" + str(child14.tag) + ":" + str(child14.text))
                                                                if nr_child == 0 and tag12 not in attr_dict.keys():
                                                                    attr_dict[tag12] = None
                                                    if MO_filter_list("Ericsson", MO_4, MOs_Flag) is True:																
                                                        items.append({**header_dict,**MEId_dict,**attr_dict})					
                                                    attr_dict = {}
                                                #MO level - 5 level below 								
                                                for child10 in child9.findall('./{genericNrm.xsd}VsDataContainer'):								
                                                    vsDataContainer_Id_5 = str(dict(child10.attrib).get('id'))								
                                                    for child11 in child10.findall('./{genericNrm.xsd}attributes'):									
                                                        #MO
                                                        for child12 in child11.findall('./{genericNrm.xsd}vsDataType'):
                                                            MO_5 = str(child12.text)[6:]									
                                                            attr_dict['vsDataType'] = MO_5
                                                            attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                            attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                            attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2
                                                            attr_dict["vs" + MO_3 + "_Id"] = vsDataContainer_Id_3
                                                            attr_dict["vs" + MO_4 + "_Id"] = vsDataContainer_Id_4
                                                            attr_dict["vs" + MO_5 + "_Id"] = vsDataContainer_Id_5											
                                                        #Parametros
                                                        if MO_filter_list("Ericsson", MO_5, MOs_Flag) is True:															
                                                            for child12 in child11:
                                                                for child13 in child12:
                                                                    tag13 = str(child13.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                                    nr_child = 0
                                                                    if child13.text is None:
                                                                        str13 = ""																	
                                                                    if child13.text is not None:
                                                                        #print("MO_5:", MO_5)
                                                                        if re.search('[a-zA-Z0-9_]+', child13.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                            str13 = ""
                                                                        else:
                                                                            str13 = str(child13.text)
                                                                    attr_dict[tag13] = str13 if check_Key(attr_dict, tag13) is False else ("" if attr_dict[tag13] is None else attr_dict[tag13]+ ";")  + str13#corrigi a parte de concatenação									
                                                                    for child14 in child13:
                                                                        nr_child += 1 												
                                                                        tag14 = str(child14.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                                        if re.search('^[;]+$', attr_dict[tag13]):#aqui vou limpar os casos de str13 que vao agregndo ;;; quando o que interessa e o valor em no parametro da lyer abaixo
                                                                            #print("MO_5:" + MO_5 +";"+ str(child13.tag) +":"+ str13)														
                                                                            attr_dict[tag13] =""														
                                                                            #print(";;;;;")
                                                                        if child14.text:														
                                                                            attr_dict[tag13 + '_' + tag14] =  str(child14.text) if check_Key(attr_dict, tag13 + '_' + tag14) is False else attr_dict[tag13 + '_' + tag14] + ";" + str(child14.text)
                                                                        else:
                                                                            attr_dict[tag13 + '_' + tag14] =  "" if check_Key(attr_dict, tag13 + '_' + tag14) is False else attr_dict[tag13 + '_' + tag14] + ";" + ""																
                                                                        for child15 in child14:
                                                                            print("NEW layer of attributes ADDED - Layer 5" + str(child15.tag) + ":" + str(child15.text))
                                                                    if nr_child == 0 and tag13 not in attr_dict.keys():
                                                                        attr_dict[tag13] = None
                                                        if MO_filter_list("Ericsson", MO_5, MOs_Flag) is True:																	
                                                            items.append({**header_dict,**MEId_dict,**attr_dict})					
                                                        attr_dict = {}														
                                                    #MO level - 6 level below 								
                                                    for child11 in child10.findall('./{genericNrm.xsd}VsDataContainer'):								
                                                        vsDataContainer_Id_6 = str(dict(child11.attrib).get('id'))								
                                                        for child12 in child11.findall('./{genericNrm.xsd}attributes'):									
                                                            #MO
                                                            for child13 in child12.findall('./{genericNrm.xsd}vsDataType'):
                                                                MO_6 = str(child13.text)[6:]									
                                                                attr_dict['vsDataType'] = MO_6
                                                                attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                                attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                                attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2
                                                                attr_dict["vs" + MO_3 + "_Id"] = vsDataContainer_Id_3
                                                                attr_dict["vs" + MO_4 + "_Id"] = vsDataContainer_Id_4
                                                                attr_dict["vs" + MO_5 + "_Id"] = vsDataContainer_Id_5	
                                                                attr_dict["vs" + MO_6 + "_Id"] = vsDataContainer_Id_6											
                                                            #Parametros
                                                            if MO_filter_list("Ericsson", MO_6, MOs_Flag) is True:															
                                                                for child13 in child12:
                                                                    for child14 in child13:
                                                                        nr_child = 0 											
                                                                        tag14 = str(child14.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                                        if child14.text is None:																		
                                                                            str14 = ""																		
                                                                        if child14.text is not None:																		
                                                                            #print("MO_6:", MO_6)
                                                                            if re.search('[a-zA-Z0-9_]+', child14.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                                str14 = ""
                                                                            else:
                                                                                str14 = str(child14.text)
                                                                        attr_dict[tag14] = str14 if check_Key(attr_dict, tag14) is False else ("" if attr_dict[tag14] is None else attr_dict[tag14]+ ";")  + str14#corrigi a parte de concatenação																												
                                                                        for child15 in child14:
                                                                            nr_child += 1 												
                                                                            tag15 = str(child15.tag).replace('{EricssonSpecificAttributes.xsd}','')	
                                                                            if re.search('^[;]+$', attr_dict[tag14]):#aqui vou limpar os casos de str14 que vao agregndo ;;; quando o que interessa e o valor em no parametro da lyer abaixo
                                                                                #print("MO_6:" + MO_6 +";"+ str(child14.tag) +":"+ str14)														
                                                                                attr_dict[tag14] =""														
                                                                                #print(";;;;;")
                                                                            if child15.text:														
                                                                                attr_dict[tag14 + '_' + tag15] =  str(child15.text) if check_Key(attr_dict, tag14 + '_' + tag15) is False else attr_dict[tag14 + '_' + tag15] + ";" + str(child15.text)
                                                                            else:
                                                                                attr_dict[tag14 + '_' + tag15] =  "" if check_Key(attr_dict, tag14 + '_' + tag15) is False else attr_dict[tag14 + '_' + tag15] + ";" + ""	   															
                                                                            for child16 in child15:
                                                                                print("NEW layer of attributes ADDED - Layer 6" + str(child16.tag) + ":" + str(child16.text))
                                                                        if nr_child == 0 and tag14 not in attr_dict.keys():
                                                                            attr_dict[tag14] = None
                                                            if MO_filter_list("Ericsson", MO_6, MOs_Flag) is True:																		
                                                                items.append({**header_dict,**MEId_dict,**attr_dict})					
                                                            attr_dict = {}


            # without SubNetwork_2	=> directly to MeContext		
                for child2 in child1.findall('./{genericNrm.xsd}MeContext'):
                    header_dict['SubNetwork_Id_2'] = None					
                    header_dict['MeContext_Id'] = str(dict(child2.attrib).get('id'))			
                #VsDataContainer 	
                    for child3 in child2.findall('./{genericNrm.xsd}VsDataContainer'):
                        vsDataContainer_Id = str(dict(child3.attrib).get('id'))							
                        for child4 in child3:
                            for child5 in child4:								
                                if child5.tag == '{genericNrm.xsd}vsDataType':
                                    MO = str(child5.text).replace('{EricssonSpecificAttributes.xsd}','')[6:]
                                    attr_dict['vsDataType'] = MO
                                    attr_dict["vs" + MO + "_Id"] = vsDataContainer_Id 									    
                                else:
                                    for child6 in child5:									
                                        attr_dict[str(child6.tag).replace('{EricssonSpecificAttributes.xsd}','')] = str(child6.text) if isinstance(child6.text, str) is True else ""
                    if MO_filter_list("Ericsson", MO, MOs_Flag) is True:                        
                        items.append({**header_dict,  **attr_dict})					
                    attr_dict = {}						
            #ManagedElement      
                    for child3 in child2.findall('./{genericNrm.xsd}ManagedElement'):
                        MEId_dict['ManagedElement_Id'] = str(dict(child3.attrib).get('id'))
                        #attributes - userLabel
                        for child4 in child3.findall('./{genericNrm.xsd}attributes/{genericNrm.xsd}userLabel'):
                            MEId_dict['UserLabel'] = str(child4.text) if isinstance(child4.text, str) is True else ""								 
                        #VsDataContainer									
                        for child4 in child3.findall('./{genericNrm.xsd}VsDataContainer'):
                            vsDataContainer_Id_0 = str(dict(child4.attrib).get('id'))
								
                            #MO level - eNodeB level								
                            for child5 in child4.findall('./{genericNrm.xsd}attributes'):							
                                #MO
                                for child6 in child5.findall('./{genericNrm.xsd}vsDataType'):
                                    MO_0 = str(child6.text)[6:]									
                                    attr_dict['vsDataType'] = MO_0
                                    attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0										
                                #Parametros
                                if MO_filter_list("Ericsson", MO_0, MOs_Flag) is True:									
                                    for child6 in child5:
                                        for child7 in child6:
                                            tag7 = str(child7.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                            nr_child = 0
                                            if child7.text is None:
                                                str7 = ""											
                                            if child7.text is not None:
                                                #print("MO_0:", MO_0)
                                                if re.search('[a-zA-Z0-9_]+', child7.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                    str7 = ""
                                                else:
                                                    str7= str(child7.text)
                                            attr_dict[tag7] = str7 if check_Key(attr_dict, tag7) is False else ("" if attr_dict[tag7] is None else attr_dict[tag7]+ ";")  + str7#corrigi a parte de concatenação	                                           											   
                                            for child8 in child7:
                                                nr_child += 1 												
                                                tag8 = str(child8.tag).replace('{EricssonSpecificAttributes.xsd}','')													
                                                if re.search('^[;]+$', attr_dict[tag7]):#aqui vou limpar os casos de str7 que vao agregndo ;;; quando o que interessa e o valor em no parametro da lyer abaixo
                                                    #print("MO_0:" + MO_0 +";"+ str(child7.tag) +":"+ str7)														
                                                    attr_dict[tag7] =""														
                                                    #print(";;;;;")
                                                if child8.text:														
                                                    attr_dict[tag7 + '_' + tag8] =  str(child8.text) if check_Key(attr_dict, tag7 + '_' + tag8) is False else attr_dict[tag7 + '_' + tag8] + ";" + str(child8.text)
                                                else:
                                                    attr_dict[tag7 + '_' + tag8] =  "" if check_Key(attr_dict, tag7 + '_' + tag8) is False else attr_dict[tag7 + '_' + tag8] + ";" + ""														
                                                for child9 in child8:
                                                    print("NEW layer of attributes ADDED - Layer 0" + str(child9.tag) + ":" + str(child9.text))
                                            if nr_child == 0 and tag7 not in attr_dict.keys():
                                                attr_dict[tag7] = None
                                if MO_filter_list("Ericsson", MO_0, MOs_Flag) is True:      												
                                    items.append({**header_dict,**MEId_dict,**attr_dict})					
                                attr_dict = {}
                            #MO level - 1 level below 								
                            for child5 in child4.findall('./{genericNrm.xsd}VsDataContainer'):								
                                vsDataContainer_Id_1 = str(dict(child5.attrib).get('id'))								
                                for child6 in child5.findall('./{genericNrm.xsd}attributes'):									
                                    #MO
                                    for child7 in child6.findall('./{genericNrm.xsd}vsDataType'):
                                        MO_1 = str(child7.text)[6:]									
                                        attr_dict['vsDataType'] = MO_1
                                        attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                        attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                    #Parametros
                                    if MO_filter_list("Ericsson", MO_1, MOs_Flag) is True:											
                                        for child7 in child6:
                                            for child8 in child7:
                                                tag8 = str(child8.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                nr_child = 0
                                                if child8.text is None:													
                                                    str8 = ""													
                                                if child8.text is not None:													
                                                   #print("MO_1:",MO_1)												   
                                                    if re.search('[a-zA-Z0-9_]+', child8.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                        str8 = ""
                                                    else:
                                                        str8= str(child8.text)													
                                                attr_dict[tag8] = str8 if check_Key(attr_dict, tag8) is False else ("" if attr_dict[tag8] is None else attr_dict[tag8]+ ";")  + str8#corrigi a parte de concatenação									
                                                for child9 in child8:  												
                                                    nr_child += 1 												
                                                    tag9 = str(child9.tag).replace('{EricssonSpecificAttributes.xsd}','')													

                                                    if re.search('^[;]+$', attr_dict[tag8]):#aqui vou limpar os casos de str8 que vao agregndo ;;; quando o que interessa e o valor em no parametro da layer abaixo
                                                        #print("MO_1:" + MO_1 +";"+ str(child8.tag) +":"+ str8)														
                                                        attr_dict[tag8] =""														
                                                        #print(";;;;;")	
                                                    if child9.text:															
                                                        attr_dict[tag8 + '_' + tag9] =  str(child9.text) if check_Key(attr_dict, tag8 + '_' + tag9) is False else attr_dict[tag8 + '_' + tag9] + ";" + str(child9.text)
                                                    else:
                                                        attr_dict[tag8 + '_' + tag9] =  "" if check_Key(attr_dict, tag8 + '_' + tag9) is False else attr_dict[tag8 + '_' + tag9] + ";" + ""																
                                                    for child10 in child9:
                                                        print("NEW layer of attributes ADDED - Layer 1" + str(child10.tag) + ":" + str(child10.text))
                                                if nr_child == 0 and tag8 not in attr_dict.keys():
                                                    attr_dict[tag8] = None
                                    if MO_filter_list("Ericsson", MO_1, MOs_Flag) is True: 													
                                        items.append({**header_dict,**MEId_dict,**attr_dict})					
                                    attr_dict = {}									
                                #MO level - 2 level below 								
                                for child6 in child5.findall('./{genericNrm.xsd}VsDataContainer'):								
                                    vsDataContainer_Id_2 = str(dict(child6.attrib).get('id'))								
                                    for child7 in child6.findall('./{genericNrm.xsd}attributes'):									
                                        #MO
                                        for child8 in child7.findall('./{genericNrm.xsd}vsDataType'):
                                            MO_2 = str(child8.text)[6:]									
                                            attr_dict['vsDataType'] = MO_2
                                            attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                            attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                            attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2											
                                        #Parametros
                                        if MO_filter_list("Ericsson", MO_2, MOs_Flag) is True:											
                                            for child8 in child7:
                                                for child9 in child8:
                                                    tag9 = str(child9.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                    nr_child = 0
                                                    if child9.text is None:													
                                                        str9 = ""													
                                                    if child9.text is not None:													
                                                        #print("MO_2:", MO_2)														
                                                        if re.search('[a-zA-Z0-9_]+', child9.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                            str9 = ""
                                                        else:
                                                            str9= str(child9.text)													
                                                    attr_dict[tag9] = str9 if check_Key(attr_dict, tag9) is False else ("" if attr_dict[tag9] is None else attr_dict[tag9]+ ";")  + str9#corrigi a parte de concatenação																																				
                                                    for child10 in child9:
                                                        nr_child += 1 												
                                                        tag10 = str(child10.tag).replace('{EricssonSpecificAttributes.xsd}','')													
													
                                                        if re.search('^[;]+$', attr_dict[tag9]):#aqui vou limpar os casos de str9 que vao agregndo ;;; quando o que interessa e o valor em no parametro da layer abaixo
                                                            #print("MO_2:" + MO_2 +";"+ str(child9.tag) +":"+ str9)														
                                                            attr_dict[tag9] =""														
                                                            #print(";;;;;")
                                                        if child10.text:																
                                                            attr_dict[tag9 + '_' + tag10] =  str(child10.text) if check_Key(attr_dict, tag9 + '_' + tag10) is False else attr_dict[tag9 + '_' + tag10] + ";" + str(child10.text)
                                                        else:
                                                            #print("MO_2:" + MO_2 +";"+ str(child9.tag) +":"+ str9 + "\n" + str(child10.tag) +":"+ str(child10.text))
                                                            attr_dict[tag9 + '_' + tag10] =  "" if check_Key(attr_dict, tag9 + '_' + tag10) is False else attr_dict[tag9 + '_' + tag10] + ";" + ""															 
                                                        for child11 in child10:
                                                            print("NEW layer of attributes ADDED - Layer 2" + str(child11.tag) + ":" + str(child11.text))
                                                    if nr_child == 0 and tag9 not in attr_dict.keys():
                                                        attr_dict[tag9] = None
                                        if MO_filter_list("Ericsson", MO_2, MOs_Flag) is True: 														
                                            items.append({**header_dict,**MEId_dict,**attr_dict})					
                                        attr_dict = {}													
                                    #MO level - 3 level below 								
                                    for child7 in child6.findall('./{genericNrm.xsd}VsDataContainer'):								
                                        vsDataContainer_Id_3 = str(dict(child7.attrib).get('id'))								
                                        for child8 in child7.findall('./{genericNrm.xsd}attributes'):									
                                            #MO
                                            for child9 in child8.findall('./{genericNrm.xsd}vsDataType'):
                                                MO_3 = str(child9.text)[6:]									
                                                attr_dict['vsDataType'] = MO_3
                                                attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2
                                                attr_dict["vs" + MO_3 + "_Id"] = vsDataContainer_Id_3										
                                            #Parametros
                                            if MO_filter_list("Ericsson", MO_3, MOs_Flag) is True:     												
                                                for child9 in child8:
                                                    for child10 in child9:
                                                        nr_child = 0 											
                                                        tag10 = str(child10.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                        if child10.text is None:
                                                            str10 = ""														
                                                        if child10.text is not None:
                                                            #print("MO_3:", MO_3)															
                                                            if re.search('[a-zA-Z0-9_]+', child10.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                str10 = ""
                                                            else:
                                                                str10= str(child10.text)														
                                                        attr_dict[tag10] = str10 if check_Key(attr_dict, tag10) is False else ("" if attr_dict[tag10] is None else attr_dict[tag10]+ ";")  + str10#corrigi a parte de concatenação											
                                                        for child11 in child10:
                                                            nr_child += 1 												
                                                            tag11 = str(child11.tag).replace('{EricssonSpecificAttributes.xsd}','')

                                                            if re.search('^[;]+$', attr_dict[tag10]):#aqui vou limpar os casos de str10 que vao agregndo ;;; quando o que interessa e o valor em no parametro da layer abaixo
                                                                #print("MO_3:" + MO_3 +";"+ str(child10.tag) +":"+ str10)														
                                                                attr_dict[tag10] =""														
                                                                #print(";;;;;")
                                                            if child11.text:																
                                                                attr_dict[tag10 + '_' + tag11] =  str(child11.text) if check_Key(attr_dict, tag10 + '_' + tag11) is False else attr_dict[tag10 + '_' + tag11] + ";" + str(child11.text)  
                                                            else:
                                                                #print("MO_3:" + MO_3 +";"+ str(child10.tag) +":"+ str10 + "\n" + str(child11.tag) +":"+ str(child11.text))
                                                                attr_dict[tag10 + '_' + tag11] =  "" if check_Key(attr_dict, tag10 + '_' + tag11) is False else attr_dict[tag10 + '_' + tag11] + ";" + ""		
     															
                                                            for child12 in child11:
                                                                print("NEW layer of attributes ADDED - Layer 3" + str(child12.tag) + ":" + str(child12.text))
                                                        if nr_child == 0 and tag10 not in attr_dict.keys():
                                                            attr_dict[tag10] = None
                                            if MO_filter_list("Ericsson", MO_3, MOs_Flag) is True:															
                                                items.append({**header_dict,**MEId_dict,**attr_dict})					
                                            attr_dict = {}												
                                        #MO level - 4 level below 								
                                        for child8 in child7.findall('./{genericNrm.xsd}VsDataContainer'):								
                                            vsDataContainer_Id_4 = str(dict(child8.attrib).get('id'))								
                                            for child9 in child8.findall('./{genericNrm.xsd}attributes'):									
                                                #MO
                                                for child10 in child9.findall('./{genericNrm.xsd}vsDataType'):
                                                    MO_4 = str(child10.text)[6:]									
                                                    attr_dict['vsDataType'] = MO_4
                                                    attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                    attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                    attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2
                                                    attr_dict["vs" + MO_3 + "_Id"] = vsDataContainer_Id_3
                                                    attr_dict["vs" + MO_4 + "_Id"] = vsDataContainer_Id_4										
                                                #Parametros
                                                if MO_filter_list("Ericsson", MO_4, MOs_Flag) is True:													
                                                    for child10 in child9:
                                                        for child11 in child10:
                                                            nr_child = 0 											
                                                            tag11 = str(child11.tag).replace('{EricssonSpecificAttributes.xsd}','')	
                                                            if child11.text is None:
                                                                str11 = ""															
                                                            if child11.text is not None:
                                                                #print("MO_4:", MO_4)																	
                                                                if re.search('[a-zA-Z0-9_]+', child11.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                    str11 = ""
                                                                else:
                                                                    str11= str(child11.text)
                                                            attr_dict[tag11] = str11 if check_Key(attr_dict, tag11) is False else ("" if attr_dict[tag11] is None else attr_dict[tag11]+ ";")  + str11#corrigi a parte de concatenação											
																									
                                                            for child12 in child11:
                                                                nr_child += 1 												
                                                                tag12 = str(child12.tag).replace('{EricssonSpecificAttributes.xsd}','')

                                                                if re.search('^[;]+$', attr_dict[tag11]):#aqui vou limpar os casos de str11 que vao agregndo ;;; quando o que interessa e o valor em no parametro da layer abaixo
                                                                    #print("MO_4:" + MO_4 +";"+ str(child11.tag) +":"+ str11)														
                                                                    attr_dict[tag11] =""														
                                                                    #print(";;;;;")
                                                                if child12.text:																
                                                                    attr_dict[tag11 + '_' + tag12] =  str(child12.text) if check_Key(attr_dict, tag11 + '_' + tag12) is False else attr_dict[tag11 + '_' + tag12] + ";" + str(child12.text)  
                                                                else:
                                                                    #print("MO_4:" + MO_4 +";"+ str(child11.tag) +":"+ str11 + "\n" + str(child12.tag) +":"+ str(child12.text))
                                                                    attr_dict[tag11 + '_' + tag12] =  "" if check_Key(attr_dict, tag11 + '_' + tag12) is False else attr_dict[tag11 + '_' + tag12] + ";" + ""		
      															
                                                                for child13 in child12:
                                                                    print("NEW layer of attributes ADDED - Layer 4" + str(child13.tag) + ":" + str(child13.text))
                                                            if nr_child == 0 and tag11 not in attr_dict.keys():
                                                                attr_dict[tag11] = None
                                                if MO_filter_list("Ericsson", MO_4, MOs_Flag) is True:																
                                                    items.append({**header_dict,**MEId_dict,**attr_dict})					
                                                attr_dict = {}
                                            #MO level - 5 level below 								
                                            for child9 in child8.findall('./{genericNrm.xsd}VsDataContainer'):								
                                                vsDataContainer_Id_5 = str(dict(child9.attrib).get('id'))								
                                                for child10 in child9.findall('./{genericNrm.xsd}attributes'):									
                                                    #MO
                                                    for child11 in child10.findall('./{genericNrm.xsd}vsDataType'):
                                                        MO_5 = str(child11.text)[6:]									
                                                        attr_dict['vsDataType'] = MO_5
                                                        attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                        attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                        attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2
                                                        attr_dict["vs" + MO_3 + "_Id"] = vsDataContainer_Id_3
                                                        attr_dict["vs" + MO_4 + "_Id"] = vsDataContainer_Id_4
                                                        attr_dict["vs" + MO_5 + "_Id"] = vsDataContainer_Id_5											
                                                    #Parametros
                                                    if MO_filter_list("Ericsson", MO_5, MOs_Flag) is True:															
                                                        for child11 in child10:
                                                            for child12 in child11:
                                                                tag12 = str(child12.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                                nr_child = 0
                                                                if child12.text is None:																	
                                                                    str12 = ""																
                                                                if child12.text is not None:																	
                                                                    #    print("MO_5:", MO_5)																	
                                                                    if re.search('[a-zA-Z0-9_]+', child12.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                        str12 = ""
                                                                    else:
                                                                        str12= str(child12.text)
                                                                attr_dict[tag12] = str12 if check_Key(attr_dict, tag12) is False else ("" if attr_dict[tag12] is None else attr_dict[tag12]+ ";")  + str12#corrigi a parte de concatenação											
				
                                                                for child13 in child12:
                                                                    nr_child += 1 												
                                                                    tag13 = str(child13.tag).replace('{EricssonSpecificAttributes.xsd}','')

                                                                    if re.search('^[;]+$', attr_dict[tag12]):#aqui vou limpar os casos de str12 que vao agregndo ;;; quando o que interessa e o valor em no parametro da layer abaixo
                                                                        #print("MO_5:" + MO_5 +";"+ str(child12.tag) +":"+ str12)														
                                                                        attr_dict[tag12] =""														
                                                                        #print(";;;;;")
                                                                    if child13.text:																
                                                                        attr_dict[tag12 + '_' + tag13] =  str(child13.text) if check_Key(attr_dict, tag12 + '_' + tag13) is False else attr_dict[tag12 + '_' + tag13] + ";" + str(child13.text)  
                                                                    else:
                                                                        #print("MO_5:" + MO_5 +";"+ str(child12.tag) +":"+ str12 + "\n" + str(child13.tag) +":"+ str(child13.text))
                                                                        attr_dict[tag12 + '_' + tag13] =  "" if check_Key(attr_dict, tag12 + '_' + tag13) is False else attr_dict[tag12 + '_' + tag13] + ";" + ""
																	                                 															
                                                                    for child14 in child13:
                                                                        print("NEW layer of attributes ADDED - Layer 5" + str(child14.tag) + ":" + str(child14.text))
                                                                if nr_child == 0 and tag12 not in attr_dict.keys():
                                                                    attr_dict[tag12] = None
                                                    if MO_filter_list("Ericsson", MO_5, MOs_Flag) is True:																	
                                                        items.append({**header_dict,**MEId_dict,**attr_dict})					
                                                    attr_dict = {}														
                                                #MO level - 6 level below 								
                                                for child10 in child9.findall('./{genericNrm.xsd}VsDataContainer'):								
                                                    vsDataContainer_Id_6 = str(dict(child10.attrib).get('id'))								
                                                    for child11 in child10.findall('./{genericNrm.xsd}attributes'):									
                                                        #MO
                                                        for child12 in child11.findall('./{genericNrm.xsd}vsDataType'):
                                                            MO_6 = str(child12.text)[6:]									
                                                            attr_dict['vsDataType'] = MO_6
                                                            attr_dict["vs" + MO_0 + "_Id"] = vsDataContainer_Id_0											
                                                            attr_dict["vs" + MO_1 + "_Id"] = vsDataContainer_Id_1
                                                            attr_dict["vs" + MO_2 + "_Id"] = vsDataContainer_Id_2
                                                            attr_dict["vs" + MO_3 + "_Id"] = vsDataContainer_Id_3
                                                            attr_dict["vs" + MO_4 + "_Id"] = vsDataContainer_Id_4
                                                            attr_dict["vs" + MO_5 + "_Id"] = vsDataContainer_Id_5	
                                                            attr_dict["vs" + MO_6 + "_Id"] = vsDataContainer_Id_6											
                                                        #Parametros
                                                        if MO_filter_list("Ericsson", MO_6, MOs_Flag) is True:															
                                                            for child12 in child11:
                                                                for child13 in child12:
                                                                    nr_child = 0 											
                                                                    tag13 = str(child13.tag).replace('{EricssonSpecificAttributes.xsd}','')
                                                                    if child13.text is None:																																	
                                                                        str13 = ""																	
                                                                    if child13.text is not None:																																	
                                                                        #print("MO_6:", MO_6)																	
                                                                        if re.search('[a-zA-Z0-9_]+', child13.text) is None: #o parser vai ter caracteres manhosos.Logo eu forço a copiar so os casos em que ha caracteres alfanumericos
                                                                            str13 = ""
                                                                        else:
                                                                            str13= str(child13.text)
                                                                    attr_dict[tag13] = str13 if check_Key(attr_dict, tag13) is False else ("" if attr_dict[tag13] is None else attr_dict[tag13]+ ";")  + str13#corrigi a parte de concatenação											
													
                                                                    for child14 in child13:
                                                                        nr_child += 1 												
                                                                        tag14 = str(child14.tag).replace('{EricssonSpecificAttributes.xsd}','')

                                                                        if re.search('^[;]+$', attr_dict[tag13]):#aqui vou limpar os casos de str13 que vao agregndo ;;; quando o que interessa e o valor em no parametro da layer abaixo
                                                                            #print("MO_6:" + MO_6 +";"+ str(child13.tag) +":"+ str13)														
                                                                            attr_dict[tag13] =""														
                                                                            #print(";;;;;")
                                                                        if child14.text:																
                                                                            attr_dict[tag13 + '_' + tag14] =  str(child14.text) if check_Key(attr_dict, tag13 + '_' + tag14) is False else attr_dict[tag13 + '_' + tag14] + ";" + str(child14.text)  
                                                                        else:
                                                                            #print("MO_6:" + MO_6 +";"+ str(child13.tag) +":"+ str13 + "\n" + str(child14.tag) +":"+ str(child14.text))
                                                                            attr_dict[tag13 + '_' + tag14] =  "" if check_Key(attr_dict, tag13 + '_' + tag14) is False else attr_dict[tag13 + '_' + tag14] + ";" + ""

                                                                        for child15 in child14:
                                                                            print("NEW layer of attributes ADDED - Layer 6" + str(child15.tag) + ":" + str(child15.text))
                                                                    if nr_child == 0 and tag13 not in attr_dict.keys():
                                                                        attr_dict[tag13] = None
                                                        if MO_filter_list("Ericsson", MO_6, MOs_Flag) is True:																		
                                                            items.append({**header_dict,**MEId_dict,**attr_dict})					
                                                        attr_dict = {}	


															
    return items;

#####################################Nokia 4G Parser########################################

def Nokia_Collector(file, MOs_Flag):
    items = []
    attr_dict = {}
    tree = ET.parse(file.open(encoding='utf-8'))
    root = tree.getroot()
    if re.match("^\{raml[0-9]{2}\.xsd\}raml", root.tag):
    #if(root.tag == "{raml20.xsd}raml"):
        raml_version = str(root.tag)[:-4]
        #começar abaixo do root	
        for child0 in root.findall('./' + raml_version + 'cmData'):
            for child1 in child0.findall("./" + raml_version + "header"):
                FileName = str(file)
                DateTime = str(dict(child1.attrib).get('dateTime'))				
            for child1 in child0.findall("./"+ raml_version + "managedObject"):
                distName = str(dict(child1.attrib).get('distName'))			
                MO = str(dict(child1.attrib).get('class'))
                if MO.find(":") > -1:
                    pos_start = MO.find(":")
                    pos_end = len(MO)                    
                    MO = MO[pos_start + 1 : pos_end]
                if distName[0:14] == "PLMN-PLMN/BSC-":					
                    MO = "BSC_" + MO
                elif distName[0:14] == "PLMN-PLMN/RNC-":					
                    MO = "RNC_" + MO				
                else:
                    MO = MO	 						
                if MO_filter_list("Nokia", MO, MOs_Flag) is True:			
                    attr_dict['FileName'] = FileName
                    attr_dict['dateTime'] = DateTime				
                    attr_dict['class'] = MO
                    attr_dict['version'] = str(dict(child1.attrib).get('version'))
                    attr_dict['distName'] = distName								
                    attr_dict['id'] = str(dict(child1.attrib).get('id'))
                    for child2 in child1.findall("./" + raml_version + "p"):		
                        Par   = str(dict(child2.attrib).get('name'))
                        Value = str(child2.text)					
                        attr_dict[Par] = Value
                    for child2 in child1.findall("./" + raml_version + "list"):				
                        list_Name = str(dict(child2.attrib).get('name'))				
                        for child3 in child2.findall("./" + raml_version + "p"):
                            attr_dict[list_Name] =  str(child3.text) if check_Key(attr_dict, list_Name) is False else attr_dict[list_Name] + ";" + str(child3.text)						
                            for child4 in child3:							
                                print("NEW layer of attributes ADDED (4) not supported" + str(dict(child4.attrib).get('name')) + ":" + str(child4.text))						
                        for child3 in child2.findall("./" + raml_version + "item"):						
                            item_elements = 0
                            for child4 in child3:
                                Field = str(dict(child4.attrib).get('name')) if isinstance(str(dict(child4.attrib).get('name')), str) is True else ""
                                Field_value = str(child4.text) if isinstance(str(child4.text), str) is True else ""
                                Item_Field = Field + "=" + Field_value
                                if check_Key(attr_dict, list_Name) is True:
                                    if item_elements == 0:
                                        attr_dict[list_Name] = attr_dict[list_Name] + "; ITEM:" + Item_Field
                                    else:									
                                        attr_dict[list_Name] = Item_Field if check_Key(attr_dict, list_Name) is False else attr_dict[list_Name] + ";" + Item_Field
                                else:
                                    attr_dict[list_Name] = "ITEM:" + Item_Field
                                item_elements = item_elements + 1							
                                for child5 in child4:							
                                    print("NEW layer of attributes ADDED(5) not supportes" + str(dict(child5.attrib).get('name')) + ":" + str(child5.text))
                    #print(attr_dict['distName'][0:14])
                    #if attr_dict['distName'][0:14]!="PLMN-PLMN/RNC-":
                    items.append({**attr_dict})								
                    attr_dict = {}
    return items;	

##################check if a key is already part of a dictionary################

def check_Key(dict, key):    
    if key in dict.keys(): 
        return True
    else: 
        return False	
		
###############################check MO structure###############################

def check_MOs_structures(MO_struct, vendor, items):	

    #choose the right field name
    if vendor == "Nokia":
	    Field = "class"
    elif vendor == "Ericsson":		
	    Field = "vsDataType"		
    elif vendor == "Huawei":		
	    Field = "MO"	
				
    #MO_struct = {}
    for i in items: 
        if i[Field] not in MO_struct.keys():			
            MO_struct[i[Field]] = list(i.keys())
        if i[Field] in MO_struct.keys():
            for key in i.keys():
                MO_struct[i[Field]].append(key) if key not in MO_struct[i[Field]] else MO_struct[i[Field]]
    #print(MO_struct)				
    return(MO_struct)
	
###########################All MOs or just part##################################	

def MO_filter_list(vendor, MO, MOs_Flag):
    if MOs_Flag is True:
	    return True
    #lista caso seja Nokia		
    if vendor == 'Nokia':
        class_values = ['MRBTS', 'LNBTS', 'LNBTS_FDD', 'LNBTS_TDD','LNCEL', 'LNCEL_FDD', 'LNCEL_TDD', 'MTRACE', 'CHANNEL', 'CHANNELGROUP', 'CREL', 'RETU', 'RETU_R', 'ANTL', 'ANTL_R', 'RMOD', 'RMOD_R', 'CTRLTS', 'LCELL', 'LNA', 'CELLMAPPING', 'NRBTS', 'LTEENB', 'NRCELL', 'NRCELLGRP', 'TCE', 'NRMTRACECU', 'NRMTRACEDU', 'NRCELL_FDD', 'TRACKINGAREA', 'RNC_RNC', 'RNC_WBTS', 'RNC_WCEL']	
    #lista caso seja E//
    elif vendor == "Ericsson":		
        class_values = ['EUtranCellFDD', 'PmUeMeasControl', 'ReportConfigEUtraIntraFreqPm', 'FeatureState', 'OptionalFeatureLicense', 'ManagedElement', 'ENodeBFunction', 'PmEventService', 'GNBDUFunction', 'NRCellDU', 'NRCellCU', 'NRSectorCarrier', 'UtranCell', 'LocationArea', 'RoutingArea', 'ServiceArea', 'Ura', 'RncFunction', 'IubLink']		
    #lista caso seja Huawei
    elif vendor == "Huawei":		
        class_values = ['SRAN_ENODEBFUNCTION', 'SRAN_CELL', 'SRAN_CELLOP', 'SRAN_CNOPERATOR', 'SRAN_CNOPERATORTA', 'UMTS_RNC_UCELL', 'UMTS_RNC_URNCBASIC', 'UMTS_RNC_UCNOPERGROUP', 'UMTS_RNC_UCNOPERATOR']    
				
    return True if MO in class_values else False

############################################################################		
#copiar cada MO para os CSVs, BDs...
def MOs_to_CSV_SQL(vendor, items, output_classes, input):
	
    if vendor == "Nokia":	
        Field = 'class'
    if vendor == "Ericsson":	
        Field = 'vsDataType'
    if vendor == "Huawei":	
        Field = 'MO'		

	#create a path for vendor where to put CSVs
    os.makedirs(input['ProjName'] + '/'+ vendor + '/')

    if input["DB_Postgres"] is True:
        user = 'postgres'
        password = 'postgres'
        port = "5432"
        databaseName = vendor#'postgres'#input["ProjName"]	 
        postgres = sqlalchemy.create_engine('postgresql://' + user + ':' + password + '@localhost:' + port + '/' + databaseName)
		
    if input["DB_SQLServer"] is True: 
        # parameters
        DB = {'servername': 'localhost\SQLEXPRESS', 'database': vendor, 'driver': 'driver=SQL+Server+Native+Client+11.0'}
        conn = pyodbc.connect("driver={SQL Server};server=localhost\SQLEXPRESS; database=master; trusted_connection=true", autocommit=True)
        mycursor = conn.cursor()
        mycursor.execute("If(db_id(N'" + DB['database'] + "') IS NOT NULL) DROP DATABASE " + DB['database'])		
        mycursor.execute("If(db_id(N'" + DB['database'] + "') IS NULL) CREATE DATABASE " + DB['database'])
        mycursor.close()
        engine_sqlserver = sqlalchemy.create_engine('mssql+pyodbc://' + DB['servername'] + '/' + DB['database'] + '?trusted_connection=yes&' + DB['driver'])  

    print("Managed Objects parsed from:", vendor)
    for j in output_classes:
   	    #criar os ficheiros CSV
        df = pd.DataFrame([x for x in items if x[Field] == j], columns=output_classes[j]).drop(columns = [Field])
        print(j)
        #print(output_classes[j])		
        df.to_csv(input['ProjName'] + '/'+ vendor + '/' + "%s.csv" %j, header = True, index =False)

        #change column name due to not case sensitive in SQL
        column_capitalized = [column.capitalize() for column in df.columns]
        column_not_capitalized = [column for column in df.columns]			
        for key in df.columns:
            if column_capitalized.count(key.capitalize()) >1:
                rep_key_ins_case = []
                for column in column_not_capitalized:
                    if column.casefold() == key.casefold():		    			
                        rep_key_ins_case.append(column)
                rep_key_ins_case.sort()				
                #print("rep_key_ins_case", rep_key_ins_case)
                if rep_key_ins_case.index(key) != 0:		
                    df.rename(columns={key:(key + '_v'+ str(rep_key_ins_case.index(key)))}, inplace=True)
				
       	#criar DBs-SQLite com os MOs
        if input["ProjName"] is not None and input["DB_SQLite"] is True:		
            conn = sql.connect(input["ProjName"] + vendor +'.db' )
            df.to_sql(j, conn, if_exists='append', index = False)			
        #create DB_PostGres			
        if input["DB_Postgres"] is True:
            conn_postgres = postgres.connect()
            df.to_sql(j, conn_postgres, if_exists='append', index = False)
        #create DB_SQLServer		
        if input["DB_SQLServer"] is True: 	
            conn_sqlserver= engine_sqlserver.connect()
            df.to_sql(j, conn_sqlserver, if_exists='append', index = False)				
						
    if input["DB_Postgres"] is True: 
        postgres.close()
		
    if input["DB_SQLServer"] is True: 
        conn_sqlserver.close()    
		
############################################################################################
#####################################REPORTING PART#########################################
############################################################################################

#######################################Nokia extract MOs####################################
	
def extract_df_Nokia_MOs(MOs_struct, items, output_base, dir_CSVs, report_option):
    #initialize:
    LNBTS = pd.DataFrame()
    CTRLTS = pd.DataFrame()
    MTRACE = pd.DataFrame()
    CTRLTS = pd.DataFrame()
    MRBTS = pd.DataFrame()	
    LNCEL = pd.DataFrame()
    LNCEL_FDD = pd.DataFrame()
    LNCEL_TDD = pd.DataFrame()
    ANT_R = pd.DataFrame()	
    ANTL_R = pd.DataFrame()
    CHANNEL = pd.DataFrame()
    NRBTS = pd.DataFrame()	
    LTEENB = pd.DataFrame() 
    NRCELL = pd.DataFrame()
    NRCELL_FDD = pd.DataFrame()	
    NRCELLGRP = pd.DataFrame()	
    LTEENB = pd.DataFrame() 
    NRMTRACECU = pd.DataFrame() 
    NRMTRACEDU = pd.DataFrame() 
    TCE = pd.DataFrame() 
    TRACKINGAREA = pd.DataFrame()
    RNC = pd.DataFrame()	
    WBTS = pd.DataFrame()
    WCEL = pd.DataFrame()	
	
    for j in MOs_struct:
        #filtrar os campos que interessam para os reports
        if j == "LNBTS":
            LNBTS = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'actCellTrace', 'actMDTCellTrace', 'actUeThroughputMeas', 'enbName', 'supportedCellTechnology', 'operationalState', 'reqFreqBands', 'actSecRatRep', 'actEvtSecRatRep', 'actLteNrDualConnectivity', 'actDynTrigLteNrDualConnectivity', 'actDl256QamChQualEst']).fillna('').astype('str')
            LNBTS['distName'] = LNBTS.distName.str.replace('PLMN-PLMN/',"")
            LNBTS[['MRBTS', 'LNBTS']] = LNBTS.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
            LNBTS = LNBTS.rename(columns = {'distName':'LNBTS_distName', 'operationalState':'LNBTS_operationalState', 'actDl256QamChQualEst':'LNBTS_actDl256QamChQualEst'})						
        if j == "CTRLTS":
            if report_option in (1, 3):
                CTRLTS = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'cellTraceRepMode', 'ueTraceRepMode', 'taTracing', 'traceRepMode', 'vendorSpecTracing']).fillna('').astype('str')
                CTRLTS['distName'] = CTRLTS.distName.str.replace('PLMN-PLMN/',"")				
                CTRLTS[['MRBTS', 'LNBTS', 'CTRLTS']] = CTRLTS.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                CTRLTS = CTRLTS.rename(columns = {"distName":"CTRLTS_distName"})
        if j == "MTRACE":
            if report_option in (1, 3):
                MTRACE = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'jobType', 'tceIpAddress', 'cellMaxActiveUEsTraced', 'traceRrcSetting', 'traceS1Setting', 'traceX2Setting', 'actUeThroughputMR', 'reportAmount', 'reportInterval', 'cellTaTracing', 'immedMDTForceUEConsent', 'immedMDTObtainLocation', 'immedMDTPosMethod', 'tATracingSynch', 'immedMDTSelectOnlyGNSSUes', 'cellMaxActiveMDTUEsTraced', 'immedMDTAnonymization', 'periodicUeMeas', 'cellMacTracing', 'immedMDTControl', 'enhCellVendorSpecTracing', 'cellVendorSpecTracing', 'tracerrcmsgcategory', 'tracex2msgcategory', 'traceS1MsgCategory', 'interfaceSelection']).fillna('').astype('str')
                MTRACE['distName'] = MTRACE.distName.str.replace('PLMN-PLMN/',"")
                MTRACE[['MRBTS', 'LNBTS', 'CTRLTS', 'MTRACE']] = MTRACE.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                MTRACE = MTRACE.rename(columns = {"distName":"MTRACE_distName"})			    
        if j == "MRBTS":
            if report_option in (2, 3):
                MRBTS = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'btsName', 'name', 'latitude', 'longitude']).fillna('').astype('str')
                MRBTS['distName'] = MRBTS.distName.str.replace('PLMN-PLMN/',"")                
                MRBTS[['MRBTS']] = MRBTS.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                MRBTS = MRBTS.rename(columns = {'distName':'MRBTS_distName', 'name':'MRBTS_name'})							
        if j == "LNCEL":
            if report_option in (2, 3):
                LNCEL = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'cellname', 'eutraCelId', 'administrativeState', 'operationalState', 'cellTechnology', 'phyCellId', 'expectedCellSize', 'pMax', 'actModulationSchemeDl', 'dl256QamDeactChQualThr', 'dl256QamReactChQualThr', 'perDl256QamChQualEst', 'dlCellPwrRed', 'iniDl256QamChQualEst', 'perDl256QamChQualEst', 'actDl256QamChQualEst', 'dl256QamDeactChQualThr', 'dl256QamReactChQualThr']).fillna('').astype('str')
                LNCEL['distName'] = LNCEL.distName.str.replace('PLMN-PLMN/',"") 
                LNCEL[['MRBTS', 'LNBTS', 'LNCEL']] = LNCEL.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                LNCEL = LNCEL.rename(columns = {'distName':'LNCEL_distName', 'operationalState':'LNCEL_operationalState'})				
        if j == "LNCEL_FDD":
            if report_option in (2, 3):
                LNCEL_FDD = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'dlChBw', 'dlMimoMode', 'dlRsBoost', 'earfcnDL', 'earfcnUL', 'rootSeqIndex', 'ulChBw']).fillna('').astype('str')
                LNCEL_FDD['distName'] = LNCEL_FDD.distName.str.replace('PLMN-PLMN/',"")				
                LNCEL_FDD[['MRBTS', 'LNBTS', 'LNCEL', 'LNCELL_FDD']] = LNCEL_FDD.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z_]*-',""))
                LNCEL_FDD = LNCEL_FDD.rename(columns = {'distName':'LNCEL_FDD_distName'})			
        if j == "LNCEL_TDD":
            if report_option in (2, 3):
                LNCEL_TDD = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'ChBw', 'dlMimoMode', 'dlRsBoost', 'earfcn', 'rootSeqIndex']).fillna('').astype('str')
                LNCEL_TDD['distName'] = LNCEL_TDD.distName.str.replace('PLMN-PLMN/',"")				
                LNCEL_TDD[['MRBTS', 'LNBTS', 'LNCEL', 'LNCELL_TDD']] = LNCEL_TDD.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z_]*-',""))
                LNCEL_TDD = LNCEL_TDD.rename(columns = {'distName':'LNCEL_TDD_distName'})				
        if j == "RETU":
            if report_option in (2, 3):
                RETU = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'maxAngle', 'minAngle','mechanicalAngle', 'sectorID', 'antBandList', 'antlDNList', 'baseStationID']).fillna('').astype('str')
                RETU['distName'] = RETU.distName.str.replace('PLMN-PLMN/',"")					
                RETU[['MRBTS', 'EQM', 'APEQM', 'ALD', 'RETU']] = RETU.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                RETU = RETU.rename(columns = {'distName':'RETU_distName'}) 
                RETU = RETU.drop(columns=['EQM', 'APEQM', 'ALD'])
        if j == "RETU_R":
            if report_option in (2, 3):
                RETU_R = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'antlDNList', 'configDN', 'angle', 'antModel', 'sectorID', 'baseStationID', 'maxAngle', 'sectorID', 'subunitNumber']).fillna('').astype('str')
                RETU_R['distName'] = RETU_R.distName.str.replace('PLMN-PLMN/',"")					
                RETU_R[['MRBTS', 'EQM_R', 'APEQM_R', 'ALD_R', 'RETU_R']] = RETU_R.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                RETU_R = RETU_R.rename(columns = {'distName':'RETU_R_distName'}) 
                RETU_R = RETU_R.drop(columns=['EQM_R', 'APEQM_R', 'ALD_R'])				
        if j == "ANT":
            if report_option in (2, 3):
                ANT = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'AntennaPathDelayUL', 'AntennaPathDelayDL']).fillna('').astype('str')
                ANT['distName'] = ANT.distName.str.replace('PLMN-PLMN/',"")				
                ANT[['MRBTS', 'EQM', 'APEQM', 'ALD', 'RETU']] = ANT_R.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                ANT = ANT.rename(columns = {'distName':'ANT_R_distName'}) 
                ANT = ANT.drop(columns=['EQM', 'APEQM', 'ALD'])				
        if j == "ANT_R":
            if report_option in (2, 3):
                ANT_R = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'AntennaPathDelayUL', 'AntennaPathDelayDL']).fillna('').astype('str')
                ANT_R['distName'] = ANT_R.distName.str.replace('PLMN-PLMN/',"")					
                ANT_R[['MRBTS', 'EQM_R', 'APEQM_R', 'ALD_R', 'RETU_R']] = ANT_R.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                ANT_R = ANT_R.rename(columns = {'distName':'ANT_R_distName'}) 
                ANT_R = ANT_R.drop(columns=['EQM', 'APEQM', 'ALD'])
        if j == "CHANNEL":
            if report_option in (2, 3):
                CHANNEL = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'antlDN']).fillna('').astype('str')
                CHANNEL['distName'] = CHANNEL.distName.str.replace('PLMN-PLMN/',"")					
                CHANNEL[['MRBTS', 'MNL', 'MNLENT', 'CELLMAPPING','LCELL', 'CHANNELGROUP', 'CHANNEL']] = CHANNEL.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                CHANNEL = CHANNEL.rename(columns = {'distName':'CHANNEL_distName'}) 
                CHANNEL = CHANNEL.drop(columns=['MNL', 'MNLENT'])
        ##5G##
        if j == "NRBTS":		
            NRBTS = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'actCellTraceReport', 'actPCMDReport', 'mcc', 'mnc', 'operationalState', 'name', 'gNbCuType', 'actCustomBSR']).fillna('').astype('str')
            NRBTS['distName'] = NRBTS.distName.str.replace('PLMN-PLMN/',"")
            NRBTS[['MRBTS', 'NRBTS']] = NRBTS.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
            NRBTS = NRBTS.rename(columns = {'distName':'NRBTS_distName', 'operationalState':'NRBTS_operationalState', 'name': 'NRBTS_name', 'mcc': 'NRBTS_mcc', 'mnc': 'NRBTS_mnc'})			
        if j == "LTEENB":
            if report_option in (2, 3):		
                LTEENB = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'eNodeBId','enbPlmn']).fillna('').astype('str')
                LTEENB['distName'] = LTEENB.distName.str.replace('PLMN-PLMN/',"")
                LTEENB[['MRBTS', 'NRBTS', 'LTEENB']] = LTEENB.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                LTEENB = LTEENB.rename(columns = {'distName':'LTEENB_distName'})		    
        if j == "NRCELL":
            if report_option in (2, 3):
                NRCELL = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'plmnList','cellname', 'lcrId', 'administrativeState', 'operationalState', 'cellBarred', 'cellDepType', 'cellTechnology', 'physCellId', 'expectedCellSize', 'nrCellIdentity', 'nrCellType', 'nrarfcn', 'freqBandIndicatorNR', 'chBw', 'gscn', 'ssbScs','tddFrameStructure', 'actDlMuMimo', 'dlMimoMode','actBeamforming', 'beamSet', 'actPdschAtSsbSlots', 'arfcnSsbPbch', 'configuredEpsTac', 'trackingAreaDN']).fillna('').astype('str')
                NRCELL['distName'] = NRCELL.distName.str.replace('PLMN-PLMN/',"") 
                NRCELL[['MRBTS', 'NRBTS', 'NRCELL']] = NRCELL.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                NRCELL = NRCELL.rename(columns = {'distName':'NRCELL_distName', 'operationalState':'NRCELL_operationalState'})
        if j == "NRCELL_FDD":
            if report_option in (2, 3):
                NRCELL_FDD = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'chBwDl', 'chBwUl', 'nrarfcnDl', 'nrarfcnUl']).fillna('').astype('str')
                NRCELL_FDD['distName'] = NRCELL_FDD.distName.str.replace('PLMN-PLMN/',"") 
                NRCELL_FDD[['MRBTS', 'NRBTS', 'NRCELL', 'NRCELL_FDD']] = NRCELL_FDD.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z_]*-',""))
                NRCELL_FDD = NRCELL_FDD.rename(columns = {'distName':'NRCELL_FDD_distName'})			
        if j == "NRCELLGRP":
            if report_option in (2, 3):
                NRCELLGRP = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'nrCellList', 'scs', 'numberOfTransmittedSsBlocks', 'ssBurstSetPeriod']).fillna('').astype('str')
                NRCELLGRP['distName'] = NRCELLGRP.distName.str.replace('PLMN-PLMN/',"") 
                NRCELLGRP[['MRBTS', 'NRBTS', 'NRCELLGRP']] = NRCELLGRP.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                NRCELLGRP = NRCELLGRP.rename(columns = {'distName':'NRCELLGRP_distName'})
                LTEENB = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'eNodeBId','enbPlmn']).fillna('').astype('str')
                LTEENB['distName'] = LTEENB.distName.str.replace('PLMN-PLMN/',"")
                LTEENB[['MRBTS', 'NRBTS', 'LTEENB']] = LTEENB.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                LTEENB = LTEENB.rename(columns = {'distName':'LTEENB_distName'})
        if j == "TRACKINGAREA":
            if report_option in (2, 3):
                TRACKINGAREA = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'fiveGsTac']).fillna('').astype('str')
                TRACKINGAREA['distName'] = TRACKINGAREA.distName.str.replace('PLMN-PLMN/',"") 
                TRACKINGAREA[['MRBTS', 'NRBTS', 'TRACKINGAREA']] = TRACKINGAREA.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                TRACKINGAREA = TRACKINGAREA.rename(columns = {'distName':'TRACKINGAREA_distName'})		    	
        if j == "LTEENB":
            if report_option in (2, 3):
                LTEENB = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'eNodeBId']).fillna('').astype('str')
                LTEENB['distName'] = LTEENB.distName.str.replace('PLMN-PLMN/',"") 
                LTEENB[['MRBTS', 'NRBTS', 'LTEENB']] = LTEENB.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                LTEENB = LTEENB.rename(columns = {'distName':'LTEENB_distName'})
        if j == "NRMTRACECU":
            if report_option in (1, 3):
                NRMTRACECU = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'actCellTraceReportingCU', 'traceNGAPSetting', 'traceXNSetting', 'traceNRRRCSetting', 'scope', 'jobType', 'cellMaxActiveUEsTraced', 'nrRanTraceReference', 'actPCMDReporting']).fillna('').astype('str')
                NRMTRACECU['distName'] = NRMTRACECU.distName.str.replace('PLMN-PLMN/',"") 
                NRMTRACECU[['MRBTS', 'NRBTS', 'NRCTRLTS', 'NRMTRACECU']] = NRMTRACECU.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                NRMTRACECU = NRMTRACECU.rename(columns = {'distName':'NRMTRACECU_distName', 'scope':'NRMTRACECU_scope', 'MRBTS':'NRMTRACECU_MRBTS', 'NRBTS':'NRMTRACECU_NRBTS', 'actPCMDReporting': 'NRMTRACECU_actPCMDReporting', 'nrRanTraceReference': 'NRMTRACECU_nrRanTraceReference'})				
        if j == "NRMTRACEDU":
            if report_option in (1, 3):
                NRMTRACEDU = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'scope', 'nrRanTraceReference', 'actPCMDReporting' ]).fillna('').astype('str')
                NRMTRACEDU['distName'] = NRMTRACEDU.distName.str.replace('PLMN-PLMN/',"") 
                NRMTRACEDU[['MRBTS', 'NRBTS', 'NRDU', 'NRCTRLTSDU', 'NRMTRACEDU']] = NRMTRACEDU.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                NRMTRACEDU = NRMTRACEDU.rename(columns = {'distName':'NRMTRACEDU_distName', 'scope':'NRMTRACEDU_scope', 'MRBTS':'NRMTRACEDU_MRBTS', 'NRBTS':'NRMTRACEDU_NRBTS', 'actPCMDReporting': 'NRMTRACEDU_actPCMDReporting', 'nrRanTraceReference': 'NRMTRACEDU_nrRanTraceReference'})
        if j == "TCE":
            if report_option in (1, 3):
                TCE = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'tceIpAddress', 'tcePortNumber' ]).fillna('').astype('str')
                TCE['distName'] = TCE.distName.str.replace('PLMN-PLMN/',"") 
                TCE[[ 'MRBTS', 'MNL', 'TCEADM', 'TCE']] = TCE.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                TCE = TCE.rename(columns = {'distName':'TCE_distName', 'MRBTS':'TCE_MRBTS'})
        ##3G
        if j == "RNC_RNC":
            if report_option in (2, 3):		
                RNC = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'name', 'ActivePRNC', 'RNCName', 'CommonMCC', 'CommonMNC']).fillna('').astype('str')
                RNC['distName'] = RNC.distName.str.replace('PLMN-PLMN/',"")
                RNC[['RNC']] = RNC.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                RNC = RNC.rename(columns = {'distName':'RNC_distName', 'name':'RNC_name'})	
        if j == "RNC_WBTS":
            if report_option in (2, 3):		
                WBTS = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'name', 'type', 'IPNBId', 'NEType', 'WBTSName', 'linkedMrsiteDN', 'SBTSId']).fillna('').astype('str')
                WBTS['distName'] = WBTS.distName.str.replace('PLMN-PLMN/',"")
                WBTS[['RNC', 'WBTS']] = WBTS.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                WBTS = WBTS.rename(columns = {'distName':'WBTS_distName', 'name':'WBTS_name'})	
        if j == "RNC_WCEL":
            if report_option in (2, 3):		
                WCEL = pd.DataFrame([x for x in items if x['class'] == j], columns=['distName', 'WCELMCC', 'WCELMNC', 'name', 'AdminCellState', 'CId', 'CellBarred', 'CellRange', 'Cell_Reserved', 'LAC', 'RAC', 'SAC', 'URAId', 'PriScrCode', 'SectorID', 'angle', 'UARFCN', 'WCelState', 'HSDPAenabled', 'HSDSCHOpState', 'HSUPAEnabled', 'EDCHOpState']).fillna('').astype('str')
                WCEL['distName'] = WCEL.distName.str.replace('PLMN-PLMN/',"")
                WCEL[['RNC', 'WBTS', 'WCEL']] = WCEL.distName.str.split("/", expand = True).apply(lambda x: x.str.replace(r'[A-Z]*-',""))
                WCEL = WCEL.rename(columns = {'distName':'WCEL_distName', 'name':'WCEL_name'})					

    items = []
    return(LNBTS, CTRLTS, MTRACE, MRBTS, LNCEL, LNCEL_FDD, LNCEL_TDD, NRBTS, LTEENB, NRCELL, NRCELLGRP, LTEENB, NRMTRACECU, NRMTRACEDU, TCE, NRCELL_FDD, TRACKINGAREA, RNC, WBTS, WCEL)
	
   
###################################Check the list of LNBTS on AOI##############################

def LNBTS_AoI(cell_list):
    AoI_LNBTS = cell_list["eUtranCellId"].astype(int)//256
    AoI_LNBTS = AoI_LNBTS.unique().astype(str)
    AoI_LNBTS = pd.DataFrame(AoI_LNBTS, columns = ['AoI_MRBTS'])	
    return(AoI_LNBTS)

###############################Nokia Trace Configuration Analysis##############################
	   
def Nokia_Traces_Configurations(output_base_dir, LNBTS, CTRLTS, MTRACE, NRBTS, NRMTRACECU, NRMTRACEDU, TCE, AoI_cell_list, MDT, MRs, Thr, PCMD, ENDC, NSETAP):
    
    #check 4G Traces	
    if LNBTS.empty:
        print("There's no LNBTS => Probably is not a 4G XML!!!")
    else:
        print("There's LNBTS => Probably this is a 4G XML!!!")	
        AoI_LNBTS, LNBTS, LNBTS_w_All_Features, LNBTS_without_MTRACES, LNBTS_w_Trc_active, LNBTS_without_Trc_active, LNBTS_w_TAs, LNBTS_w_MRs, LNBTS_w_Thr, LNBTS_w_MDT, LNBTS_without_All_Features, missing_AoI_LNBTS, LNBTS_without_TAs, LNBTS_w_Trc_active_without_TAs, LNBTS_without_MRs, LNBTS_w_Trc_active_w_TAs_without_MRs, LNBTS_without_Thr, LNBTS_w_Trc_active_w_TAs_MRs_without_Thr, LNBTS_without_MDT, LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT, LNBTS_w_ENDC, LNBTS_without_ENDC, LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC, LNBTS_w_NSETAP, LNBTS_without_NSETAP, LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP = Nokia_4G_Traces_Configurations(output_base_dir, LNBTS, CTRLTS, MTRACE, AoI_cell_list, MDT, MRs, Thr, ENDC, NSETAP)
        Output_Nokia_4G_Trace_reports(output_base_dir, ENDC, NSETAP, AoI_LNBTS, LNBTS, LNBTS_w_All_Features, LNBTS_without_MTRACES, LNBTS_w_Trc_active, LNBTS_without_Trc_active, LNBTS_w_TAs, LNBTS_w_MRs, LNBTS_w_Thr, LNBTS_w_MDT, LNBTS_without_All_Features, Thr, MDT, MRs, missing_AoI_LNBTS, LNBTS_without_TAs, LNBTS_w_Trc_active_without_TAs, LNBTS_without_MRs, LNBTS_w_Trc_active_w_TAs_without_MRs, LNBTS_without_Thr, LNBTS_w_Trc_active_w_TAs_MRs_without_Thr, LNBTS_without_MDT, LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT, LNBTS_w_ENDC, LNBTS_without_ENDC, LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC, LNBTS_w_NSETAP, LNBTS_without_NSETAP, LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP)
		
    #check 5G Traces	
    if NRBTS.empty:
        print("There's no NRBTS => Probably is not a 5G XML!!!")
    else:
        print("There's NRBTS => Probably is a 5G XML!!!")	
        AoI_NRBTS, NRBTS, missing_AoI_NRBTS, NRBTS_without_NRCTRLTSs, NRBTS_w_Trc_active, NRBTS_without_Trc_active, NRBTS_w_PCMD, NRBTS_without_PCMD, NRBTS_w_Trc_active_without_PCMD, NRBTS_w_All_Features, NRBTS_without_All_Features = Nokia_5G_Traces_Configurations(output_base_dir, NRBTS, NRMTRACECU, NRMTRACEDU, TCE, AoI_cell_list, PCMD)
        Output_Nokia_5G_Trace_reports(output_base_dir, PCMD, AoI_NRBTS, NRBTS, missing_AoI_NRBTS, NRBTS_without_NRCTRLTSs, NRBTS_w_Trc_active, NRBTS_without_Trc_active, NRBTS_w_PCMD, NRBTS_without_PCMD, NRBTS_w_Trc_active_without_PCMD, NRBTS_w_All_Features, NRBTS_without_All_Features)		
	
###############################Nokia 4G Trace Configuration Analysis##############################

def Nokia_4G_Traces_Configurations(output_base_dir, LNBTS, CTRLTS, MTRACE, AoI_cell_list, MDT, MRs, Thr, ENDC, NSETAP):
    #Merge 4G MO dataframes for Trace Configuration
    try:
        Traces_per_LNBTS = LNBTS.merge(CTRLTS, on=['MRBTS', 'LNBTS'], how = 'left').merge(MTRACE, on=['MRBTS', 'LNBTS', 'CTRLTS'], how = 'left')
        Traces_per_LNBTS['Nr_of_CTRLTSs']= Traces_per_LNBTS.groupby(["MRBTS", "LNBTS"])["CTRLTS"].transform('nunique')
        Traces_per_LNBTS['Nr_of_MTRACEs']= Traces_per_LNBTS.groupby(["MRBTS", "LNBTS"])["MTRACE"].transform('nunique')
    except:
        try:
            Traces_per_LNBTS = LNBTS.merge(MTRACE, on=['MRBTS', 'LNBTS'], how = 'left')
            Traces_per_LNBTS['Nr_of_MTRACEs']= Traces_per_LNBTS.groupby(["MRBTS", "LNBTS"])["MTRACE"].transform('nunique')
            print("The CTRLTS is missing in the XML file")
            quit()
        except:
            print("At least 2 of the important MOs are missing in the XML file: CTRLTS, MTRACE, LNBTS")
            quit() 	

    #AoI_LNBTS
    if not AoI_cell_list.empty:	
        AoI_LNBTS = LNBTS_AoI(AoI_cell_list)
    else:
        AoI_LNBTS = pd.DataFrame()		
    #updatar lista de Traces em função de AoI
    if not AoI_LNBTS.empty:
        Traces_per_LNBTS = Traces_per_LNBTS.merge(AoI_LNBTS, left_on=['MRBTS'], right_on=['AoI_MRBTS'], how = 'inner')

    #list of eNodeBs collected
    LNBTS = Traces_per_LNBTS['LNBTS_distName'].unique()
    LNBTS = pd.DataFrame(LNBTS, columns = ['LNBTS_collected'])  	

    #list of eNBs not collected when compared with AoI_LNBTS
    if not AoI_LNBTS.empty:
        missing_AoI_LNBTS = AoI_LNBTS.merge(LNBTS, left_on=['AoI_MRBTS'], right_on=['LNBTS_collected'], how = 'outer', indicator=True)
        missing_AoI_LNBTS = missing_AoI_LNBTS[missing_AoI_LNBTS['_merge'] == 'left_only']
        missing_AoI_LNBTS = missing_AoI_LNBTS['AoI_MRBTS'] 		
    else:
        missing_AoI_LNBTS = pd.DataFrame()    	
	
	#LNBTS_w_Trace_not_Active
    LNBTS_without_MTRACES = Traces_per_LNBTS[(Traces_per_LNBTS['Nr_of_CTRLTSs'] == 0) | (Traces_per_LNBTS['Nr_of_MTRACEs'] == 0)]
    LNBTS_without_MTRACES.reset_index(drop=True, inplace=True)
	
	#LNBTS_w_right_config
    Traces_per_LNBTS['cellMaxActiveUEsTraced'].fillna(0, inplace = True)
    Traces_per_LNBTS['cellMaxActiveUEsTraced'] = Traces_per_LNBTS['cellMaxActiveUEsTraced'].astype(int) 	
    #Reporting CellTraces_Active - LNBTS:actCellTrace-1(true); CTRLTS:cellTraceRepMode-0(Online),ueTraceRepMode-0(Online);MTRACE:Jobtype-[1,2],tceIpAddress not empty,cellMaxActiveUEsTraced>=300,traceRrcSetting-0(All),traceS1Setting-0(All), traceX2Setting-0(All)
    LNBTS_w_Trc_active = Traces_per_LNBTS[((Traces_per_LNBTS['actCellTrace'] =="true") | (Traces_per_LNBTS['actCellTrace'] =="1")) & ((Traces_per_LNBTS['cellTraceRepMode'] == "0") | (Traces_per_LNBTS['cellTraceRepMode'] == "Online") | (Traces_per_LNBTS['traceRepMode'].str.contains("ITEM:cellTraceRepMode=Online"))) & ((Traces_per_LNBTS['ueTraceRepMode'] == "0") |  (Traces_per_LNBTS['ueTraceRepMode'] == "Online") | (Traces_per_LNBTS['traceRepMode'].str.contains("ueTraceRepMode=Online"))) & ((Traces_per_LNBTS['traceRrcSetting'] == "0")| (Traces_per_LNBTS['traceRrcSetting'] == "All")) & ((Traces_per_LNBTS['traceS1Setting'] == "0") | (Traces_per_LNBTS['traceS1Setting'] == "All")) & ((Traces_per_LNBTS['traceX2Setting'] == "0") | (Traces_per_LNBTS['traceX2Setting'] =="All")) & (Traces_per_LNBTS['cellMaxActiveUEsTraced'] >= 300) & ((Traces_per_LNBTS['jobType'] =="1") | (Traces_per_LNBTS['jobType'] =="TraceOnly") | (Traces_per_LNBTS['jobType']== "2") | (Traces_per_LNBTS['jobType']=="ImmediateMDTAndTrace")) & ((Traces_per_LNBTS['tceIpAddress'].isnull() == False) | (Traces_per_LNBTS['tceIpAddress'] !="")) ]	
    #EnodeBs com Cell Trace Activation not active
    LNBTS_without_Trc_active = LNBTS.merge(LNBTS_w_Trc_active, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
    LNBTS_without_Trc_active = LNBTS_without_Trc_active[LNBTS_without_Trc_active['_merge'] == 'left_only']
    LNBTS_without_Trc_active = LNBTS_without_Trc_active['LNBTS_collected']	

    #Reporting CellTraces_and_TAs: -CTRLTS: taTracing-1(true); -MTRACE:cellTaTracing-1(true),tATracingSynch-0(false) 
    LNBTS_w_TAs = Traces_per_LNBTS[((Traces_per_LNBTS['taTracing'] =="1") | (Traces_per_LNBTS['taTracing']== "true") |(Traces_per_LNBTS['vendorSpecTracing'].str.contains("ITEM:taTracing=true"))) & ((Traces_per_LNBTS['cellTaTracing'] =="1") | (Traces_per_LNBTS['cellTaTracing'] =="true")| (Traces_per_LNBTS['cellVendorSpecTracing'].str.contains("cellTaTracing=true", na=False))) & ((~(Traces_per_LNBTS['tATracingSynch']=="1")) &(~(Traces_per_LNBTS['tATracingSynch']=="true")) & (~(Traces_per_LNBTS['enhCellVendorSpecTracing'].str.contains("tATracingSynch=true", na=False))))]	
    LNBTS_w_Trc_active_w_TAs =  LNBTS_w_Trc_active[((LNBTS_w_Trc_active['taTracing'] =="1") | (LNBTS_w_Trc_active['taTracing']== "true") |(LNBTS_w_Trc_active['vendorSpecTracing'].str.contains("ITEM:taTracing=true"))) & ((LNBTS_w_Trc_active['cellTaTracing'] =="1") | (LNBTS_w_Trc_active['cellTaTracing'] =="true")| (LNBTS_w_Trc_active['cellVendorSpecTracing'].str.contains("cellTaTracing=true"))) & ((~(LNBTS_w_Trc_active['tATracingSynch']=="1")) &(~(LNBTS_w_Trc_active['tATracingSynch']=="true")) & (~(LNBTS_w_Trc_active['enhCellVendorSpecTracing'].str.contains("tATracingSynch=true", na=False))))]	
    #EnodeBs without TAs	
    LNBTS_without_TAs = LNBTS.merge(LNBTS_w_TAs, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
    LNBTS_without_TAs = LNBTS_without_TAs[LNBTS_without_TAs['_merge'] == 'left_only']
    LNBTS_without_TAs = LNBTS_without_TAs['LNBTS_collected']
    LNBTS_w_Trc_active_without_TAs = LNBTS.merge(LNBTS_w_Trc_active_w_TAs, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
    LNBTS_w_Trc_active_without_TAs = LNBTS_w_Trc_active_without_TAs[LNBTS_w_Trc_active_without_TAs['_merge'] == 'left_only']
    LNBTS_w_Trc_active_without_TAs = LNBTS_w_Trc_active_without_TAs['LNBTS_collected']	
		
    #Reporting_w_MRs: - MTRACE: reportAmount-65(Infinity), reportInterval-7ou8(5120ms or 10240ms) 
    if MRs == "10":
        LNBTS_w_Trc_active_w_TAs_MRs = LNBTS_w_Trc_active_w_TAs[((LNBTS_w_Trc_active_w_TAs['reportAmount']== "65") | (LNBTS_w_Trc_active_w_TAs['periodicUeMeas'].str.contains("reportAmount=infinity"))) & ((LNBTS_w_Trc_active_w_TAs['reportInterval']== "8") | (LNBTS_w_Trc_active_w_TAs['periodicUeMeas'].str.contains("reportInterval=10240ms")))]
        LNBTS_w_MRs = Traces_per_LNBTS[((Traces_per_LNBTS['reportAmount']== "65") | (Traces_per_LNBTS['periodicUeMeas'].str.contains("reportAmount=infinity"))) & ((Traces_per_LNBTS['reportInterval']== "8") | (Traces_per_LNBTS['periodicUeMeas'].str.contains("reportInterval=10240ms")))]
    else:
        LNBTS_w_Trc_active_w_TAs_MRs = LNBTS_w_Trc_active_w_TAs[((LNBTS_w_Trc_active_w_TAs['reportAmount']== "65") | (LNBTS_w_Trc_active_w_TAs['periodicUeMeas'].str.contains("reportAmount=infinity"))) & ((LNBTS_w_Trc_active_w_TAs['reportInterval']== "7") | (LNBTS_w_Trc_active_w_TAs['periodicUeMeas'].str.contains("reportInterval=5120ms")))]
        LNBTS_w_MRs = Traces_per_LNBTS[((Traces_per_LNBTS['reportAmount']== "65") | (Traces_per_LNBTS['periodicUeMeas'].str.contains("reportAmount=infinity"))) & ((Traces_per_LNBTS['reportInterval']== "7") | (Traces_per_LNBTS['periodicUeMeas'].str.contains("reportInterval=5120ms")))]   	
    #EnodeBs without MRs
    LNBTS_w_Trc_active_w_TAs_without_MRs = LNBTS.merge(LNBTS_w_Trc_active_w_TAs_MRs, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
    LNBTS_w_Trc_active_w_TAs_without_MRs = LNBTS_w_Trc_active_w_TAs_without_MRs[LNBTS_w_Trc_active_w_TAs_without_MRs['_merge'] == 'left_only']
    LNBTS_w_Trc_active_w_TAs_without_MRs = LNBTS_w_Trc_active_w_TAs_without_MRs['LNBTS_collected']     
    LNBTS_without_MRs = LNBTS.merge(LNBTS_w_MRs, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
    LNBTS_without_MRs = LNBTS_without_MRs[LNBTS_without_MRs['_merge'] == 'left_only']
    LNBTS_without_MRs = LNBTS_without_MRs['LNBTS_collected']

	#Reporting_w_Thr: - LNBTS:actUeThroughputMeas-1(true);MTRACE:cellMaxActiveUEsTraced-300,actUeThroughputMR-1(true) 
    if Thr == True:
        LNBTS_w_Trc_active_w_TAs_MRs_Thr = LNBTS_w_Trc_active_w_TAs_MRs[((LNBTS_w_Trc_active_w_TAs_MRs["actUeThroughputMeas"]=="1") |(LNBTS_w_Trc_active_w_TAs_MRs["actUeThroughputMeas"]=="true")) & ((LNBTS_w_Trc_active_w_TAs_MRs["actUeThroughputMR"]=="1") | (LNBTS_w_Trc_active_w_TAs_MRs["actUeThroughputMR"]=="true")) & (LNBTS_w_Trc_active_w_TAs_MRs['cellMaxActiveUEsTraced'] >= 300)]
        LNBTS_w_Thr = Traces_per_LNBTS[((Traces_per_LNBTS["actUeThroughputMeas"]=="1") | (Traces_per_LNBTS["actUeThroughputMeas"]=="true")) & ((Traces_per_LNBTS["actUeThroughputMR"]=="1") | (Traces_per_LNBTS["actUeThroughputMR"]=="true")) & (Traces_per_LNBTS['cellMaxActiveUEsTraced'] >= 300) ]
    else:
        LNBTS_w_Thr = None
    #EnodeBs without Thr
    LNBTS_w_Trc_active_w_TAs_MRs_without_Thr = LNBTS.merge(LNBTS_w_Trc_active_w_TAs_MRs_Thr, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
    LNBTS_w_Trc_active_w_TAs_MRs_without_Thr = LNBTS_w_Trc_active_w_TAs_MRs_without_Thr[LNBTS_w_Trc_active_w_TAs_MRs_without_Thr['_merge'] == 'left_only']
    LNBTS_w_Trc_active_w_TAs_MRs_without_Thr = LNBTS_w_Trc_active_w_TAs_MRs_without_Thr['LNBTS_collected']     
    LNBTS_without_Thr = LNBTS.merge(LNBTS_w_Thr, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
    LNBTS_without_Thr = LNBTS_without_Thr[LNBTS_without_Thr['_merge'] == 'left_only']
    LNBTS_without_Thr = LNBTS_without_Thr['LNBTS_collected']    		
		
	#reporting MDT:- LNBTS: actMDTCellTrace-1(true); MTRACE:Jobtype-2(ImmediateMDTAndTrace),immedMDTAnonymization-1(true),immedMDTForceUEConsent-1(true),immedMDTObtainLocation-1(true),immedMDTPosMethod-1(GNSS), immedMDTSelectOnlyGNSSUes-0(false),cellMaxActiveMDTUEsTraced-300   
    if MDT == True:
        #check eNodeBs_w_all_Features_active
        LNBTS_w_Trc_active_w_TAs_MRs_Thr["cellMaxActiveMDTUEsTraced_v2"] = LNBTS_w_Trc_active_w_TAs_MRs_Thr['cellMaxActiveMDTUEsTraced'].replace("", "0").astype(int)
        LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTControl_cellMaxActiveMDTUEsTraced"]=LNBTS_w_Trc_active_w_TAs_MRs_Thr['immedMDTControl'].str.extract(r'cellMaxActiveMDTUEsTraced=([0-9]{0,6});').replace(np.NaN, "0").astype(int)		
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT = LNBTS_w_Trc_active_w_TAs_MRs_Thr[((LNBTS_w_Trc_active_w_TAs_MRs_Thr["actMDTCellTrace"] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["actMDTCellTrace"] == "true") ) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr["jobType"] == "2")| (LNBTS_w_Trc_active_w_TAs_MRs_Thr["jobType"] == "ImmediateMDTAndTrace")) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTAnonymization"] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTAnonymization"] == "true") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTControl"].str.contains("immedMDTAnonymization=NoIdentity"))) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTForceUEConsent"] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTForceUEConsent"] == "true") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTControl"].str.contains("immedMDTForceUEConsent=true"))) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTObtainLocation"] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTObtainLocation"] == "true") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTControl"].str.contains("immedMDTObtainLocation=true"))) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTPosMethod"] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTPosMethod"] == "true") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTControl"].str.contains("immedMDTPosMethod=GNSS"))) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTSelectOnlyGNSSUes"] == "0") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTSelectOnlyGNSSUes"] == "false") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTControl"].str.contains("immedMDTSelectOnlyGNSSUes=false"))) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr["cellMaxActiveMDTUEsTraced_v2"] >= 300) | (LNBTS_w_Trc_active_w_TAs_MRs_Thr["immedMDTControl_cellMaxActiveMDTUEsTraced"] >= 300))] 
        #check Traces w MDT		
        Traces_per_LNBTS["cellMaxActiveMDTUEsTraced_v2"] = Traces_per_LNBTS['cellMaxActiveMDTUEsTraced'].replace("", "0").astype(int)
        Traces_per_LNBTS["immedMDTControl_cellMaxActiveMDTUEsTraced"] = Traces_per_LNBTS['immedMDTControl'].str.extract(r'cellMaxActiveMDTUEsTraced=([0-9]{0,6});').replace(np.NaN, "0").astype(int)			
        LNBTS_w_MDT = Traces_per_LNBTS[((Traces_per_LNBTS["actMDTCellTrace"] == "1") | ((Traces_per_LNBTS["actMDTCellTrace"] == "true"))) & ((Traces_per_LNBTS["jobType"] == "2") | (Traces_per_LNBTS["jobType"] == "ImmediateMDTAndTrace")) & ((Traces_per_LNBTS["immedMDTAnonymization"] == "1") | (Traces_per_LNBTS["immedMDTAnonymization"] == "true") | (Traces_per_LNBTS["immedMDTControl"].str.contains("immedMDTAnonymization=NoIdentity"))) & ((Traces_per_LNBTS["immedMDTForceUEConsent"] == "1") | (Traces_per_LNBTS["immedMDTForceUEConsent"] == "true") | (Traces_per_LNBTS["immedMDTControl"].str.contains("immedMDTForceUEConsent=true"))) & ((Traces_per_LNBTS["immedMDTObtainLocation"] == "1") | (Traces_per_LNBTS["immedMDTObtainLocation"] == "true") | (Traces_per_LNBTS["immedMDTControl"].str.contains("immedMDTObtainLocation=true"))) & ((Traces_per_LNBTS["immedMDTPosMethod"] == "1") | (Traces_per_LNBTS["immedMDTPosMethod"] == "true") | (Traces_per_LNBTS["immedMDTControl"].str.contains("immedMDTPosMethod=GNSS"))) & ((Traces_per_LNBTS["immedMDTSelectOnlyGNSSUes"] == "0") | (Traces_per_LNBTS["immedMDTSelectOnlyGNSSUes"] == "false") | (Traces_per_LNBTS["immedMDTControl"].str.contains("immedMDTSelectOnlyGNSSUes=false"))) & ((Traces_per_LNBTS["cellMaxActiveMDTUEsTraced_v2"] >= 300) | (Traces_per_LNBTS["immedMDTControl_cellMaxActiveMDTUEsTraced"] >= 300))] 
	    #EnodeBs without MDT
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT = LNBTS.merge(LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT = LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT[LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT['_merge'] == 'left_only']
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT = LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT['LNBTS_collected']     
        LNBTS_without_MDT = LNBTS.merge(LNBTS_w_MDT, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
        LNBTS_without_MDT = LNBTS_without_MDT[LNBTS_without_MDT['_merge'] == 'left_only']
        LNBTS_without_MDT = LNBTS_without_MDT['LNBTS_collected']  
    else:
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT = LNBTS_w_Trc_active_w_TAs_MRs_Thr
        LNBTS_w_MDT = None
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT = None
        LNBTS_without_MDT = None

    #ENDC: LNBTS: actSecRatRep-1(True), actEvtSecRatRep-1(True), actLteNrDualConnectivity-1(True), actDynTrigLteNrDualConnectivity-1(True) 
    if ENDC == True:
        #check eNodeBs_w_all_Features_active
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT[((LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT['actSecRatRep'] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT['actSecRatRep']== "true")) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT['actEvtSecRatRep'] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT['actEvtSecRatRep']== "true")) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT['actLteNrDualConnectivity'] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT['actLteNrDualConnectivity']== "true")) & ((LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT['actDynTrigLteNrDualConnectivity'] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT['actDynTrigLteNrDualConnectivity']== "true"))]
        #check Traces w ENDC
        LNBTS_w_ENDC = Traces_per_LNBTS[((Traces_per_LNBTS['actSecRatRep'] == "1") | (Traces_per_LNBTS['actSecRatRep']== "true")) & ((Traces_per_LNBTS['actEvtSecRatRep'] == "1") | (Traces_per_LNBTS['actEvtSecRatRep']== "true")) & ((Traces_per_LNBTS['actLteNrDualConnectivity'] == "1") | (Traces_per_LNBTS['actLteNrDualConnectivity']== "true")) & ((Traces_per_LNBTS['actDynTrigLteNrDualConnectivity'] == "1") | (Traces_per_LNBTS['actDynTrigLteNrDualConnectivity']== "true"))]
        #EnodeBs without ENDC
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC = LNBTS.merge(LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC[LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC['_merge'] == 'left_only']
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC['LNBTS_collected']     
        LNBTS_without_ENDC = LNBTS.merge(LNBTS_w_ENDC, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
        LNBTS_without_ENDC = LNBTS_without_ENDC[LNBTS_without_ENDC['_merge'] == 'left_only']
        LNBTS_without_ENDC = LNBTS_without_ENDC['LNBTS_collected']  		
    else:
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT
        LNBTS_w_ENDC = None
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC = None
        LNBTS_without_ENDC = None
		
    #NSETAP: MTRACE:  
    if NSETAP == True:
        #check eNodeBs_w_all_Features_active
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_NSETAP = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC[((LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC['interfaceSelection'] == "1") | (LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC['interfaceSelection']== "NSETAP"))]
        #check Traces w NSETAP
        LNBTS_w_NSETAP = Traces_per_LNBTS[((Traces_per_LNBTS['interfaceSelection'] == "1") | (Traces_per_LNBTS['interfaceSelection']== "NSETAP"))]
        #EnodeBs without NSETAP
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP = LNBTS.merge(LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_NSETAP, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP[LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP['_merge'] == 'left_only']
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP['LNBTS_collected']
        LNBTS_without_NSETAP = LNBTS.merge(LNBTS_w_NSETAP, left_on=['LNBTS_collected'], right_on=['LNBTS_distName'], how='outer', indicator=True)
        LNBTS_without_NSETAP = LNBTS_without_NSETAP[LNBTS_without_NSETAP['_merge'] == 'left_only']
        LNBTS_without_NSETAP = LNBTS_without_NSETAP['LNBTS_collected'] 		
    else:
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_NSETAP = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC
        LNBTS_w_NSETAP = None
        LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP = None
        LNBTS_without_NSETAP = None        	

    #list of enodebs w all Features active
    LNBTS_w_All_Features = LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC['LNBTS_distName'].unique()	
    LNBTS_w_All_Features = pd.DataFrame(LNBTS_w_All_Features, columns = ['LNBTS'])

    #list of enodebs without all Features active
    LNBTS_without_All_Features = LNBTS.merge(LNBTS_w_All_Features, left_on=['LNBTS_collected'], right_on=['LNBTS'], how='outer', indicator=True)
    LNBTS_without_All_Features = LNBTS_without_All_Features[LNBTS_without_All_Features['_merge']=='left_only']	
    LNBTS_without_All_Features = pd.DataFrame(LNBTS_without_All_Features, columns = ['LNBTS_collected'])
    LNBTS_without_All_Features = LNBTS_without_All_Features.rename(columns = {'LNBTS_collected':'LNBTSs'})
    
    #Output_Nokia_4G_Trace_reports(output_base_dir, AoI_LNBTS, LNBTS, LNBTS_w_All_Features, LNBTS_without_MTRACES, LNBTS_w_Trc_active, LNBTS_without_Trc_active, LNBTS_w_TAs, LNBTS_w_MRs, LNBTS_w_Thr, LNBTS_w_MDT, LNBTS_without_All_Features, Thr, MDT, MRs, missing_AoI_LNBTS, LNBTS_without_TAs, LNBTS_w_Trc_active_without_TAs, LNBTS_without_MRs, LNBTS_w_Trc_active_w_TAs_without_MRs, LNBTS_without_Thr, LNBTS_w_Trc_active_w_TAs_MRs_without_Thr, LNBTS_without_MDT, LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT)
    return  AoI_LNBTS, LNBTS, LNBTS_w_All_Features, LNBTS_without_MTRACES, LNBTS_w_Trc_active, LNBTS_without_Trc_active, LNBTS_w_TAs, LNBTS_w_MRs, LNBTS_w_Thr, LNBTS_w_MDT, LNBTS_without_All_Features, missing_AoI_LNBTS, LNBTS_without_TAs, LNBTS_w_Trc_active_without_TAs, LNBTS_without_MRs, LNBTS_w_Trc_active_w_TAs_without_MRs, LNBTS_without_Thr, LNBTS_w_Trc_active_w_TAs_MRs_without_Thr, LNBTS_without_MDT, LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT, LNBTS_w_ENDC, LNBTS_without_ENDC, LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC, LNBTS_w_NSETAP, LNBTS_without_NSETAP, LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP

	
###############################Nokia 5G Trace Configuration Analysis##############################

def Nokia_5G_Traces_Configurations(output_base_dir, NRBTS, NRMTRACECU, NRMTRACEDU, TCE, AoI_cell_list, PCMD):

    #Merge 5G MO dataframes for Trace Configuration
    try:
        Traces_per_NRBTS = NRBTS.merge(NRMTRACECU, left_on=["MRBTS", "NRBTS"], right_on=["NRMTRACECU_MRBTS", "NRMTRACECU_NRBTS"], how = 'left').merge(NRMTRACEDU, left_on=['MRBTS', 'NRBTS'], right_on=['NRMTRACEDU_MRBTS', 'NRMTRACEDU_NRBTS'], how = 'left')
        Traces_per_NRBTS = Traces_per_NRBTS.drop(columns=['NRMTRACECU_MRBTS', 'NRMTRACECU_NRBTS', 'NRMTRACEDU_MRBTS', 'NRMTRACEDU_NRBTS'])
        if not TCE.empty:
            Traces_per_NRBTS = Traces_per_NRBTS.merge(TCE, left_on=['MRBTS'], right_on=['TCE_MRBTS'], how = 'left')
            Traces_per_NRBTS = Traces_per_NRBTS.drop(columns=['TCE_MRBTS'])			
        else:
            print("There's no TCE!")		
        Traces_per_NRBTS['Nr_of_NRCTRLTSs']= Traces_per_NRBTS.groupby(["MRBTS", "NRBTS"])["NRCTRLTS"].transform('nunique')
        Traces_per_NRBTS['Nr_of_NRMTRACECUs']= Traces_per_NRBTS.groupby(["MRBTS", "NRBTS"])["NRMTRACECU"].transform('nunique')
        Traces_per_NRBTS['Nr_of_NRDUs']= Traces_per_NRBTS.groupby(["MRBTS", "NRBTS"])["NRDU"].transform('nunique')		
        Traces_per_NRBTS['Nr_of_NRCTRLTSDUs']= Traces_per_NRBTS.groupby(["MRBTS", "NRBTS"])["NRCTRLTSDU"].transform('nunique')
        Traces_per_NRBTS['Nr_of_NRMTRACEDUs']= Traces_per_NRBTS.groupby(["MRBTS", "NRBTS"])["NRMTRACEDU"].transform('nunique')		
    except:
        try:
            Traces_per_NRBTS = NRBTS.merge(NRMTRACECU, left_on=["MRBTS", "NRBTS"], right_on=["NRMTRACECU_MRBTS", "NRMTRACECU_NRBTS"], how = 'left')
            Traces_per_NRBTS = Traces_per_NRBTS.drop(columns=['NRMTRACECU_MRBTS', 'NRMTRACECU_NRBTS'])			
            if not TCE.empty:
                Traces_per_NRBTS = Traces_per_NRBTS.merge(TCE, left_on=['MRBTS'], right_on=['TCE_MRBTS'], how = 'left')
                Traces_per_NRBTS = Traces_per_NRBTS.drop(columns=['TCE_MRBTS'])					
            else:
                print("There no TCE")				
            Traces_per_NRBTS['Nr_of_NRCTRLTSs']= Traces_per_LNBTS.groupby(["MRBTS", "NRBTS"])["NRCTRLTS"].transform('nunique')            			
            Traces_per_NRBTS['Nr_of_NRMTRACECUs']= Traces_per_LNBTS.groupby(["MRBTS", "NRBTS"])["NRMTRACECU"].transform('nunique')	
            print("The NRMTRACEDU is missing in the XML file")
            quit()
        except:
            print("At least 2 of the important MOs are missing in the XML file: NRBTS, NRMTRACECU, NRMTRACEDU")
            quit() 	

    #AoI_NRBTS
    if not AoI_cell_list.empty:	
        AoI_NRBTS = NRBTS_AoI(AoI_cell_list)
    else:
        AoI_NRBTS = pd.DataFrame()		
    #updatar lista de Traces em função de AoI
    if not AoI_NRBTS.empty:
        Traces_per_NRBTS = Traces_per_NRBTS.merge(AoI_NRBTS, left_on=['MRBTS'], right_on=['AoI_MRBTS'], how = 'inner')

    #list of NRBTS collected
    NRBTS = Traces_per_NRBTS['NRBTS_distName'].unique()
    NRBTS = pd.DataFrame(NRBTS, columns = ['NRBTS_collected'])  	
   
    #list of NRBTS not collected when compared with AoI_NRBTS
    if not AoI_NRBTS.empty:
        missing_AoI_NRBTS = AoI_NRBTS.merge(NRBTS, left_on=['AoI_MRBTS'], right_on=['NRBTS_collected'], how = 'outer', indicator=True)
        missing_AoI_NRBTS = missing_AoI_NRBTS[missing_AoI_NRBTS['_merge'] == 'left_only']
        missing_AoI_NRBTS = missing_AoI_NRBTS['AoI_MRBTS'] 		
    else:
        missing_AoI_NRBTS = pd.DataFrame()    	
		
	#NRBTS_w_Trace_not_Active
    NRBTS_without_NRCTRLTSs = Traces_per_NRBTS[(Traces_per_NRBTS['Nr_of_NRCTRLTSs'] == 0)]
    NRBTS_without_NRCTRLTSs.reset_index(drop=True, inplace=True)
	
	#NRBTS_w_right_config
    Traces_per_NRBTS['cellMaxActiveUEsTraced'].fillna(0, inplace = True)
    Traces_per_NRBTS['cellMaxActiveUEsTraced'] = Traces_per_NRBTS['cellMaxActiveUEsTraced'].astype(int)
    #Reporting CellTraces_Active - NRBTS:actCellTraceReport-1(true); NRMTRACECU:actCellTraceReportingCU-1(True),actCellTraceReportingCU-1(True), traceNGAPSetting-1(all), traceXNSetting-1(all), traceNRRRCSetting-1(all), cellMaxActiveUEsTraced>1000, jobType-1ou2(TraceOnly or ImmediateMDTAndTrace), NRMTRACECU_scope-0(allResources), NRMTRACEDU_scope-0(allResources); NRMTRACECU:nrRanTraceReference(not null) ;NRMTRACEDU:nrRanTraceReference(not null)      
    NRBTS_w_Trc_active = Traces_per_NRBTS[((Traces_per_NRBTS['actCellTraceReport'] =="true") | (Traces_per_NRBTS['actCellTraceReport'] =="1")) & ((Traces_per_NRBTS['actCellTraceReportingCU'] == "1") | (Traces_per_NRBTS['actCellTraceReportingCU'] == "true")) & ((Traces_per_NRBTS['traceNGAPSetting'] == "1") | (Traces_per_NRBTS['traceNGAPSetting'] == "all")) & ((Traces_per_NRBTS['traceXNSetting'] == "1") | (Traces_per_NRBTS['traceXNSetting'] == "all")) & ((Traces_per_NRBTS['traceNRRRCSetting'] == "1") | (Traces_per_NRBTS['traceNRRRCSetting'] == "all")) & (Traces_per_NRBTS['cellMaxActiveUEsTraced'] >= 100) & ((Traces_per_NRBTS['jobType'] =="1") | (Traces_per_NRBTS['jobType'] =="TraceOnly") | (Traces_per_NRBTS['jobType']== "2") | (Traces_per_NRBTS['jobType']=="ImmediateMDTAndTrace")) & ((Traces_per_NRBTS['NRMTRACECU_scope'] =='0')|(Traces_per_NRBTS['NRMTRACECU_scope'] =='allResources')) & ((Traces_per_NRBTS['NRMTRACEDU_scope'] =='0')|(Traces_per_NRBTS['NRMTRACEDU_scope'] =='allResources'))& (Traces_per_NRBTS['NRMTRACECU_nrRanTraceReference']!="") & (Traces_per_NRBTS['NRMTRACEDU_nrRanTraceReference']!="")]	
    #Reporting CellTraces_Active - TCE: tceIpAddress IS NOT NULL; tcePortNumber IS NOT NULL;
    if not TCE.empty:    
        NRBTS_w_Trc_active = NRBTS_w_Trc_active[(NRBTS_w_Trc_active['tceIpAddress'] !="") & (NRBTS_w_Trc_active['tcePortNumber'] !="")]

    #NRBTS com Cell Trace Activation not active
    NRBTS_without_Trc_active = NRBTS.merge(NRBTS_w_Trc_active, left_on=['NRBTS_collected'], right_on=['NRBTS_distName'], how='outer', indicator=True)
    NRBTS_without_Trc_active = NRBTS_without_Trc_active[NRBTS_without_Trc_active['_merge'] == 'left_only']
    NRBTS_without_Trc_active = NRBTS_without_Trc_active['NRBTS_collected']		
				
    #checking PCMD Feature activation -NRBTS:actPCMDReport-1(true); -NRMTRACECU: actPCMDReporting-(true); -NRMTRACEDU: actPCMDReporting-(true);		
    if PCMD == True:
        NRBTS_w_PCMD = Traces_per_NRBTS[((Traces_per_NRBTS['actPCMDReport'] =="1")| (Traces_per_NRBTS['actPCMDReport'] =="true")) & ((Traces_per_NRBTS['NRMTRACECU_actPCMDReporting'] =="1") | (Traces_per_NRBTS['NRMTRACECU_actPCMDReporting'] =="true")) & ((Traces_per_NRBTS['NRMTRACEDU_actPCMDReporting'] =="1") | (Traces_per_NRBTS['NRMTRACEDU_actPCMDReporting'] =="true"))]	
        NRBTS_w_Trc_active_w_PCMD = NRBTS_w_Trc_active[((NRBTS_w_Trc_active['actPCMDReport'] =="1")| (NRBTS_w_Trc_active['actPCMDReport'] =="true")) & ((NRBTS_w_Trc_active['NRMTRACECU_actPCMDReporting'] =="1") | (NRBTS_w_Trc_active['NRMTRACECU_actPCMDReporting'] =="true")) & ((NRBTS_w_Trc_active['NRMTRACEDU_actPCMDReporting'] =="1") | (NRBTS_w_Trc_active['NRMTRACEDU_actPCMDReporting'] =="true"))]
    #NRBTSs without PCMD
    NRBTS_without_PCMD = NRBTS.merge(NRBTS_w_PCMD, left_on=['NRBTS_collected'], right_on=['NRBTS_distName'], how='outer', indicator=True)
    NRBTS_without_PCMD = NRBTS_without_PCMD[NRBTS_without_PCMD['_merge'] == 'left_only']
    NRBTS_without_PCMD = NRBTS_without_PCMD['NRBTS_collected']
    NRBTS_w_Trc_active_without_PCMD = NRBTS.merge(NRBTS_w_Trc_active_w_PCMD, left_on=['NRBTS_collected'], right_on=['NRBTS_distName'], how='outer', indicator=True)
    NRBTS_w_Trc_active_without_PCMD = NRBTS_w_Trc_active_without_PCMD[NRBTS_w_Trc_active_without_PCMD['_merge'] == 'left_only']
    NRBTS_w_Trc_active_without_PCMD = NRBTS_w_Trc_active_without_PCMD['NRBTS_collected']			
		
    #list of NRBTS w all Features active
    NRBTS_w_All_Features = NRBTS_w_Trc_active_w_PCMD['NRBTS_distName'].unique()	
    NRBTS_w_All_Features = pd.DataFrame(NRBTS_w_All_Features, columns = ['NRBTS'])	
	
    #list of NRBTS without all Features active
    NRBTS_without_All_Features = NRBTS.merge(NRBTS_w_All_Features, left_on=['NRBTS_collected'], right_on=['NRBTS'], how='outer', indicator=True)
    NRBTS_without_All_Features = NRBTS_without_All_Features[NRBTS_without_All_Features['_merge']=='left_only']	
    NRBTS_without_All_Features = pd.DataFrame(NRBTS_without_All_Features, columns = ['NRBTS_collected'])
    NRBTS_without_All_Features = NRBTS_without_All_Features.rename(columns = {'NRBTS_collected':'NRBTSs'})	
	
    return(AoI_NRBTS, NRBTS, missing_AoI_NRBTS, NRBTS_without_NRCTRLTSs, NRBTS_w_Trc_active, NRBTS_without_Trc_active, NRBTS_w_PCMD, NRBTS_without_PCMD, NRBTS_w_Trc_active_without_PCMD, NRBTS_w_All_Features, NRBTS_without_All_Features)	

	
##########################################Output of Nokia 4G Trace Report###########################################

def Output_Nokia_4G_Trace_reports(output_base_dir, ENDC, NSETAP, AoI_LNBTS, LNBTS, LNBTS_w_All_Features, LNBTS_without_MTRACES, LNBTS_w_Trc_active, LNBTS_without_Trc_active, LNBTS_w_TAs, LNBTS_w_MRs, LNBTS_w_Thr, LNBTS_w_MDT, LNBTS_without_All_Features, Thr, MDT, MRs, missing_AoI_LNBTS, LNBTS_without_TAs, LNBTS_w_Trc_active_without_TAs, LNBTS_without_MRs, LNBTS_w_Trc_active_w_TAs_without_MRs, LNBTS_without_Thr, LNBTS_w_Trc_active_w_TAs_MRs_without_Thr, LNBTS_without_MDT, LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT, LNBTS_w_ENDC, LNBTS_without_ENDC, LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC, LNBTS_w_NSETAP, LNBTS_without_NSETAP, LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP):
	#write_outfile_report
    report = open(output_base_dir + "report_Nokia_4G_Trace_Analysis.txt", "w")
    report.write("############################################ Summary report: ############################################"+ "\n")	
    
	#when there's or not AoI
    if not AoI_LNBTS.empty:
        report.write("-Number eNodeBs expected in the network (AoI list): "+ str(len(AoI_LNBTS))  +"\n")
        report.write("-Number eNodeBs NOT FOUND in the network: " + str(len(missing_AoI_LNBTS))  +"\n")
        report.write(missing_AoI_LNBTS.to_string(header=False) + "\n")
    else:
        report.write("-No AoI cell list defined as Input! \n")
		
    #General Overview	
    report.write("-Number of eNodeBs found in the network: "+ str(len(LNBTS))  +"\n")
    report.write("-Number of eNodeBs found in the network with all Features Properly configured: " + str(len(LNBTS_w_All_Features))  +"\n")	
    report.write("-Number of eNodeBs found in the network WITHOUT all Features Properly configured: " + str(len(LNBTS_without_All_Features)) + " \n")
    if not LNBTS_without_Trc_active.empty:  
        report.write(LNBTS_without_Trc_active.to_string(header=False) + "\n \n")
	
    if not LNBTS_without_MTRACES[['LNBTS_distName', 'Nr_of_CTRLTSs', 'Nr_of_MTRACEs',]].empty:
        report.write("-Number of eNodeBs without CTRLTS/MTRACEs instances: \n")
        report.write(LNBTS_without_MTRACES[['LNBTS_distName', 'Nr_of_CTRLTSs', 'Nr_of_MTRACEs',]].to_string() + "\n \n")	
    report.write("\n################################################################################################# \n")
    report.write("#####                     More Detailed Analysis of Features Activation:                    ##### \n")
    report.write("################################################################################################# \n")

    #List of eNodeBs without All Features activated
    if not LNBTS_without_All_Features.empty:
        print("-List of eNodeBs without All Features active:")
        print(LNBTS_without_All_Features)
	
    #Trace Activation
    report.write("\n########################################################################################################################## Cell Trace Activation Feature ######################################################################################################################\n")	
    report.write("###LNBTS:actCellTrace=1(true); CTRLTS:cellTraceRepMode=0(Online),ueTraceRepMode=0(Online); MTRACE:Jobtype in [1(TraceOnly), 2(ImmediateMDTAndTrace)],tceIpAddress not empty,cellMaxActiveUEsTraced>=300,traceRrcSetting=0(All),traceS1Setting=0(All), traceX2Setting=0(All)####\n")
    report.write("-The number of eNodeBs with MTRACEs containing Cell Trace Feature correctly active:" + str(len(LNBTS_w_Trc_active['LNBTS_distName'].unique())) + "\n")
    if not LNBTS_without_Trc_active.empty: 
        report.write("-The number of eNodeBs with MTRACEs WITHOUT containing Cell Trace Feature correctly active:" + str(len(LNBTS_without_Trc_active)) + "\n")		
        report.write(LNBTS_without_Trc_active.to_string(header=False) + "\n \n")
	
    #TAs
    report.write("\n#################################Timing Advance Feature##########################################\n")
    report.write("########CTRLTS: taTracing=1(true); MTRACE:cellTaTracing=1(true),tATracingSynch=0(false) #########\n")	
    report.write("-The number of eNodeBs with MTRACEs containing Timing Advance Feature correctly active: " + str(len(LNBTS_w_TAs['LNBTS_distName'].unique())) + "\n")
    if not LNBTS_without_TAs.empty:
        report.write("-The number of eNodeBes not containg Timing Advance correctly active: " + str(len(LNBTS_without_TAs)) + "\n")
        report.write(LNBTS_without_TAs.to_string() + "\n")		
    if not LNBTS_w_Trc_active_without_TAs.empty:
        report.write("-The number of eNodeBes with Trace Configuration active not containg Timing Advance: " + str(len(LNBTS_w_Trc_active_without_TAs)) + "\n")	
        report.write(LNBTS_w_Trc_active_without_TAs.to_string() + "\n")	

    #MRs
    report.write("\n###############################Measurement Report Feature########################################\n")
    if(MRs == "10"):
        report.write("##############     MTRACE: reportAmount=65(infinity), reportInterval=8(10540ms)    ##############\n")
    else:
        report.write("##############     MTRACE: reportAmount=65(infinity), reportInterval=7(5120ms)     ##############\n")	
    report.write("-The number of eNodeBs containing Measurement Report Feature active:" + str(len(LNBTS_w_MRs['LNBTS_distName'].unique())) + "\n")
    if not LNBTS_without_MRs.empty:
        report.write("-The number of eNodeBes not containg MR Feature correctly active:" + str(len(LNBTS_without_MRs)) + "\n")
        report.write(LNBTS_without_MRs.to_string() + "\n ")		
    if not LNBTS_w_Trc_active_w_TAs_without_MRs.empty:
        report.write("-The number of eNodeBes w Trace Config and TA Features active but without MR correctly active: " + str(len(LNBTS_w_Trc_active_w_TAs_without_MRs)) + "\n")
        report.write(LNBTS_w_Trc_active_w_TAs_without_MRs.to_string() + "\n ")			

    #Thr
    if Thr == True:
        report.write("\n#####################################Throughput Feature##########################################\n")
        report.write("#LNBTS:actUeThroughputMeas=1(true); MTRACE:cellMaxActiveUEsTraced=>300,actUeThroughputMR=1(true)## \n")		
        report.write("-The number of eNodeBs with containing Throughput Feature active: " + str(len(LNBTS_w_Thr['LNBTS_distName'].unique())) + "\n")
        if not LNBTS_without_Thr.empty:
            report.write("-The number of eNodeBes not containg Throughput Feature correctly active:" + str(len(LNBTS_without_Thr)) + "\n")
            report.write(LNBTS_without_Thr.to_string() + "\n ")		
        if not LNBTS_w_Trc_active_w_TAs_MRs_without_Thr.empty:
            report.write("-The number of eNodeBes w Trace Config, MR and TA Features active but without Throughput correctly active: " + str(len(LNBTS_w_Trc_active_w_TAs_MRs_without_Thr)) + "\n")
            report.write(LNBTS_w_Trc_active_w_TAs_MRs_without_Thr.to_string() + "\n ")			        		

    #MDT		
    if MDT == True:
        report.write("\n#####################################MDT Feature##########################################\n")
        report.write("##LNBTS: actMDTCellTrace=1(true); MTRACE:Jobtype=2(ImmediateMDTAndTrace),immedMDTAnonymization=1(true),immedMDTForceUEConsent=1(true),immedMDTObtainLocation=1(true),immedMDTPosMethod=1(GNSS), immedMDTSelectOnlyGNSSUes=0(false),cellMaxActiveMDTUEsTraced=300 ##\n")		
        report.write("-The number of eNodeBs with containing MDT Feature active: " + str(len(LNBTS_w_MDT['LNBTS_distName'].unique())) + "\n")
        if not LNBTS_without_MDT.empty:
            report.write("-The number of eNodeBes not containg MDT Feature correctly active: " + str(len(LNBTS_without_MDT)) + "\n")
            report.write(LNBTS_without_MDT.to_string() + "\n ")		
        if not LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT.empty:
            report.write("-The number of eNodeBes w Trace Config, Throughput, MR and TA Features active but without MDT correctly active: " + str(len(LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT)) + "\n")
            report.write(LNBTS_w_Trc_active_w_TAs_MRs_Thr_without_MDT.to_string() + "\n ")

    #ENDC		
    if ENDC == True:
        report.write("\n#####################################ENDC Feature##########################################\n")
        report.write("##   ##\n")		
        report.write("-The number of eNodeBs with containing ENDC Feature active: " + str(len(LNBTS_w_ENDC['LNBTS_distName'].unique())) + "\n")
        if not LNBTS_without_ENDC.empty:
            report.write("-The number of eNodeBes not containg ENDC Feature correctly active: " + str(len(LNBTS_without_ENDC)) + "\n")
            report.write(LNBTS_without_ENDC.to_string() + "\n ")		
        if not LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC.empty:
            report.write("-The number of eNodeBes w Trace Config and all Features active but without NSETAP correctly active: " + str(len(LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC)) + "\n")
            report.write(LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_without_ENDC.to_string() + "\n ")			
			
    #NSETAP		
    if NSETAP == True:
        report.write("\n#####################################NSETAP Feature##########################################\n")
        report.write("##   ##\n")		
        report.write("-The number of eNodeBs with containing NSETAP interface active: " + str(len(LNBTS_w_NSETAP['LNBTS_distName'].unique())) + "\n")
        if not LNBTS_without_NSETAP.empty:
            report.write("-The number of eNodeBes not containg NSETAP interface correctly active: " + str(len(LNBTS_without_NSETAP)) + "\n")
            report.write(LNBTS_without_NSETAP.to_string() + "\n ")		
        if not LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP.empty:
            report.write("-The number of eNodeBes w Trace COnfig and all Features active but without NSETAP correctly active: " + str(len(LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP)) + "\n")
            report.write(LNBTS_w_Trc_active_w_TAs_MRs_Thr_MDT_ENDC_without_NSETAP.to_string() + "\n ")	

##########################################Output of Nokia 5G Trace Report###########################################
			
			
def Output_Nokia_5G_Trace_reports(output_base_dir, PCMD, AoI_NRBTS, NRBTS, missing_AoI_NRBTS, NRBTS_without_NRCTRLTSs, NRBTS_w_Trc_active, NRBTS_without_Trc_active, NRBTS_w_PCMD, NRBTS_without_PCMD, NRBTS_w_Trc_active_without_PCMD, NRBTS_w_All_Features, NRBTS_without_All_Features):
	#write_outfile_report
    report = open(output_base_dir + "report_Nokia_5G_Trace_Analysis.txt", "w")
    report.write("############################################ Summary report: ############################################"+ "\n")	
    
	#when there's or not AoI
    if not AoI_NRBTS.empty:
        report.write("-Number eNodeBs expected in the network (AoI list): "+ str(len(AoI_NRBTS))  +"\n")
        report.write("-Number eNodeBs NOT FOUND in the network: " + str(len(missing_AoI_NRBTS))  +"\n")
        report.write(missing_AoI_NRBTS.to_string(header=False) + "\n")
    else:
        report.write("-No AoI cell list defined as Input! \n")
		
    #General Overview	
    report.write("-Number of NRBTSs found in the network: "+ str(len(NRBTS))  +"\n")
    report.write("-Number of NRBTSs found in the network with all Features Properly configured: " + str(len(NRBTS_w_All_Features))  +"\n")	
    report.write("-Number of NRBTSs found in the network WITHOUT all Features Properly configured: " + str(len(NRBTS_without_All_Features)) + " \n")
    if not NRBTS_without_Trc_active.empty:  
        report.write(NRBTS_without_Trc_active.to_string(header=False) + "\n \n")
		
    if not NRBTS_without_NRCTRLTSs[['NRBTS_distName', 'Nr_of_NRCTRLTSs',]].empty:
        report.write("-Number of NRBTS without NRCTRLTS instances: \n")
        report.write(NRBTS_without_NRCTRLTSs[['NRBTS_distName', 'Nr_of_NRCTRLTSs',]].to_string() + "\n \n")	
    report.write("\n################################################################################################# \n")
    report.write("#####                     More Detailed Analysis of Features Activation:                    ##### \n")
    report.write("################################################################################################# \n")

    #List of NRBTSs without All Features activated
    if not NRBTS_without_All_Features.empty:
        print("-List of NRBTSs without All Features active:")
        print(NRBTS_without_All_Features)
	
    #Trace Activation
    report.write("\n########################################################################################################################## Cell Trace Activation Feature ######################################################################################################################\n")	
    report.write("###NRBTS:actCellTraceReport-1(true); NRMTRACECU:actCellTraceReportingCU-1(True),actCellTraceReportingCU-1(True), traceNGAPSetting-1(all), traceXNSetting-1(all), traceNRRRCSetting-1(all), cellMaxActiveUEsTraced>1000, jobType-1ou2(TraceOnly or ImmediateMDTAndTrace), NRMTRACECU_scope-0(allResources), NRMTRACEDU_scope-0(allResources); NRMTRACECU:nrRanTraceReference(not null) ;NRMTRACEDU:nrRanTraceReference(not null); TCE: tceIpAddress IS NOT NULL; tcePortNumber IS NOT NULL####\n")
    report.write("-The number of NRBTSs with NRCTRLTSs containing Cell Trace Feature correctly active:" + str(len(NRBTS_w_Trc_active['NRBTS_distName'].unique())) + "\n")
    if not NRBTS_without_Trc_active.empty: 
        report.write("-The number of NRBTSs with NRCTRLTSs WITHOUT containing Cell Trace Feature correctly active:" + str(len(NRBTS_without_Trc_active)) + "\n")		
        report.write(NRBTS_without_Trc_active.to_string(header=False) + "\n \n")
				
    #PCMD
    if PCMD == True:	
        report.write("\n#################################PCMD Feature##########################################\n")
        report.write("########NRBTS:actPCMDReport-1(true); NRMTRACECU:actPCMDReporting-1(true); NRMTRACEDU:actPCMDReporting-1(true); #########\n")	
        report.write("-The number of NRBTSs with NRCTRLTSs containing PCMD Feature correctly active: " + str(len(NRBTS_w_PCMD['NRBTS_distName'].unique())) + "\n")
        if not NRBTS_without_PCMD.empty:
            report.write("-The number of NRBTSs not containg PCMD correctly active: " + str(len(NRBTS_without_PCMD)) + "\n")
            report.write(NRBTS_without_PCMD.to_string() + "\n")		
        if not NRBTS_w_Trc_active_without_PCMD.empty:
            report.write("-The number of NRBTSs with Trace Configuration active not containg PCMD: " + str(len(NRBTS_w_Trc_active_without_PCMD)) + "\n")	
            report.write(NRBTS_w_Trc_active_without_PCMD.to_string() + "\n")	
		
			
#################Nokia Network reporting - the most important parameters in the network###########################

def Output_Nokia_Network_report(MRBTS, LNBTS, LNCEL, LNCEL_FDD, LNCEL_TDD, NRBTS, NRCELL, NRCELLGRP, LTEENB, NRCELL_FDD, TRACKINGAREA, RNC, WBTS, WCEL, base_dir, AoI_cell_list):
    if not LNCEL.empty:
        Output_Nokia_4G_Network_report(MRBTS, LNBTS, LNCEL, LNCEL_FDD, LNCEL_TDD, base_dir, AoI_cell_list)
    else:
        print("There's no LNCEL list!")

    if not NRCELL.empty:
        Output_Nokia_5G_Network_report(MRBTS, NRBTS, NRCELL, NRCELLGRP, LTEENB, NRCELL_FDD, TRACKINGAREA, base_dir, AoI_cell_list)
    else:
        print("There's no NRCELL list!")

    if not WCEL.empty:
        Output_Nokia_3G_Network_report(RNC, WBTS, WCEL, base_dir, AoI_cell_list)
    else:
        print("There's no WCEL list!")

		
################# Nokia 4G Network reporting - the most important parameters in the network###########################

def Output_Nokia_4G_Network_report(MRBTS, LNBTS, LNCEL, LNCEL_FDD, LNCEL_TDD, base_dir, AoI_cell_list):

    cell_list = LNCEL.merge(MRBTS, left_on=['MRBTS'], right_on=['MRBTS'], how = 'left').merge(LNBTS, left_on=['MRBTS', 'LNBTS'], right_on=['MRBTS', 'LNBTS'], how = 'left')

    if not AoI_cell_list.empty:
        cell_list['eutraCelId'] = cell_list['eutraCelId'].astype('int64')
        cell_list = cell_list.merge(AoI_cell_list, left_on=['eutraCelId'], right_on=['eUtranCellId'], how = 'inner')
		
    if not LNCEL_FDD.empty:
        cell_list = cell_list.merge(LNCEL_FDD, left_on=['MRBTS', 'LNBTS', 'LNCEL'], right_on=['MRBTS', 'LNBTS', 'LNCEL'], how = 'left')
        cell_list = cell_list.drop(columns=['LNCEL_FDD_distName'])       
    if not LNCEL_TDD.empty:
        cell_list = cell_list.merge(LNCEL_TDD, left_on=['MRBTS', 'LNBTS', 'LNCEL'], right_on=['MRBTS', 'LNBTS', 'LNCEL'], how = 'left')
        cell_list = cell_list.drop(columns=['LNCEL_TDD_distName']) 		
   
    cell_list = cell_list.drop(columns=['MRBTS', 'LNBTS', 'LNCEL', 'MRBTS_distName', 'LNBTS_distName', 'actCellTrace', 'actMDTCellTrace', 'actUeThroughputMeas'])

    network_file = base_dir + 'Nokia_4G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)
	
################# Nokia 5G Network reporting - the most important parameters in the network###########################	

def Output_Nokia_5G_Network_report(MRBTS, NRBTS, NRCELL, NRCELLGRP, LTEENB, NRCELL_FDD, TRACKINGAREA, base_dir, AoI_cell_list):

    if not MRBTS.empty:
        cell_list = NRCELL.merge(MRBTS, left_on=['MRBTS'], right_on=['MRBTS'], how = 'left').merge(NRBTS, left_on=['MRBTS', 'NRBTS'], right_on=['MRBTS', 'NRBTS'], how = 'left')
    else:
        cell_list = NRCELL.merge(NRBTS, left_on=['MRBTS', 'NRBTS'], right_on=['MRBTS', 'NRBTS'], how = 'left')        	

    if not NRCELLGRP.empty:
        cell_list = cell_list.merge(NRCELLGRP, left_on=['MRBTS', 'NRBTS', 'NRCELL'], right_on=['MRBTS', 'NRBTS', 'nrCellList'], how = 'left')
        cell_list = cell_list.drop(columns=['NRCELLGRP_distName'])   	

    if not NRCELL_FDD.empty:
        cell_list = cell_list.merge(NRCELL_FDD, left_on=['MRBTS', 'NRBTS', 'NRCELL'], right_on=['MRBTS', 'NRBTS', 'NRCELL'], how = 'left')
        cell_list = cell_list.drop(columns=['NRCELL_FDD_distName'])

    if not TRACKINGAREA.empty:
        cell_list = cell_list.merge(TRACKINGAREA, left_on=['MRBTS', 'NRBTS'], right_on=['MRBTS', 'NRBTS'], how = 'left')
        cell_list = cell_list.drop(columns=['TRACKINGAREA_distName'])		

#    if not LTEENB.empty:
#        cell_list = cell_list.merge(LTEENB, left_on=['MRBTS', 'NRBTS', 'NRCELL'], right_on=['MRBTS', 'NRBTS', 'nrCellList'], how = 'left')
#        cell_list = cell_list.drop(columns=['NRCELLGRP_distName'])   	
		
    cell_list = cell_list.drop(columns=['MRBTS', 'NRBTS', 'NRCELL', 'NRBTS_distName', 'trackingAreaDN', 'TRACKINGAREA','actCellTraceReport', 'actPCMDReport'])

    network_file = base_dir + 'Nokia_5G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)

################# Nokia 3G Network reporting - the most important parameters in the network###########################	

def Output_Nokia_3G_Network_report(RNC, WBTS, WCEL, base_dir, AoI_cell_list):

    
    cell_list = WCEL.merge(RNC, left_on=['RNC'], right_on=['RNC'], how = 'left').merge(WBTS, left_on=['RNC', 'WBTS'], right_on=['RNC','WBTS'], how = 'left')
		
    cell_list = cell_list.drop(columns=['RNC_distName', 'WBTS_distName', 'RNC', 'WBTS', 'WCEL'])

    network_file = base_dir + 'Nokia_3G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)


	
#########################################Ericsson extract MOs################################################
	
def extract_df_Ericsson_MOs(MOs_struct, items, output_base, dir_CSVs, report_option):
    #initialize:
    ENodeBFunction  = pd.DataFrame()
    EUtranCellFDD   = pd.DataFrame()	
    PmUeMeasControl = pd.DataFrame()
    PmEventService  = pd.DataFrame()
    FeatureState    = pd.DataFrame()	
    OptionalFeatureLicense = pd.DataFrame()
    ReportConfigEUtraIntraFreqPm = pd.DataFrame()
    GNBDUFunction = pd.DataFrame() 
    NRCellDU = pd.DataFrame()
    NRSectorCarrier = pd.DataFrame()		
    GNBCUCPFunction = pd.DataFrame()	
    NRCellCU = pd.DataFrame()
    RncFunction = pd.DataFrame()
    IubLink = pd.DataFrame()
    UtranCell = pd.DataFrame()	
    LocationArea = pd.DataFrame()
    RoutingArea = pd.DataFrame()
    ServiceArea = pd.DataFrame()
    Ura = pd.DataFrame()
	
    for j in MOs_struct:
        #LTE	
        if j == "ENodeBFunction":
                ENodeBFunction = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'eNBId'])
                ENodeBFunction = ENodeBFunction.rename(columns = {'SubNetwork_Id_1':'enb_SubNetwork_Id_1', 'SubNetwork_Id_2':'enb_SubNetwork_Id_2', 'MeContext_Id':'enb_MeContext_Id', 'ManagedElement_Id':'enb_ManagedElement_Id', 'vsENodeBFunction_Id':'enb_vsENodeBFunction_Id'})				
        if j == "EUtranCellFDD":
                EUtranCellFDD = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'vsEUtranCellFDD_Id', 'cellId', 'tac', 'physicalLayerCellIdGroup', 'physicalLayerSubCellId', 'earfcndl', 'earfcnul', 'administrativeState', 'dlChannelBandwidth', 'dl256QamEnabled'])
                EUtranCellFDD = EUtranCellFDD.rename(columns = {'SubNetwork_Id_1':'cell_SubNetwork_Id_1', 'SubNetwork_Id_2':'cell_SubNetwork_Id_2', 'MeContext_Id':'cell_MeContext_Id', 'ManagedElement_Id':'cell_ManagedElement_Id', 'vsENodeBFunction_Id':'cell_vsENodeBFunction_Id', 'vsEUtranCellFDD_Id':'cell_vsEUtranCellFDD_Id'})				
        #5G
        if j == "GNBDUFunction":
                GNBDUFunction = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'MeContext_Id', 'ManagedElement_Id', 'vsGNBDUFunction_Id', 'gNBDUName', 'gNBId', 'dUpLMNId_mcc', 'dUpLMNId_mnc'])
                GNBDUFunction = GNBDUFunction.rename(columns = {'SubNetwork_Id_1':'GNBDUFunction_SubNetwork_Id_1', 'MeContext_Id':'GNBDUFunction_MeContext_Id', 'ManagedElement_Id':'GNBDUFunction_ManagedElement_Id', 'vsGNBDUFunction_Id':'GNBDUFunction_vsGNBDUFunction_Id'})				
        if j == "NRCellDU":	
                NRCellDU = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'MeContext_Id', 'ManagedElement_Id', 'vsGNBDUFunction_Id', 'vsNRCellDU_Id', 'pLMNIdList_mcc', 'pLMNIdList_mnc', 'nCI', 'cellLocalId', 'nRPCI', 'bandList', 'bandListManual', 'cellRange', 'cellReservedForOperator', 'cellBarred', 'cellState', 'operationalState', 'configuredEpsTAC', 'serviceState', 'nRTAC', 'pMax', 'rachRootSequence', 'subCarrierSpacing', 'ssbGscn'])
                NRCellDU = NRCellDU.rename(columns = {'SubNetwork_Id_1':'NRCellDU_SubNetwork_Id_1', 'MeContext_Id':'NRCellDU_MeContext_Id', 'ManagedElement_Id':'NRCellDU_ManagedElement_Id', 'vsGNBDUFunction_Id':'NRCellDU_vsGNBDUFunction_Id'})				
        if j == "NRSectorCarrier":
                NRSectorCarrier = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'MeContext_Id', 'ManagedElement_Id', 'vsGNBDUFunction_Id', 'reservedBy', 'arfcnDL', 'arfcnUL', 'bSChannelBwDL', 'bSChannelBwUL', 'frequencyDL', 'frequencyUL', 'noOfRxAntennas', 'noOfTxAntennas', 'noOfUsedRxAntennas', 'noOfUsedTxAntennas'])
                NRSectorCarrier = NRSectorCarrier.rename(columns = {'SubNetwork_Id_1':'NRSectorCarrier_SubNetwork_Id_1', 'MeContext_Id':'NRSectorCarrier_MeContext_Id', 'ManagedElement_Id':'NRSectorCarrier_ManagedElement_Id'})				
        if j == "GNBCUCPFunction":
                GNBCUCPFunction = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'MeContext_Id', 'ManagedElement_Id', 'vsGNBCUCPFunction_Id', 'gNBCUName', 'gNBId', 'pLMNId_mcc', 'pLMNId_mnc'])
                GNBCUCPFunction = GNBCUCPFunction.rename(columns = {'SubNetwork_Id_1':'GNBCUCP_SubNetwork_Id_1', 'MeContext_Id':'GNBCUCP_MeContext_Id', 'ManagedElement_Id':'GNBCUCP_ManagedElement_Id', 'vsGNBCUCPFunction_Id':'GNBCUCP_vsGNBCUCPFunction_Id'})				
        if j == "NRCellCU":
                NRCellCU = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'MeContext_Id', 'ManagedElement_Id', 'vsGNBCUCPFunction_Id', 'vsNRCellCU_Id', 'primaryPLMNId_mcc', 'primaryPLMNId_mnc', 'nCI', 'cellLocalId', 'cellState', 'serviceState', 'nRTAC', 'pLMNIdList_mnc', 'pLMNIdList_mcc'])
                NRCellCU = NRCellCU.rename(columns = {'SubNetwork_Id_1':'NRCellCU_SubNetwork_Id_1', 'MeContext_Id':'NRCellCU_MeContext_Id', 'ManagedElement_Id':'NRCellCU_ManagedElement_Id', 'vsGNBCUCPFunction_Id':'NRCellCU_vsGNBCUCPFunction_Id'})				
        #3G
        if j == "RncFunction":
                RncFunction = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'rncId', 'rncType'])
                RncFunction = RncFunction.rename(columns = {'SubNetwork_Id_1':'Rnc_SubNetwork_Id_1', 'SubNetwork_Id_2':'Rnc_SubNetwork_Id_2', 'MeContext_Id':'Rnc_MeContext_Id', 'ManagedElement_Id':'Rnc_ManagedElement_Id'})				
        if j == "IubLink":
                IubLink = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsRncFunction_Id', 'vsIubLink_Id', 'IubLinkId', 'rbsId'])
                IubLink = IubLink.rename(columns = {'SubNetwork_Id_1':'Iub_SubNetwork_Id_1', 'SubNetwork_Id_2':'Iub_SubNetwork_Id_2', 'MeContext_Id':'Iub_MeContext_Id', 'ManagedElement_Id':'Iub_ManagedElement_Id', 'vsRncFunction_Id':'Iub_vsRncFunction_Id', 'vsIubLink_Id':'Iub_vsIubLink_Id'})        		
        if j == "UtranCell":
                UtranCell = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsRncFunction_Id', 'vsUtranCell_Id', 'cellReserved', 'UtranCellId', 'operationalState', 'maxPwrMax', 'localCellId','uarfcnUl','primaryScramblingCode','uarfcnDl','administrativeState', 'availabilityStatus', 'qRxLevMin', 'cId', 'antennaPosition', 'antennaPosition_latitudeSign', 'antennaPosition_latitude', 'antennaPosition_longitude', 'locationAreaRef', 'uraRef', 'mocnCellProfileRef', 'serviceAreaRef', 'routingAreaRef','iubLinkRef'])
                UtranCell = UtranCell.rename(columns = {'SubNetwork_Id_1':'cell_SubNetwork_Id_1', 'SubNetwork_Id_2':'cell_SubNetwork_Id_2', 'MeContext_Id':'cell_MeContext_Id', 'ManagedElement_Id':'cell_ManagedElement_Id', 'vsRncFunction_Id':'cell_vsRncFunction_Id', 'vsUtranCell_Id':'cell_vsUtranCell_Id'})				
        if j == "LocationArea":
                LocationArea = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsRncFunction_Id', 'vsLocationArea_Id', 'lac'])
                LocationArea = LocationArea.rename(columns = {'SubNetwork_Id_1':'lac_SubNetwork_Id_1', 'SubNetwork_Id_2':'lac_SubNetwork_Id_2', 'MeContext_Id':'lac_MeContext_Id', 'ManagedElement_Id':'lac_ManagedElement_Id', 'vsRncFunction_Id':'lac_vsRncFunction_Id', 'vsLocationArea_Id':'lac_vsLocationArea_Id'})				
        if j == "RoutingArea":
                RoutingArea = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsRncFunction_Id', 'vsLocationArea_Id', 'vsRoutingArea_Id', 'rac'])
                RoutingArea = RoutingArea.rename(columns = {'SubNetwork_Id_1':'rac_SubNetwork_Id_1', 'SubNetwork_Id_2':'rac_SubNetwork_Id_2', 'MeContext_Id':'rac_MeContext_Id', 'ManagedElement_Id':'rac_ManagedElement_Id', 'vsRncFunction_Id':'rac_vsRncFunction_Id', 'vsLocationArea_Id':'rac_vsLocationArea_Id', 'vsRoutingArea_Id':'rac_vsRoutingArea_Id'})	
        if j == "ServiceArea":
                ServiceArea = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsRncFunction_Id', 'vsLocationArea_Id', 'vsServiceArea_Id'])
                ServiceArea = ServiceArea.rename(columns = {'SubNetwork_Id_1':'sac_SubNetwork_Id_1', 'SubNetwork_Id_2':'sac_SubNetwork_Id_2', 'MeContext_Id':'sac_MeContext_Id', 'ManagedElement_Id':'sac_ManagedElement_Id', 'vsRncFunction_Id':'sac_vsRncFunction_Id', 'vsLocationArea_Id':'sac_vsLocationArea_Id', 'vsServiceArea_Id':'sac_vsServiceArea_Id'})	
        if j == "Ura":
                Ura = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsRncFunction_Id', 'vsUra_Id', 'UraId'])
                Ura = Ura.rename(columns = {'SubNetwork_Id_1':'ura_SubNetwork_Id_1', 'SubNetwork_Id_2':'ura_SubNetwork_Id_2', 'MeContext_Id':'ura_MeContext_Id', 'ManagedElement_Id':'ura_ManagedElement_Id', 'vsRncFunction_Id':'ura_vsRncFunction_Id', 'vsUra_Id':'ura_vsUra_Id'})	

        #filtrar os campos que interessam para os reports			
        if j == "ReportConfigEUtraIntraFreqPm":
            if report_option in (1, 3):
                ReportConfigEUtraIntraFreqPm = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'vsEUtranCellFDD_Id', 'vsUeMeasControl_Id', 'reportAmountPm', 'measSelectionEUtraPm', 'reportIntervalPm', 'reportQuantityPm', 'maxReportCellsPm'])
                ReportConfigEUtraIntraFreqPm = ReportConfigEUtraIntraFreqPm.rename(columns = {'SubNetwork_Id_1':'Rep_SubNetwork_Id_1', 'SubNetwork_Id_2':'Rep_SubNetwork_Id_2', 'MeContext_Id':'Rep_MeContext_Id', 'ManagedElement_Id':'Rep_ManagedElement_Id', 'vsENodeBFunction_Id':'Rep_vsENodeBFunction_Id', 'vsEUtranCellFDD_Id':'Rep_vsEUtranCellFDD_Id', 'vsUeMeasControl_Id':'Rep_vsUeMeasControl_Id'})
        if j == "PmUeMeasControl":
            if report_option in (1, 3):
                PmUeMeasControl = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'vsEUtranCellFDD_Id', 'vsUeMeasControl_Id', 'ueMeasIntraFreq1_reportConfigEUtraIntraFreqPmRef', 'ueMeasIntraFreq1_eutranFrequencyRef', 'ueMeasIntraFreq2_reportConfigEUtraIntraFreqPmRef', 'ueMeasIntraFreq2_eutranFrequencyRef'])
        if j == "PmEventService":
            if report_option in (1, 3):
                PmEventService = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'cellTraceFileSize', 'totalCellTraceStorageSize', 'totalEventStorageSize'])
                #print(PmEventService)
        if j == "FeatureState":
            if report_option in (1, 3):
                FeatureState = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'featureStateId',  'serviceState', 'featureState', 'licenseState'])
                FeatureState = FeatureState.loc[(FeatureState['featureState'] == 'ACTIVATED') & (FeatureState['serviceState'] == 'OPERABLE') & (FeatureState['featureStateId'] == "CXC4010717")].drop(columns=['featureStateId',  'serviceState', 'featureState', 'licenseState'])		    
                #print(FeatureState)
        if j == "OptionalFeatureLicense":
            if report_option in (1, 3):
                OptionalFeatureLicense = pd.DataFrame([x for x in items if x['vsDataType'] == j], columns=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'featureStateId',  'serviceState', 'featureState', 'licenseState', 'keyId'])		    
                OptionalFeatureLicense = OptionalFeatureLicense.loc[(OptionalFeatureLicense['featureState'] == 'ACTIVATED') & (OptionalFeatureLicense['serviceState'] == 'OPERABLE') & (OptionalFeatureLicense['keyId'] == "CXC4010717") & (OptionalFeatureLicense['licenseState'] == "ENABLED")].drop(columns=['featureStateId',  'serviceState', 'featureState', 'licenseState', 'keyId'])
                #print("\n")
                #print(OptionalFeatureLicense)				 
    items = []
    return(ENodeBFunction, EUtranCellFDD, ReportConfigEUtraIntraFreqPm, PmUeMeasControl, PmEventService, FeatureState, OptionalFeatureLicense, GNBDUFunction, NRCellDU, NRSectorCarrier, RncFunction, IubLink, UtranCell, LocationArea, RoutingArea, ServiceArea, Ura)


###############################Ericsson Trace Configuration Analysis##############################
	   
def Ericsson_Traces_Configurations(ENodeBFunction, EUtranCellFDD, ReportConfigEUtraIntraFreqPm, PmUeMeasControl, PmEventService, FeatureState, OptionalFeatureLicense, AoI_cell_list, output_base_dir, MRs):
    
    #check 4G Traces	
    if ENodeBFunction.empty or EUtranCellFDD.empty:
        print("There's no ENodeBFunction or EUtranCellFDD => Not possible to check Trace config for 4G!!!")
    else:
        print("There's ENodeBFunction AND EUtranCellFDD=> Not possible to check Trace config for 4G!!!")	
        AoI_eNodeBs, list_eNodeBs, list_cells, eNodeBs_without_PmEventService, PmEventService_rep_w_right_config, PmEventService_rep_w_wrong_config, CXC4010717_rep, eNodeBs_without_CXC4010717, Cells_without_Traces_config, Traces_config_w_right_config, cell_w_wrong_config, reportIntervalPm = Ericsson_4G_Traces_Configurations(ENodeBFunction, EUtranCellFDD, ReportConfigEUtraIntraFreqPm, PmUeMeasControl, PmEventService, FeatureState, OptionalFeatureLicense, AoI_cell_list, output_base_dir, MRs)
        Output_Ericsson_4G_Trace_reports(output_base_dir, AoI_eNodeBs, list_eNodeBs, AoI_cell_list, list_cells, eNodeBs_without_PmEventService, PmEventService_rep_w_right_config, PmEventService_rep_w_wrong_config, CXC4010717_rep, eNodeBs_without_CXC4010717, Cells_without_Traces_config, Traces_config_w_right_config, cell_w_wrong_config, reportIntervalPm)
		
    ##check 5G Traces	
    #if NRBTS.empty:
    #    print("There's no NRBTS => Probably is not a 5G XML!!!")
    #else:
    #    print("There's NRBTS => Probably is a 5G XML!!!")	
    #    AoI_NRBTS, NRBTS, missing_AoI_NRBTS, NRBTS_without_NRCTRLTSs, NRBTS_w_Trc_active, NRBTS_without_Trc_active, NRBTS_w_PCMD, NRBTS_without_PCMD, NRBTS_w_Trc_active_without_PCMD, NRBTS_w_All_Features, NRBTS_without_All_Features = Nokia_5G_Traces_Configurations(output_base_dir, NRBTS, NRMTRACECU, NRMTRACEDU, TCE, AoI_cell_list, PCMD)
    #    Output_Nokia_5G_Trace_reports(output_base_dir, PCMD, AoI_NRBTS, NRBTS, missing_AoI_NRBTS, NRBTS_without_NRCTRLTSs, NRBTS_w_Trc_active, NRBTS_without_Trc_active, NRBTS_w_PCMD, NRBTS_without_PCMD, NRBTS_w_Trc_active_without_PCMD, NRBTS_w_All_Features, NRBTS_without_All_Features)		
	

##################################Ericsson 4G Traces Analysis####################################################

def Ericsson_4G_Traces_Configurations(ENodeBFunction, EUtranCellFDD, ReportConfigEUtraIntraFreqPm, PmUeMeasControl, PmEventService, FeatureState, OptionalFeatureLicense, AoI_cell_list, output_base_dir, MRs):

    #reportIntervalPm chosen
    if MRs == "10":
        reportIntervalPm = "MS_10240"
    elif MRs == "5":
        reportIntervalPm = "MS_5120"
    elif MRs == "1":
        reportIntervalPm = "MS_1024"	    
		
    #AoI_eNodeBs
    if not AoI_cell_list.empty:	
        AoI_eNodeBs = LNBTS_AoI(AoI_cell_list)
    else:
        AoI_eNodeBs = pd.DataFrame()

    #list of eNodeBes found in network
    list_eNodeBs = ENodeBFunction[['enb_SubNetwork_Id_1', 'enb_SubNetwork_Id_2', 'enb_MeContext_Id', 'enb_ManagedElement_Id', 'enb_vsENodeBFunction_Id', 'eNBId']]
    if not AoI_eNodeBs.empty:
        list_eNodeBs = list_eNodeBs.merge(AoI_eNodeBs, left_on=['eNBId'], right_on=['AoI_MRBTS'], how = 'inner')	
		
    #eutran_cellid
    if not EUtranCellFDD.empty:
        EUtranCell_Id = EUtranCellFDD.merge(ENodeBFunction, left_on=['cell_SubNetwork_Id_1', 'cell_SubNetwork_Id_2', 'cell_MeContext_Id', 'cell_ManagedElement_Id', 'cell_vsENodeBFunction_Id'], right_on=['enb_SubNetwork_Id_1', 'enb_SubNetwork_Id_2', 'enb_MeContext_Id', 'enb_ManagedElement_Id', 'enb_vsENodeBFunction_Id'], how = 'inner')
        EUtranCell_Id["Cell_Id"] = ((EUtranCell_Id["eNBId"].astype(int)*256) + (EUtranCell_Id["cellId"].astype(int))).astype(str)
    else:
        print("MO:EUtranCellFDD not available in the XML file")
        quit()
		
    if not AoI_cell_list.empty:
        list_cells = EUtranCell_Id.merge(AoI_cell_list, left_on=['Cell_Id'], right_on=['eUtranCellId'], how = 'inner').astype(str)	
    else:
        list_cells = EUtranCell_Id.astype(str)		
		
    #check the config files for PMService storage
    if not PmEventService.empty:	
        PmEventService  = PmEventService.merge(ENodeBFunction, left_on=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id'], right_on=['enb_SubNetwork_Id_1', 'enb_SubNetwork_Id_2', 'enb_MeContext_Id', 'enb_ManagedElement_Id', 'enb_vsENodeBFunction_Id'], how = 'inner')		
        PmEventService_rep = PmEventService[["SubNetwork_Id_1", "SubNetwork_Id_2", "MeContext_Id", "eNBId", "cellTraceFileSize", "totalCellTraceStorageSize", "totalEventStorageSize"]] 
    else:
        print("MO:PmEventService not available in the XML file")
    #list of eNodeBs without PmEventService
    eNodeBs_without_PmEventService = list_eNodeBs.merge(PmEventService_rep, left_on=['eNBId'], right_on=['eNBId'], how = 'left', indicator = True)
    eNodeBs_without_PmEventService = eNodeBs_without_PmEventService[eNodeBs_without_PmEventService['_merge']=='left_only']	
    #list of eNodeBs with PmEventService correcly and wrongly configured	
    PmEventService_rep_w_right_config = PmEventService_rep[(PmEventService_rep['cellTraceFileSize'] == "20000") & (PmEventService_rep['totalEventStorageSize'] == "266000") & (PmEventService_rep['totalCellTraceStorageSize'] == "250000")]
    PmEventService_rep_w_wrong_config = list_eNodeBs.merge(PmEventService_rep_w_right_config, left_on=['eNBId'], right_on=['eNBId'], how = 'left', indicator = True)
    PmEventService_rep_w_wrong_config = PmEventService_rep_w_wrong_config[PmEventService_rep_w_wrong_config['_merge']=='left_only']
    PmEventService_rep_w_wrong_config = PmEventService_rep_w_wrong_config['eNBId']
    PmEventService_rep_w_wrong_config = PmEventService_rep.merge(PmEventService_rep_w_wrong_config, left_on=['eNBId'], right_on=['eNBId'], how = 'inner') 
	
    #check the list of eNodeBs with CXC4010717 active
    CXC4010717_rep = pd.DataFrame()			
    if not FeatureState.empty:
        if not OptionalFeatureLicense.empty:
            FeatureState  = FeatureState.merge(ENodeBFunction, left_on=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id'], right_on=['enb_SubNetwork_Id_1', 'enb_SubNetwork_Id_2', 'enb_MeContext_Id', 'enb_ManagedElement_Id'], how = 'inner')		
            FeatureState  = FeatureState[['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id',  "eNBId"]]
            OptionalFeatureLicense  = OptionalFeatureLicense.merge(ENodeBFunction, left_on=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id'], right_on=['enb_SubNetwork_Id_1', 'enb_SubNetwork_Id_2', 'enb_MeContext_Id', 'enb_ManagedElement_Id'], how = 'inner')		   	
            OptionalFeatureLicense  = OptionalFeatureLicense[['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id',  "eNBId"]]
            CXC4010717_rep = pd.concat([FeatureState, OptionalFeatureLicense]).drop_duplicates()	
            eNodeBs_without_CXC4010717 = list_eNodeBs.merge(CXC4010717_rep, left_on=['eNBId'], right_on=['eNBId'], how = 'left', indicator = True)
            eNodeBs_without_CXC4010717 = eNodeBs_without_CXC4010717[eNodeBs_without_CXC4010717['_merge']=='left_only']
        else:
            eNodeBs_without_CXC4010717 = list_eNodeBs

    #Merge MO dataframes - Traces
    cell_filter = list_cells[["Cell_Id", "eNBId"]].rename(columns={"Cell_Id":"Cell", "eNBId":"eNB"})	
    Traces_config = PmUeMeasControl.merge(ReportConfigEUtraIntraFreqPm, left_on=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'vsEUtranCellFDD_Id', 'vsUeMeasControl_Id'], right_on=['Rep_SubNetwork_Id_1', 'Rep_SubNetwork_Id_2', 'Rep_MeContext_Id', 'Rep_ManagedElement_Id', 'Rep_vsENodeBFunction_Id', 'Rep_vsEUtranCellFDD_Id', 'Rep_vsUeMeasControl_Id'], how = 'inner').merge(EUtranCell_Id, left_on=['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'vsEUtranCellFDD_Id'], right_on=['cell_SubNetwork_Id_1', 'cell_SubNetwork_Id_2', 'cell_MeContext_Id', 'cell_ManagedElement_Id', 'cell_vsENodeBFunction_Id', 'cell_vsEUtranCellFDD_Id'], how = 'inner')
    Traces_config = Traces_config[['SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'vsEUtranCellFDD_Id', 'eNBId', 'Cell_Id', 'reportAmountPm', 'measSelectionEUtraPm', 'reportIntervalPm', 'reportQuantityPm', 'maxReportCellsPm', 'ueMeasIntraFreq1_reportConfigEUtraIntraFreqPmRef', 'ueMeasIntraFreq1_eutranFrequencyRef', 'ueMeasIntraFreq2_reportConfigEUtraIntraFreqPmRef', 'ueMeasIntraFreq2_eutranFrequencyRef']].astype(str)		
    Traces_config = Traces_config.merge(cell_filter, left_on=['Cell_Id'], right_on=['Cell'], how = 'inner').drop(["Cell", "eNB"], axis = 1)		
    #Traces w right config
    Traces_config_w_right_config = Traces_config[(Traces_config["maxReportCellsPm"] == "8")& (Traces_config["measSelectionEUtraPm"] == "PERIODICAL") & (Traces_config["reportAmountPm"] == "0") & (Traces_config["reportIntervalPm"] == reportIntervalPm) & (Traces_config["reportQuantityPm"] == "BOTH")]
    Traces_config_w_right_config = Traces_config_w_right_config[(Traces_config_w_right_config['ueMeasIntraFreq2_reportConfigEUtraIntraFreqPmRef'].notnull() & Traces_config_w_right_config['ueMeasIntraFreq2_eutranFrequencyRef'].notnull()) | (Traces_config_w_right_config['ueMeasIntraFreq1_reportConfigEUtraIntraFreqPmRef'].notnull() & Traces_config_w_right_config['ueMeasIntraFreq1_eutranFrequencyRef'].notnull())]
	#list of cells without Trace configuration
    Cells_without_Traces_config = cell_filter.merge(Traces_config, left_on=['Cell'], right_on=['Cell_Id'], how = 'left', indicator = True)
    Cells_without_Traces_config = Cells_without_Traces_config[Cells_without_Traces_config['_merge']=='left_only']
    Cells_without_Traces_config = Cells_without_Traces_config[['eNB', 'Cell']].merge(EUtranCell_Id, left_on=["Cell"], right_on = ["Cell_Id"], how="left").rename(columns={"cell_SubNetwork_Id_1":"SubNetwork_Id_1", "cell_SubNetwork_Id_2":"SubNetwork_Id_2", "cell_MeContext_Id":"MeContext_Id", "cell_ManagedElement_Id":"ManagedElement_Id", "cell_vsENodeBFunction_Id":"vsENodeBFunction_Id", "cell_vsEUtranCellFDD_Id":"vsEUtranCellFDD_Id"})
    Cells_without_Traces_config = Cells_without_Traces_config[['eNB', 'Cell', 'SubNetwork_Id_1', 'SubNetwork_Id_2', 'MeContext_Id', 'ManagedElement_Id', 'vsENodeBFunction_Id', 'vsEUtranCellFDD_Id']].rename(columns={"Cell":"Cell_Id", "eNB":"eNB_Id"}) 	
    #list of cells with Trace configuration wrong
    cell_w_wrong_config = Traces_config[(Traces_config.Cell_Id.isin(Cells_without_Traces_config.Cell_Id) == False) & (Traces_config.Cell_Id.isin(Traces_config_w_right_config.Cell_Id) == False)]
	
    return (AoI_eNodeBs, list_eNodeBs, list_cells, eNodeBs_without_PmEventService, PmEventService_rep_w_right_config, PmEventService_rep_w_wrong_config, CXC4010717_rep, eNodeBs_without_CXC4010717, Cells_without_Traces_config, Traces_config_w_right_config, cell_w_wrong_config, reportIntervalPm)
	
##################################Ericsson Outpu Traces analysis####################################################	
	
def Output_Ericsson_4G_Trace_reports(output_base_dir, AoI_eNodeBs, list_eNodeBs, AoI_cell_list, list_cells, eNodeBs_without_PmEventService, PmEventService_rep_w_right_config, PmEventService_rep_w_wrong_config, CXC4010717_rep, eNodeBs_without_CXC4010717, Cells_without_Traces_config, Traces_config_w_right_config, cell_w_wrong_config, reportIntervalPm):
	#write_outfile_report
    report = open(output_base_dir + "Ericsson_4G_Trace_report.txt", "w")
    if not AoI_eNodeBs.empty:
        report.write("-Number eNodeBs expected in the network (AoI list): "+ str(len(AoI_eNodeBs))  +" eNodeBs. \n") 
    report.write("-Number of eNodeBs found in the network "+ str(len(list_eNodeBs))  +" eNodeBs. \n")   
    if not AoI_cell_list.empty:
        report.write("-Number cells expected in the network (AoI list): "+ str(len(AoI_cell_list))  +" cells. \n") 
    report.write("-Number of cells found in the network "+ str(len(list_cells))  +" cells. \n \n")
	
    #list eNodeBs with or without CXC4010717 active
    report.write("-Number of eNodeBs with CXC4010717 license active: "+ str(len(CXC4010717_rep))  +" eNodeBs. \n")	
    report.write("-Number of eNodeBs without CXC4010717 licenses: "+ str(len(eNodeBs_without_CXC4010717))  +" eNodeBs. \n")	
    if not eNodeBs_without_PmEventService.empty:
        report.write(eNodeBs_without_PmEventService.to_string() + "\n ")		
	
	#Writing FileSizes
    report.write("\nParameters related with PmEventService: cellTraceFileSize=20000; totalEventStorageSize=266000; totalCellTraceStorageSize=250000\n")
    report.write("-Number of eNodeBs without PmEventService entries: "+ str(len(eNodeBs_without_PmEventService))  +" eNodeBs. \n")
    if not eNodeBs_without_PmEventService.empty:	
        report.write(eNodeBs_without_PmEventService.to_string() + "\n") 	
    report.write("-Number of eNodeBs with the correct configuration for the PmEventService: "+ str(len(PmEventService_rep_w_right_config))  +" eNodeBs. \n")	
    report.write("-Number of eNodeBs with the wrong configuration for the PmEventService: "+ str(len(PmEventService_rep_w_wrong_config))  +" eNodeBs. \n")
    if not PmEventService_rep_w_wrong_config.empty:
        report.write(PmEventService_rep_w_wrong_config.to_string() + "\n ") 

    #Trace configuration
    report.write("\nParameters related with the Cell Trace Configuration: maxReportCellsPm=8; measSelectionEUtraPm=PERIODICAL; reportAmountPm=0; reportIntervalPm =" + reportIntervalPm + "; reportQuantityPm=BOTH;  \n")
    report.write("NOTE: at least one of two pairs: (ueMeasIntraFreq1_reportConfigEUtraIntraFreqPmRef + ueMeasIntraFreq1_eutranFrequencyRef) OR (ueMeasIntraFreq2_reportConfigEUtraIntraFreqPmRef + ueMeasIntraFreq2_eutranFrequencyRef) should be active \n")	
    report.write("-Number of cells without Trace configuration:"+ str(len(Cells_without_Traces_config))  +" cells. \n")
    if not Cells_without_Traces_config.empty:
        report.write(Cells_without_Traces_config.to_string() + "\n \n")        	
    report.write("-Number of cells with Trace configuration correctly actived:"+ str(len(Traces_config_w_right_config))  +" cells. \n")	
    report.write("-Number of cells with Trace configuration not correctly actived:"+ str(len(cell_w_wrong_config))  +" cells. \n")
    if not cell_w_wrong_config.empty: 
        report.write(cell_w_wrong_config.to_string() + "\n \n")
		
#################Ericsson Network reporting - the most important parameters in the network###########################

def Output_Ericsson_Network_report(ENodeBFunction, EUtranCellFDD, GNBDUFunction, NRCellDU, NRSectorCarrier, RncFunction, IubLink, UtranCell, LocationArea, RoutingArea, ServiceArea, Ura, base_dir, AoI_cell_list):
    if not EUtranCellFDD.empty:
        Output_Ericsson_4G_Network_report(ENodeBFunction, EUtranCellFDD, base_dir, AoI_cell_list)
    else:
        print("There's no EUtranCellFDD list!")

    if not NRCellDU.empty:
        Output_Ericsson_5G_Network_report(GNBDUFunction, NRCellDU, NRSectorCarrier, base_dir, AoI_cell_list)
    else:
        print("There's no NRCellDU list!")

    if not UtranCell.empty:
        Output_Ericsson_3G_Network_report(RncFunction, IubLink, UtranCell, LocationArea, RoutingArea, ServiceArea, Ura, base_dir, AoI_cell_list)
    else:
        print("There's no UtranCell list!")

def Output_Ericsson_4G_Network_report(ENodeBFunction, EUtranCellFDD, base_dir, AoI_cell_list):
    if not EUtranCellFDD.empty:
        cell_list = EUtranCellFDD.merge(ENodeBFunction, left_on=['cell_SubNetwork_Id_1', 'cell_SubNetwork_Id_2', 'cell_MeContext_Id', 'cell_ManagedElement_Id', 'cell_vsENodeBFunction_Id'], right_on=['enb_SubNetwork_Id_1', 'enb_SubNetwork_Id_2', 'enb_MeContext_Id', 'enb_ManagedElement_Id', 'enb_vsENodeBFunction_Id'], how = 'left')
        cell_list["Cell_Id"] = ((cell_list["eNBId"].astype(int)*256) + (cell_list["cellId"].astype(int))).astype(str)
        cell_list["PCI"] = ((cell_list["physicalLayerCellIdGroup"].astype(int)*3) + (cell_list["physicalLayerSubCellId"].astype(int))).astype(str)
        if not AoI_cell_list.empty:
            cell_list = cell_list.merge(AoI_cell_list, left_on=['Cell_Id'], right_on=['eUtranCellId'], how = 'inner')        		
        cell_list = cell_list[['cell_SubNetwork_Id_1', 'cell_SubNetwork_Id_2', 'cell_MeContext_Id', 'cell_ManagedElement_Id', 'cell_vsENodeBFunction_Id', 'cell_vsEUtranCellFDD_Id', 'eNBId', 'cellId', 'Cell_Id', 'PCI', 'tac', 'earfcndl', 'earfcnul', 'administrativeState', 'dlChannelBandwidth', 'dl256QamEnabled']].rename(columns={'cell_SubNetwork_Id_1':'SubNetwork_Id_1', 'cell_SubNetwork_Id_2':'SubNetwork_Id_2', 'cell_MeContext_Id':'MeContext_Id', 'cell_ManagedElement_Id': 'ManagedElement_Id', 'cell_vsENodeBFunction_Id': 'vsENodeBFunction_Id', 'cell_vsEUtranCellFDD_Id':'vsEUtranCellFDD_Id'})
    else:
        print("There's no EUtranCellFDD list!")
		
    network_file = base_dir + 'Ericsson_4G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)

##############
	
def Output_Ericsson_5G_Network_report(GNBDUFunction, NRCellDU, NRSectorCarrier, base_dir, AoI_cell_list):
    if not NRCellDU.empty:
        NRCellDU['NRCellDU_reservedBy'] = 'SubNetwork=' + NRCellDU['NRCellDU_SubNetwork_Id_1'] + ',MeContext=' + NRCellDU['NRCellDU_MeContext_Id'] + ',ManagedElement=' + NRCellDU['NRCellDU_ManagedElement_Id'] + ',vsDataGNBDUFunction=' + NRCellDU['NRCellDU_vsGNBDUFunction_Id'] + ',vsDataNRCellDU=' + NRCellDU['vsNRCellDU_Id']
        cell_list = NRCellDU.merge(GNBDUFunction, left_on=['NRCellDU_SubNetwork_Id_1', 'NRCellDU_MeContext_Id', 'NRCellDU_ManagedElement_Id', 'NRCellDU_vsGNBDUFunction_Id'], right_on=['GNBDUFunction_SubNetwork_Id_1', 'GNBDUFunction_MeContext_Id', 'GNBDUFunction_ManagedElement_Id', 'GNBDUFunction_vsGNBDUFunction_Id'], how = 'left').merge(NRSectorCarrier, left_on=['NRCellDU_reservedBy'], right_on=['reservedBy'], how = 'left')
        cell_list = cell_list.rename(columns={'NRCellDU_SubNetwork_Id_1': 'SubNetwork_Id', 'NRCellDU_MeContext_Id': 'MeContext_Id', 'NRCellDU_ManagedElement_Id':'ManagedElement_Id'})
        cell_list = cell_list.drop(columns=['GNBDUFunction_SubNetwork_Id_1', 'GNBDUFunction_MeContext_Id', 'GNBDUFunction_ManagedElement_Id', 'GNBDUFunction_vsGNBDUFunction_Id', 'NRCellDU_vsGNBDUFunction_Id', 'vsNRCellDU_Id', 'NRCellDU_reservedBy', 'reservedBy', 'NRSectorCarrier_SubNetwork_Id_1', 'NRSectorCarrier_MeContext_Id', 'NRSectorCarrier_ManagedElement_Id', 'vsGNBDUFunction_Id'])
        #print(cell_list)
        #print(list(cell_list))	

        if not AoI_cell_list.empty:
            cell_list = cell_list.merge(AoI_cell_list, left_on=['nCI'], right_on=['eUtranCellId'], how = 'inner')        		
    else:
        print("There's no NRCellDU list!")
		
    network_file = base_dir + 'Ericsson_5G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)

#############
	
def Output_Ericsson_3G_Network_report(RncFunction, IubLink, UtranCell, LocationArea, RoutingArea, ServiceArea, Ura, base_dir, AoI_cell_list):
    if not UtranCell.empty:
        cell_list = UtranCell.merge(RncFunction, left_on=['cell_SubNetwork_Id_1', 'cell_SubNetwork_Id_2', 'cell_MeContext_Id', 'cell_ManagedElement_Id'], right_on=['Rnc_SubNetwork_Id_1', 'Rnc_SubNetwork_Id_2', 'Rnc_MeContext_Id', 'Rnc_ManagedElement_Id'], how = 'left')
        cell_list = cell_list.drop(columns=['Rnc_SubNetwork_Id_1', 'Rnc_SubNetwork_Id_2', 'Rnc_MeContext_Id', 'Rnc_ManagedElement_Id'])
        if not IubLink.empty:
            IubLink['IubLink_temp'] = 'SubNetwork=' + IubLink['Iub_SubNetwork_Id_1'] + ',SubNetwork=' + IubLink['Iub_SubNetwork_Id_2'] + ',MeContext=' + IubLink['Iub_MeContext_Id'] + ',ManagedElement=' + IubLink['Iub_ManagedElement_Id'] + ',vsDataRncFunction=' + IubLink['Iub_vsRncFunction_Id'] + ',vsDataIubLink='+ IubLink['Iub_vsIubLink_Id']         		
            cell_list = cell_list.merge(IubLink, left_on=['iubLinkRef'], right_on=['IubLink_temp'], how = 'left')
            cell_list = cell_list.drop(columns=['Iub_SubNetwork_Id_1', 'Iub_SubNetwork_Id_2', 'Iub_MeContext_Id', 'Iub_ManagedElement_Id', 'Iub_vsRncFunction_Id', 'Iub_vsIubLink_Id', 'IubLink_temp', 'iubLinkRef'])	
        if not LocationArea.empty:
            LocationArea['LocationArea_temp'] = 'SubNetwork=' + LocationArea['lac_SubNetwork_Id_1'] + ',SubNetwork=' + LocationArea['lac_SubNetwork_Id_2'] + ',MeContext=' + LocationArea['lac_MeContext_Id'] + ',ManagedElement=' + LocationArea['lac_ManagedElement_Id'] + ',vsDataRncFunction=' + LocationArea['lac_vsRncFunction_Id'] + ',vsDataLocationArea='+ LocationArea['lac_vsLocationArea_Id']        		
            cell_list = cell_list.merge(LocationArea, left_on=['locationAreaRef'], right_on=['LocationArea_temp'], how = 'left')
            cell_list = cell_list.drop(columns=['lac_SubNetwork_Id_1', 'lac_SubNetwork_Id_2', 'lac_MeContext_Id', 'lac_ManagedElement_Id', 'lac_vsRncFunction_Id','LocationArea_temp', 'locationAreaRef', 'lac_vsLocationArea_Id'])	
        if not RoutingArea.empty:
            RoutingArea['RoutingArea_temp'] = 'SubNetwork=' + RoutingArea['rac_SubNetwork_Id_1'] + ',SubNetwork=' + RoutingArea['rac_SubNetwork_Id_2'] + ',MeContext=' + RoutingArea['rac_MeContext_Id'] + ',ManagedElement=' + RoutingArea['rac_ManagedElement_Id'] + ',vsDataRncFunction=' + RoutingArea['rac_vsRncFunction_Id'] + ',vsDataLocationArea='+ RoutingArea['rac_vsLocationArea_Id'] + ',vsDataRoutingArea='+ RoutingArea['rac_vsRoutingArea_Id']       		
            cell_list = cell_list.merge(RoutingArea, left_on=['routingAreaRef'], right_on=['RoutingArea_temp'], how = 'left')
            cell_list = cell_list.drop(columns=['rac_SubNetwork_Id_1', 'rac_SubNetwork_Id_2', 'rac_MeContext_Id', 'rac_ManagedElement_Id', 'rac_vsRncFunction_Id', 'rac_vsLocationArea_Id', 'rac_vsRoutingArea_Id', 'RoutingArea_temp', 'routingAreaRef'])			
        if not ServiceArea.empty:
            ServiceArea['ServiceArea_temp'] = 'SubNetwork=' + ServiceArea['sac_SubNetwork_Id_1'] + ',SubNetwork=' + ServiceArea['sac_SubNetwork_Id_2'] + ',MeContext=' + ServiceArea['sac_MeContext_Id'] + ',ManagedElement=' + ServiceArea['sac_ManagedElement_Id'] + ',vsDataRncFunction=' + ServiceArea['sac_vsRncFunction_Id'] + ',vsDataLocationArea='+ ServiceArea['sac_vsLocationArea_Id'] + ',vsDataServiceArea='+ ServiceArea['sac_vsServiceArea_Id']       		
            cell_list = cell_list.merge(ServiceArea, left_on=['serviceAreaRef'], right_on=['ServiceArea_temp'], how = 'left')
            cell_list = cell_list.drop(columns=['sac_SubNetwork_Id_1', 'sac_SubNetwork_Id_2', 'sac_MeContext_Id', 'sac_ManagedElement_Id', 'sac_vsRncFunction_Id', 'sac_vsLocationArea_Id', 'sac_vsServiceArea_Id', 'ServiceArea_temp', 'serviceAreaRef'])		
        if not Ura.empty:
            Ura['Ura_temp'] = 'SubNetwork=' + Ura['ura_SubNetwork_Id_1'] + ',SubNetwork=' + Ura['ura_SubNetwork_Id_2'] + ',MeContext=' + Ura['ura_MeContext_Id'] + ',ManagedElement=' + Ura['ura_ManagedElement_Id'] + ',vsDataRncFunction=' + Ura['ura_vsRncFunction_Id'] +  ',vsDataUra='+ Ura['ura_vsUra_Id']      		
            cell_list = cell_list.merge(Ura, left_on=['uraRef'], right_on=['Ura_temp'], how = 'left')
            cell_list = cell_list.drop(columns=['ura_SubNetwork_Id_1', 'ura_SubNetwork_Id_2', 'ura_MeContext_Id', 'ura_ManagedElement_Id', 'ura_vsRncFunction_Id', 'ura_vsUra_Id', 'Ura_temp', 'uraRef'])		

        cell_list['CGI'] = ((cell_list["rncId"].astype(int)*65536) + (cell_list["localCellId"].astype(int))).astype(str)
        cell_list = cell_list.rename(columns={'cell_SubNetwork_Id_1': 'SubNetwork_Id_1', 'cell_SubNetwork_Id_2': 'SubNetwork_Id_2', 'cell_MeContext_Id': 'MeContext_Id', 'cell_vsUtranCell_Id': 'vsUtranCell_Id'})
        cell_list = cell_list.drop(columns=['cell_vsRncFunction_Id', 'cell_ManagedElement_Id', 'mocnCellProfileRef'])		
        if not AoI_cell_list.empty:
            cell_list = cell_list.merge(AoI_cell_list, left_on=['CGI'], right_on=['eUtranCellId'], how = 'inner')        		
    else:
        print("There's no UtranCell list!")
		
    network_file = base_dir + 'Ericsson_3G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)


#########################################huawei extract MOs################################################
	
def extract_df_Huawei_MOs(MOs_struct, items, output_base, dir_CSVs, report_option):
    #initialize:
    SRAN_ENODEBFUNCTION   = pd.DataFrame()
    SRAN_CELL             = pd.DataFrame()	
    SRAN_CELLOP           = pd.DataFrame()
    SRAN_CNOPERATOR       = pd.DataFrame()
    SRAN_CNOPERATORTA     = pd.DataFrame()	
    UMTS_RNC_UCELL        = pd.DataFrame()
    UMTS_RNC_URNCBASIC    = pd.DataFrame()
    UMTS_RNC_UCNOPERGROUP = pd.DataFrame()
    UMTS_RNC_UCNOPERATOR  = pd.DataFrame()	
    GSM_BSC_GCELL         = pd.DataFrame()
	
    for j in MOs_struct:
        #4G	
        if j == "SRAN_ENODEBFUNCTION":
                SRAN_ENODEBFUNCTION = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid', 'ENODEBFUNCTIONNAME', 'ENODEBID'])
                SRAN_ENODEBFUNCTION = SRAN_ENODEBFUNCTION.rename(columns = {'neid':'ENODEBFUNCTION_neid'})				
        if j == "SRAN_CELL":
                SRAN_CELL = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid', 'netype', 'neversion', 'productversion', 'CELLID', 'LOCALCELLID', 'CELLNAME', 'AIRCELLFLAG', 'CELLACTIVESTATE', 'CELLADMINSTATE', 'PHYCELLID', 'DLBANDWIDTH', 'DLEARFCN', 'FDDTDDIND', 'FREQBAND', 'NBCELLFLAG', 'ROOTSEQUENCEIDX', 'ULEARFCN', 'ULEARFCNCFGIND', 'WORKMODE'])
                SRAN_CELL = SRAN_CELL.rename(columns = {'neid':'CELL_neid', 'LOCALCELLID':'CELL_LOCALCELLID'})
        if report_option in (2, 3):
            if j == "SRAN_CELLOP":
                    SRAN_CELLOP = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid', 'LOCALCELLID', 'TRACKINGAREAID'])
                    SRAN_CELLOP = SRAN_CELLOP.rename(columns = {'neid':'CELLOP_neid', 'LOCALCELLID':'CELLOP_LOCALCELLID', 'TRACKINGAREAID':'CELLOP_TRACKINGAREAID'})
            if j == "SRAN_CNOPERATOR":
                    SRAN_CNOPERATOR = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid', 'CNOPERATORID', 'MCC', 'MNC'])
                    SRAN_CNOPERATOR = SRAN_CNOPERATOR.rename(columns = {'neid':'CNOPERATOR_neid', 'CNOPERATORID':'CNOPERATOR_CNOPERATORID'})
            if j == "SRAN_CNOPERATORTA":
                    SRAN_CNOPERATORTA = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid', 'CNOPERATORID', 'TAC', 'TRACKINGAREAID'])
                    SRAN_CNOPERATORTA = SRAN_CNOPERATORTA.rename(columns = {'neid':'CNOPERATORTA_neid', 'TRACKINGAREAID':'CNOPERATORTA_TRACKINGAREAID', 'CNOPERATORID':'CNOPERATORTA_CNOPERATORID'})
        #3G	
        if j == "UMTS_RNC_UCELL":
                UMTS_RNC_UCELL = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid','ACTSTATUS', 'BLKSTATUS', 'CELLID', 'CELLNAME', 'LAC', 'LOCELL', 'LOGICRNCID', 'NODEBNAME', 'PSCRAMBCODE', 'RAC', 'SAC', 'UARFCNDOWNLINK', 'UARFCNUPLINK', 'CNOPGRPINDEX', 'netype', 'neversion'])
                UMTS_RNC_UCELL = UMTS_RNC_UCELL.rename(columns = {'neid':'UCELL_neid', 'LOGICRNCID':'UCELL_LOGICRNCID', 'CNOPGRPINDEX':'UCELL_CNOPGRPINDEX'})		
        if j == "UMTS_RNC_URNCBASIC":
                UMTS_RNC_URNCBASIC = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid', 'RNCID'])
                UMTS_RNC_URNCBASIC = UMTS_RNC_URNCBASIC.rename(columns = {'neid':'URNCBASIC_neid'})	
        if report_option in (2, 3):
            if j == "UMTS_RNC_UCNOPERGROUP":
                    UMTS_RNC_UCNOPERGROUP = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid', 'LOGICRNCID', 'CNOPGRPINDEX', 'CNOPINDEX1'])
                    UMTS_RNC_UCNOPERGROUP = UMTS_RNC_UCNOPERGROUP.rename(columns = {'neid':'UCNOPERGROUP_neid', 'LOGICRNCID':'UCNOPERGROUP_LOGICRNCID', 'CNOPGRPINDEX': 'UCNOPERGROUP_CNOPGRPINDEX', 'CNOPINDEX1': 'UCNOPERGROUP_CNOPINDEX1'})
            if j == "UMTS_RNC_UCNOPERATOR":
                    UMTS_RNC_UCNOPERATOR = pd.DataFrame([x for x in items if x['MO'] == j], columns=['neid', 'LOGICRNCID', 'CNOPINDEX', 'MCC', 'MNC'])
                    UMTS_RNC_UCNOPERATOR = UMTS_RNC_UCNOPERATOR.rename(columns = {'neid':'UCNOPERATOR_neid', 'LOGICRNCID':'UCNOPERATOR_LOGICRNCID', 'CNOPINDEX': 'UCNOPERATOR_CNOPINDEX'})	

        #2G	
        if j == "GSM_BSC_GCELL":
                GSM_BSC_GCELL = pd.DataFrame([x for x in items if x['MO'] == j], columns=['netype', 'neversion','neid', 'ACTSTATUS','ADMSTAT', 'MCC', 'MNC', 'OPNAME', 'CELLID', 'CI', 'CELLNAME', 'GLOCELLID', 'LAC', 'MOCNCMCELL', 'NCC', 'BCC'])
                GSM_BSC_GCELL = GSM_BSC_GCELL.rename(columns = {'neid':'GCELL_neid'})						
				
    items = []
    return(SRAN_ENODEBFUNCTION, SRAN_CELL, SRAN_CELLOP, SRAN_CNOPERATOR, SRAN_CNOPERATORTA, UMTS_RNC_UCELL, UMTS_RNC_URNCBASIC, UMTS_RNC_UCNOPERGROUP, UMTS_RNC_UCNOPERATOR, GSM_BSC_GCELL)


#################Huawei Network reporting - the most important parameters in the network###########################

def Output_Huawei_Network_report(SRAN_ENODEBFUNCTION, SRAN_CELL, SRAN_CELLOP, SRAN_CNOPERATOR, SRAN_CNOPERATORTA, UMTS_RNC_UCELL, UMTS_RNC_URNCBASIC, UMTS_RNC_UCNOPERGROUP, UMTS_RNC_UCNOPERATOR, GSM_BSC_GCELL, base_dir, AoI_cell_list):
    if not SRAN_CELL.empty:
        Output_Huawei_4G_Network_report(SRAN_ENODEBFUNCTION, SRAN_CELL, SRAN_CELLOP, SRAN_CNOPERATOR, SRAN_CNOPERATORTA, base_dir, AoI_cell_list)
    else:
        print("There's no SRAN_CELL list!")

    #if not NRCELL.empty:
    #    Output_Ericsson_5G_Network_report()
    #else:
    #    print("There's no NRCELL list!")

    if not UMTS_RNC_UCELL.empty:
        Output_Huawei_3G_Network_report(UMTS_RNC_UCELL, UMTS_RNC_URNCBASIC, UMTS_RNC_UCNOPERGROUP, UMTS_RNC_UCNOPERATOR, base_dir, AoI_cell_list)
    else:
        print("There's no UCELL list!")
		
    if not GSM_BSC_GCELL.empty:
        Output_Huawei_2G_Network_report(GSM_BSC_GCELL, base_dir, AoI_cell_list)
    else:
        print("There's no GCELL list!")
	

def Output_Huawei_4G_Network_report(SRAN_ENODEBFUNCTION, SRAN_CELL, SRAN_CELLOP, SRAN_CNOPERATOR, SRAN_CNOPERATORTA, base_dir, AoI_cell_list):
    if not SRAN_CELL.empty:
        cell_list = SRAN_CELL.merge(SRAN_ENODEBFUNCTION, left_on=['CELL_neid'], right_on=['ENODEBFUNCTION_neid'], how = 'left').merge(SRAN_CELLOP, left_on =['CELL_neid', 'CELL_LOCALCELLID'], right_on=['CELLOP_neid', 'CELLOP_LOCALCELLID'], how='left').merge(SRAN_CNOPERATORTA, left_on = ['CELLOP_neid', 'CELLOP_TRACKINGAREAID'], right_on=['CNOPERATORTA_neid', 'CNOPERATORTA_TRACKINGAREAID'], how='left').merge(SRAN_CNOPERATOR, left_on = ['CNOPERATORTA_neid', 'CNOPERATORTA_CNOPERATORID'], right_on=['CNOPERATOR_neid', 'CNOPERATOR_CNOPERATORID'], how='left')
	
#        if not AoI_cell_list.empty:
#            cell_list = cell_list.merge(AoI_cell_list, left_on=['Cell_Id'], right_on=['eUtranCellId'], how = 'inner')        		
        cell_list = cell_list[['CELL_neid', 'netype', 'neversion', 'productversion', 'MCC', 'MNC', 'ENODEBFUNCTIONNAME', 'ENODEBID','CELLID', 'CELL_LOCALCELLID', 'CELLNAME', 'AIRCELLFLAG', 'CELLACTIVESTATE', 'CELLADMINSTATE', 'PHYCELLID', 'DLBANDWIDTH', 'DLEARFCN', 'FDDTDDIND', 'FREQBAND', 'NBCELLFLAG', 'ROOTSEQUENCEIDX', 'ULEARFCN', 'ULEARFCNCFGIND', 'TAC','WORKMODE' ]].rename(columns={'CELL_neid': 'neid', 'CELL_LOCALCELLID': 'LOCALCELLID'})
        print(cell_list.keys())			
    network_file = base_dir + 'Huawei_4G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)


def Output_Huawei_3G_Network_report(UMTS_RNC_UCELL, UMTS_RNC_URNCBASIC, UMTS_RNC_UCNOPERGROUP, UMTS_RNC_UCNOPERATOR, base_dir, AoI_cell_list):
    if not UMTS_RNC_UCELL.empty:
        cell_list = UMTS_RNC_UCELL.merge(UMTS_RNC_URNCBASIC, left_on=['UCELL_neid'], right_on=['URNCBASIC_neid'], how = 'left').merge(UMTS_RNC_UCNOPERGROUP, left_on =['UCELL_neid', 'UCELL_LOGICRNCID', 'UCELL_CNOPGRPINDEX'], right_on=['UCNOPERGROUP_neid', 'UCNOPERGROUP_LOGICRNCID', 'UCNOPERGROUP_CNOPGRPINDEX'], how='left').merge(UMTS_RNC_UCNOPERATOR, left_on = ['UCNOPERGROUP_neid', 'UCNOPERGROUP_LOGICRNCID', 'UCNOPERGROUP_CNOPINDEX1'], right_on=['UCNOPERATOR_neid', 'UCNOPERATOR_LOGICRNCID', 'UCNOPERATOR_CNOPINDEX'], how='left')
 	
#        if not AoI_cell_list.empty:
#            cell_list = cell_list.merge(AoI_cell_list, left_on=['Cell_Id'], right_on=['eUtranCellId'], how = 'inner')        		
        cell_list = cell_list[['netype', 'neversion', 'MCC', 'MNC', 'UCELL_neid', 'UCELL_LOGICRNCID','RNCID', 'NODEBNAME', 'CELLID', 'CELLNAME', 'ACTSTATUS', 'BLKSTATUS', 'LAC', 'LOCELL', 'PSCRAMBCODE', 'RAC', 'SAC', 'UARFCNDOWNLINK', 'UARFCNUPLINK']].rename(columns={'UCELL_neid':'neid', 'UCELL_LOGICRNCID':'LOGICRNCID' })
        print(cell_list.keys())			
    network_file = base_dir + 'Huawei_3G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)
	
	
def Output_Huawei_2G_Network_report(GSM_BSC_GCELL, base_dir, AoI_cell_list):
    if not GSM_BSC_GCELL.empty:
        cell_list = GSM_BSC_GCELL
	
#        if not AoI_cell_list.empty:
#            cell_list = cell_list.merge(AoI_cell_list, left_on=['Cell_Id'], right_on=['eUtranCellId'], how = 'inner')        		
        cell_list = cell_list[['netype', 'neversion','GCELL_neid', 'MCC', 'MNC', 'OPNAME', 'CELLID', 'CI', 'CELLNAME', 'GLOCELLID','ACTSTATUS','ADMSTAT', 'LAC', 'MOCNCMCELL', 'NCC', 'BCC']].rename(columns={'GCELL_neid':'neid'})		
        print(cell_list.keys())	
    network_file = base_dir + 'Huawei_2G_NetworkList.csv'
    cell_list.to_csv(network_file, index=False)

################################General Function for Reporting Part - All vendors################################

def reporting (MOs_struct_Nokia, items_Nokia, flag_Nokia, MOs_struct_Ericsson, items_Ericsson, flag_Ericsson, MOs_struct_Huawei, items_Huawei, flag_Huawei, input, AoI_list):
    #Nokia
    if flag_Nokia:
	    #daframe_MOs
        LNBTS, CTRLTS, MTRACE, MRBTS, LNCEL, LNCEL_FDD, LNCEL_TDD, NRBTS, LTEENB, NRCELL, NRCELLGRP, LTEENB, NRMTRACECU, NRMTRACEDU, TCE, NRCELL_FDD, TRACKINGAREA, RNC, WBTS, WCEL = extract_df_Nokia_MOs(MOs_struct_Nokia, items_Nokia, input['dir'], input['ProjName'], input['Reporting'])	
		#Trace Analysis
        if input['Reporting'] in [1,3]:	
            Nokia_Traces_Configurations(input['ProjName'], LNBTS, CTRLTS, MTRACE, NRBTS, NRMTRACECU, NRMTRACEDU, TCE, AoI_list, input['MDT'], input['MRs'], input['Thr'], input['PCMD'], input['ENDC'], input['NSETAP'])
        #Network Analysis
        if input['Reporting'] in [2,3]:
            Output_Nokia_Network_report(MRBTS, LNBTS, LNCEL, LNCEL_FDD, LNCEL_TDD, NRBTS, NRCELL, NRCELLGRP, LTEENB, NRCELL_FDD, TRACKINGAREA, RNC, WBTS, WCEL, input['ProjName'], AoI_list)		
	#Ericsson	
    if flag_Ericsson:
	    #daframe_MOs	
        ENodeBFunction, EUtranCellFDD, ReportConfigEUtraIntraFreqPm, PmUeMeasControl, PmEventService, FeatureState, OptionalFeatureLicense, GNBDUFunction, NRCellDU, NRSectorCarrier, RncFunction, IubLink, UtranCell, LocationArea, RoutingArea, ServiceArea, Ura = extract_df_Ericsson_MOs(MOs_struct_Ericsson, items_Ericsson, input['dir'], input['ProjName'], input['Reporting'])
		#Trace Analysis
        if input['Reporting'] in [1,3]:	
            Ericsson_Traces_Configurations(ENodeBFunction, EUtranCellFDD, ReportConfigEUtraIntraFreqPm, PmUeMeasControl, PmEventService, FeatureState, OptionalFeatureLicense, AoI_list, input['ProjName'], input['MRs'])
        #Network Analysis
        if input['Reporting'] in [2,3]:
            Output_Ericsson_Network_report(ENodeBFunction, EUtranCellFDD, GNBDUFunction, NRCellDU, NRSectorCarrier, RncFunction, IubLink, UtranCell, LocationArea, RoutingArea, ServiceArea, Ura, input['ProjName'], AoI_list)
	#huawei	
    if flag_Huawei:
	    #daframe_MOs	    			
        SRAN_ENODEBFUNCTION, SRAN_CELL, SRAN_CELLOP, SRAN_CNOPERATOR, SRAN_CNOPERATORTA,UMTS_RNC_UCELL, UMTS_RNC_URNCBASIC, UMTS_RNC_UCNOPERGROUP, UMTS_RNC_UCNOPERATOR, GSM_BSC_GCELL = extract_df_Huawei_MOs(MOs_struct_Huawei, items_Huawei, input['dir'], input['ProjName'], input['Reporting'])
        #Network Analysis
        if input['Reporting'] in [2,3]:
            Output_Huawei_Network_report(SRAN_ENODEBFUNCTION, SRAN_CELL, SRAN_CELLOP, SRAN_CNOPERATOR, SRAN_CNOPERATORTA, UMTS_RNC_UCELL, UMTS_RNC_URNCBASIC, UMTS_RNC_UCNOPERGROUP, UMTS_RNC_UCNOPERATOR, GSM_BSC_GCELL, input['ProjName'], AoI_list)		
			
##################################################Main function##################################################	

if __name__ == '__main__': #isto significa que o modulo "main" programa arranca deste ficheiro. Caso contrario, caso a main arrancasse noutro ficheiro, era esse o valor da variavel aqui
    #read inputs
    input, file, AoI_list = main()
    start_time = time.time()     
    #parser
    MOs_struct_Nokia, items_Nokia, flag_Nokia, MOs_struct_Ericsson, items_Ericsson, flag_Ericsson, MOs_struct_Huawei, items_Huawei, flag_Huawei = parser(file, input)
    
	#reporting
    reporting(MOs_struct_Nokia, items_Nokia, flag_Nokia, MOs_struct_Ericsson, items_Ericsson, flag_Ericsson, MOs_struct_Huawei, items_Huawei, flag_Huawei, input, AoI_list)
	
    print("Total Time: "+str(round((time.time() - start_time), 2)) + " seconds.")

################################################################################################################

