# -*- encoding: utf-8 -*-
import os
import pypyodbc
import re
import sqlite3
import sys
import time
import csv
import codecs

import warnings

import xml.etree.ElementTree as ET

import pdsys_sql as sql  # Local file with system table SQL

import configparser

########################################  FROM ANWB 5th Feb
###
###
###   DONE suppress strings - now an option
###
###   DONE  xml subprocedure for main, o2o and o2m
### DONE match key by looping through (lower)fieldnames
###   Index query dictionaries by fieldname instead of number
###
###   DONE Top only xml declaration
###   DONE write unicode xml
###   DONE *xmlns:xsi stuff in xml namespaces
###
###   DONE test against non-latin1 fieldnames/groups


###   DONE test what happens if you leave the keys as they are
###   DONE !! pick up types from lookup fields
###   DONE!! pass Explorer types across to metadata (don't translate)  and dbtype
###   DONE!! Groups - all lookups are group.  Non-lookup strings are not, therefore not visible.
###   DONE - first numeric non-key is set as Objective

#########  DEFAULTS
testdb = False
testsql = True
replacekeys = False
allowsources = False
allowstrings = True
write_flat_files = True

sample = '100 '

#####  ANYTHING ABOVE HERE CAN BE RECONFIGURED IN PROPERTIES FILE ########
pe_properties = sys.argv[1]
parser = configparser.ConfigParser()
# Open the properties file with the correct encoding
with codecs.open(pe_properties, 'r', encoding='utf-8') as f:
    parser.readfp(f)

warnings.simplefilter('error', UnicodeWarning)

errors=''
now = time.strftime("%Y%m%d-%H%M%S")
data_dir = parser.get('pd2sql', 'data_dir')
data_dirnow= data_dir + '\\' + now







class ExplorerDomain:
    def __init__(self):



        if testdb:
            from fake import fake_db




            self.pddb = 'fakepd.db'
            fake_db(self.pddb)
            tablesql = 'select * from table'
            lookupsql = 'select * from lookups'
            self.groupnamesql = 'select cdd_name from tables'
        else:
            # cnxn = pyodbc.connect('DSN=pdsys2')
            # cursor = cnxn.cursor()
            dsn = parser.get('pd2sql', 'dsn')
            self.pddb = 'DSN=' + dsn + ';unicode_results=True;CHARSET=UTF8'


            tablesql = sql.tablesql
            lookupsql = sql.lookupsql
            fieldsql = sql.fieldsql


            self.groupnamesql = "select cdd_name from CUST_DOMAIN_DATA where CDD_CD_ID='%s' and CDD_IS_SYSTEM_GROUP='F' and CDD_ADVANCED_USE_ONLY='F'"

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        if not os.path.exists(data_dirnow):
            os.makedirs(data_dirnow)

        self.domain = parser.get('pd2sql', 'domain')
        self._groups = None

        # Get the objective from the properties file if it exists
        try:
            objective = parser.get('pd2sql', 'objective')

            #print('Got objective...',objective)
            self.objectivegroup = objective.split('.')[0]
            self.objective = objective.split('.')[1]
            self.objectiveset = True
            #print('Objective is', self.objectivegroup, self.objective)
            #print('Objective is', self.objectivegroup, self.objective)
        except:
            self.objective = None
            self.objectivegroup = None
            self.objectiveset = False

        # Load in the tables & lookups data   # print( self.pdgroups['Giver'])
        self.pdgroups = querytodict(tablesql % self.domain, self.pddb, 1)
        self.pdlookups = querytodict(lookupsql % self.domain, self.pddb, 2)
        self.pdfields = querytodict(fieldsql, self.pddb, 0)
        print('LENGTH',len(self.pdlookups))

        # Identify the main, onetoone, onetomany groups
        self.make_group_lists()

        # Generate SQL
        self.group_sql()

        # print( 'Main fields',self.sqlgroups[self.main]['finalfields'])
        self.create_xml()


        self.write_statusfile()

    # list of groups to be imported
    def groups(self):
        #  single list of PD groups to load
        if self._groups == None:
            try:
                groupsin = parser.get('pd2sql', 'groups')
                print("Specified groups ")
                tmp = []

                for i in groupsin.split(','):
                    tmp.append(i)
                self._groups = tmp
            except:
                try:
                    xgroups = parser.get('pd2sql', 'xgroups')
                    print("Excluding groups " + xgroups)
                    # Get all valid pdgroups
                    tmp = querytolist(self.groupnamesql % self.domain, self.pddb)

                    # then remove xtables
                    for i in xgroups.split(","):
                        if (i in tmp):
                            tmp.remove(i)

                    self._groups = tmp
                except:
                    print("No groups or xgroups specified. Will load all.")
                    self._groups = querytolist(self.groupnamesql % self.domain, self.pddb)

            print('Groups:', self._groups)

        return self._groups

    # Set the main, onetoone, and onetomany groups, and mainkey
    def make_group_lists(self):
        # Set attributes main, onetoone, onetomany
        self.onetoone = []
        self.onetomany = []

        for group in self.groups():
            if self.pdgroups[group]['cdd_parent_cdd_id'] == 'NULL' or self.pdgroups[group]['cdd_parent_cdd_id'] == None:
                self.main = group

            else:
                print(self.pdgroups[group])
                if self.pdgroups[group]['cdd_one_to_many'] == 'T':
                    self.onetomany.append(group)
                else:
                    self.onetoone.append(group)





    def mainkey(self):
        return self.pdgroups[self.main]['cdd_key']

    def group_sql(self):
        try:
            return self.sqlgroups
        except:
            self.sqlgroups = {}

            for group in self.groups():
                # print( 'Processing group:', group)
                self.sqlgroups[group] = {}
                ## Get key for group
                print('keys:',group, self.pdgroups[group]['cdp_paramname'],self.pdgroups[group]['cdd_key'], self.pdgroups[group]['cdd_parent_cdd_id'] )
                # CDD_
                if self.pdgroups[group]['cdd_parent_cdd_id'] == 'NULL' or self.pdgroups[group]['cdd_parent_cdd_id'] == None:
                    self.sqlgroups[group]['key'] = self.pdgroups[group]['cdd_key']
                elif self.pdgroups[group]['cdp_paramname'] == 'NULL' or self.pdgroups[group]['cdp_paramname'] == None:
                    #self.sqlgroups[group]['key'] = self.pdgroups[group]['cdd_key']
                    print('KEY error on '+group)

                    raise
                else:
                    self.sqlgroups[group]['key'] = self.pdgroups[group]['cdp_paramname']


                ## Replace key with mainkey if necessary, (and don't currently replace lookup source fieldnames)
                ## and any given group.hack or all.hack
                ## Populates: self.sqlgroups[g]['hacked']
                self.hacksql(group)

                ## Generate lookup references for sqlgroups[group]: toplu, bottomlu, sourcefields, lookups
                self.lookups(group)
                # print( 'got lookups')
                ## Build expanded SQL components
                self.sqlgroups[group]['expanded_sql'] = 'select top ' +  sample + '  hacked.* ' \
                                                        + '\n------- TOPLU\n'\
                                                        + self.sqlgroups[group]['toplu'] \
                                                        + '\n------- END TOPLU\n' \
                                                        + ' from ( ' \
                                                        + '\n------- HACKED\n'\
                                                        + self.sqlgroups[group]['hacked'] \
                                                        + '\n------- END HACKED\n'\
                                                        + ' ) hacked  ' \
                                                        + '\n------- BOTTOMLU\n'\
                                                        + self.sqlgroups[group]['bottomlu']

                # Wrap in field selector

                self.select_fields(group, self.pddb)



                ####print( self.sqlgroups[g]['select fields sql'])
                if testsql or write_flat_files:
                    test_sql(self.pddb, self.sqlgroups[group]['select fields sql'], group)



                sql = self.sqlgroups[group]['select fields sql']
                #print(sql)
                f = open(data_dirnow + '\\' + group + '.sql', 'w', encoding="utf-8")
                f.write(sql)
                f.close()
                # print( 'Done group SQL')

        return self.sqlgroups



    def select_fields(self, group, db):
            # Wraps core sql query in an outer select statement,
            # suppressing source fields and strings, renaming lookups, renaming renames!

            # self.sqlgroups[group]['expandedfields'], self.sqlgroups[group]['expandedtypes'], self.sqlgroups[group][
            #    'expandedsizes'] \
            #    = get_field_info(self.sqlgroups[group]['expanded_sql'], self.pddb)

            self.sqlgroups[group]['expandedsources'], self.sqlgroups[group]['expandedrenames'], self.sqlgroups[group]['expandedtypes'], self.sqlgroups[group][
                'expandedsizes'] = get_pdfield_info(self, group, self.pddb)

            selectedfields = ''
            selectedtypes = []
            selectedsizes = []
            booleans = []

            print( 'sourcefields:',self.sqlgroups[group]['expandedsources'])

            for i in range(len(self.sqlgroups[group]['expandedsources'])):
                expsource = self.sqlgroups[group]['expandedsources'][i]
                exprename = self.sqlgroups[group]['expandedrenames'][i]
                exptype = self.sqlgroups[group]['expandedtypes'][i]
                expsize = self.sqlgroups[group]['expandedsizes'][i]

              # Potentially supress source fields, or tag __pdsrc
                if expsource in self.sqlgroups[group]['sourcefields']:
                    if allowsources:
                        print('fff '+expsource + ' as ' + exprename+ '__pdsrc,')
                        selectedfields = selectedfields + expsource + ' as ' + exprename+ '__pdsrc,'
                        selectedtypes.append(exptype)
                        selectedsizes.append(expsize)
                    else:
                        print('Excluding source field ', expsource)

                 # Potentially suppress lone (non-lookup) strings
                elif expsource=='<lookup field>':
                 # and group+'__'+(self.sqlgroups[group]['expandedfields'][i]).lower() in self.pdfields:
                    selectedfields = selectedfields + exprename + '__pdlookup as '+exprename+','
                    selectedtypes.append(exptype)
                    selectedsizes.append(expsize)

                elif exptype == 'boolean':
                    selectedfields = selectedfields + '(case when '+expsource+'=1 then 1  when '+expsource+'=0 then 0 end) as '+exprename+','
                    selectedtypes.append('integer')
                    selectedsizes.append(expsize)

                # Potentially suppress lone (non-lookup) strings
                elif allowstrings or exptype != "string": ##or re.match(r'.*__pdlookup', expfield):
                    # and group+'__'+(self.sqlgroups[group]['expandedfields'][i]).lower() in self.pdfields:
                    selectedfields = selectedfields + expsource + ' as ' + exprename + ','
                    selectedtypes.append(exptype)
                    selectedsizes.append(expsize)
                else:
                    print('Excluding non-lookup string field ', expsource)

                if expsource == self.sqlgroups[group]['key']:
                    self.sqlgroups[group]['key'] = exprename

                print('Obj?', self.objectiveset, expsource, exptype, isnumeric(exptype))
                if not (self.objectiveset) and isnumeric(exptype) and \
                        re.search('as ' + exprename + '(,|$| )', selectedfields) and \
                        not (exprename == self.sqlgroups[group]['key'] or exprename == self.mainkey()) \
                        and (self.pdgroups[group]['cdd_one_to_many'] == 'F'):
                    self.objective = exprename
                    self.objectivegroup = group
                    self.objectiveset = True
                    print('Setting objective:' + self.objectivegroup, self.objective, exptype)

            selectedfields = re.sub(',$', '', selectedfields)## Trim trailing comma
            print('s1', selectedfields)
            selectedfieldsql=selectedfields


####            for rn in self.sqlgroups[group]['renamefields']:
####                print('ZZZ', self.sqlgroups[group]['renamefields'][rn])
####                source = self.sqlgroups[group]['renamefields'][rn]['cdf_source_fieldname']
####                rename = self.sqlgroups[group]['renamefields'][rn]['cdf_fieldname']
####                print('rename ', source, ' as ', rename)
####                if not (re.search('(,|^)' + source + '(,|$| )', selectedfieldsql)):
####                    print('ERROR:', source, 'not in output field list for ', group, ' - ', selectedfieldsql)
####                    global errors
####                    errors = errors + '\n' + 'ERROR: ' + source + ' not in output field list ' + group + ' - ' + selectedfieldsql
####                selectedfieldsql = re.sub('(,|^)(' + source + ')(,|$)', r'\1\2 as ' + rename + r'\3', selectedfieldsql)
####                selectedfieldsql = re.sub('(,|^)(' + source + ') as "' + source + '__pdlookup"(,|$)',
####                                          r'\1\2 as "' + rename + r'"\3', selectedfieldsql)
####                selectedfieldsql = re.sub('(,|^)(' + source + ') as "' + source + '__pdsrc"(,|$)',
####                                          r'\1\2 as "' + rename + r'"\3', selectedfieldsql)
####

####                if source == self.sqlgroups[group]['key']:
####                    self.sqlgroups[group]['key'] = rename
####
####                    # print('s5', selectedfieldsql)
####
####                    ############ set as default objective something that exists in output
####                for i in range(len(self.sqlgroups[group]['expandedfields'])):
####                    expfield = self.sqlgroups[group]['expandedfields'][i]
####                    exptype = self.sqlgroups[group]['expandedtypes'][i]
####                    if expfield == rename:
####                        print('Obj?', self.objectiveset, expfield, exptype, isnumeric(exptype))
####                        if not (self.objectiveset) and isnumeric(exptype) and \
####                                re.search('(,|^)' + source + '(,|$| )', selectedfieldsql) and \
####                                not (expfield == self.sqlgroups[group]['key'] or expfield == self.mainkey()) \
####                                and (self.pdgroups[group]['cdd_one_to_many'] == 'F'):
####                            self.objective = expfield
####                            self.objectivegroup = group
####                            self.objectiveset = True
####                            print('Setting objective:' + self.objectivegroup, self.objective, exptype)

#####            selectedfieldsql = re.sub('(,|^)[^ ]+(,|$)', r'\1', selectedfieldsql) ## removes double commas?
#####            selectedfieldsql = re.sub(',$', '', selectedfieldsql)

            self.sqlgroups[group]['selectedtypes'] = selectedtypes
            self.sqlgroups[group]['selectedsizes'] = selectedsizes

            # Replace irregular key with main key, if required
            if replacekeys and self.sqlgroups[group]['key'] != self.mainkey():
                oldkey = self.sqlgroups[group]['key']
                print('Replacing key', oldkey)
                selectedfieldsql = re.sub(r'(?i)(\s|,|^)' + oldkey + '(\s|,|$)',
                                          r'\1' + oldkey + ' as ' + self.mainkey() + r'\2', selectedfieldsql, 1)

                self.sqlgroups[group]['key'] = self.mainkey()

            # Select wrapper for the selected fields
            self.sqlgroups[group]['select fields sql'] = 'select \n' + selectedfieldsql + '\nfrom (\n' + \
                                                         (self.sqlgroups[group]['expanded_sql']) + '\n) __outer_select'

            # Gets field type for final sql
            self.sqlgroups[group]['finalfields'], self.sqlgroups[group]['finaltypes'], self.sqlgroups[group][
                'finalsizes'] \
                = get_field_info(self.sqlgroups[group]['select fields sql'], self.pddb)
            # print( 'final fields', self.sqlgroups[group]['finalfields'])

####    def select_fieldsOrig(self, group, db):
####            # Wraps core sql query in an outer select statement,
####            # suppressing source fields and strings, renaming lookups, renaming renames!
####
####            # self.sqlgroups[group]['expandedfields'], self.sqlgroups[group]['expandedtypes'], self.sqlgroups[group][
####            #    'expandedsizes'] \
####            #    = get_field_info(self.sqlgroups[group]['expanded_sql'], self.pddb)
####
####            self.sqlgroups[group]['expandedfields'], self.sqlgroups[group]['expandedtypes'], self.sqlgroups[group][
####                'expandedsizes'] = get_pdfield_info(self, group, self.pddb)
####
####            print('expandedfields', self.sqlgroups[group]['expandedfields'])
####            selectedfields = ''
####            selectedtypes = []
####            selectedsizes = []
####            booleans = []
####            # print( 'sourcefields:',self.sqlgroups[group]['sourcefields'])
####
####            #### Build list of selected fields and types (ie. not sources for lookups, not a non-lookup string
####            #### MAYBE (and not 'secret' fields not in domain)
####            ##
####            ## !!!!!!!!!!!!!!!  TO do!!
####            ## get types from select sql, not pdfields (for ghost fields)
####            # print( 'lookups:', self.sqlgroups[group]['lookups'])
####
####            self.sqlgroups[group]['renamedfields'] = {}
####            #
####            renamesql = "SELECT [CDF_SOURCE_FIELDNAME], [CDF_FIELDNAME] \
####                        FROM   [PDSystem].[dbo].[CUST_DOMAIN_FIELD] cdf,  [PDSystem].[dbo].[CUST_DOMAIN_DATA] cdd \
####                        where cdd_cd_id=" + self.domain + " and cdd.cdd_id=cdf.cdf_cdd_id  and cdd.cdd_name=N'" + group + "' and CDF_TYPE='DATA'"
####            #
####            self.sqlgroups[group]['renamefields'] = querytodict(renamesql, self.pddb, 0)
####            print(self.sqlgroups[group]['renamefields'])
####
####            print('xxx', self.sqlgroups[group]['renamefields'])
####            for i in range(len(self.sqlgroups[group]['expandedfields'])):
####                expfield = self.sqlgroups[group]['expandedfields'][i]
####                exptype = self.sqlgroups[group]['expandedtypes'][i]
####                if exptype == "<class 'bool'>":
####                    booleans.append(expfield)
####                    print('bool', booleans)
####                expsize = self.sqlgroups[group]['expandedsizes'][i]
####                # print( 'exptype', expfield, exptype, self.objectiveset,self.sqlgroups[group]['key'],  self.mainkey())
####                # if not (self.objectiveset) and isnumeric(exptype) and not (
####                #                expfield == self.sqlgroups[group]['key'] or expfield == self.mainkey()):
####                #    self.objective = expfield
####                #    self.objectivegroup = group
####                #    self.objectiveset = True
####                #    print('Setting objective:' + self.objectivegroup,self.objective, exptype)
####
####                # Potentially supress source fields, or tag __pdsrc
####                if expfield in self.sqlgroups[group]['sourcefields']:
####                    if allowsources:
####                        selectedfields = selectedfields + expfield + ' as "' + expfield + '__pdsrc",'
####                        selectedtypes.append(exptype)
####                        selectedsizes.append(expsize)
####                    else:
####                        print('Excluding source field ', expfield)
####
####                # Potentially suppress lone (non-lookup) strings
####                elif allowstrings or exptype != "<class 'str'>" or re.match(r'.*__pdlookup', expfield):
####                    # and group+'__'+(self.sqlgroups[group]['expandedfields'][i]).lower() in self.pdfields:
####                    selectedfields = selectedfields + expfield + ','
####                    selectedtypes.append(exptype)
####                    selectedsizes.append(expsize)
####                else:
####                    print('Excluding non-lookup string field ', expfield)
####
####            ## Trim trailing comma
####            selectedfields = re.sub(',$', '', selectedfields)
####            print('s1', selectedfields)
####
####            ## Strip out lookups flag for final field list...
####            # self.sqlgroups[group]['selectedfields'] = re.sub('__pdlookup', r'', selectedfields).split(',')
####            # print('Selected fields2[]', self.sqlgroups[group]['selectedfields'])
####
####            ## ...but use 'blah blah as lookup__pdlookup' in actual sql
####            selectedfieldsql = re.sub('(([^,]*)__pdlookup)', r'\1 as \2', selectedfields)
####            ##selectedfieldsql =  selectedfields
####
####
####            for rn in self.sqlgroups[group]['renamefields']:
####                print('ZZZ', self.sqlgroups[group]['renamefields'][rn])
####                source = self.sqlgroups[group]['renamefields'][rn]['cdf_source_fieldname']
####                rename = self.sqlgroups[group]['renamefields'][rn]['cdf_fieldname']
####                print('rename ', source, ' as ', rename)
####                if not (re.search('(,|^)' + source + '(,|$| )', selectedfieldsql)):
####                    print('ERROR:', source, 'not in output field list for ', group, ' - ', selectedfieldsql)
####                    global errors
####                    errors = errors + '\n' + 'ERROR: ' + source + ' not in output field list ' + group + ' - ' + selectedfieldsql
####                selectedfieldsql = re.sub('(,|^)(' + source + ')(,|$)', r'\1\2 as ' + rename + r'\3', selectedfieldsql)
####                selectedfieldsql = re.sub('(,|^)(' + source + ') as "' + source + '__pdlookup"(,|$)',
####                                          r'\1\2 as "' + rename + r'"\3', selectedfieldsql)
####                selectedfieldsql = re.sub('(,|^)(' + source + ') as "' + source + '__pdsrc"(,|$)',
####                                          r'\1\2 as "' + rename + r'"\3', selectedfieldsql)
####
####                if source in booleans:
####                    selectedfieldsql = re.sub('(,|^)(' + source + ') as', r'\1(case when \2=1 then 1 else 0 end) as',
####                                              selectedfieldsql)
####
####                if source == self.sqlgroups[group]['key']:
####                    self.sqlgroups[group]['key'] = rename
####
####                    # print('s5', selectedfieldsql)
####
####                    ############ set as default objective something that exists in output
####                for i in range(len(self.sqlgroups[group]['expandedfields'])):
####                    expfield = self.sqlgroups[group]['expandedfields'][i]
####                    exptype = self.sqlgroups[group]['expandedtypes'][i]
####                    if expfield == rename:
####                        print('Obj?', self.objectiveset, expfield, exptype, isnumeric(exptype))
####                        if not (self.objectiveset) and isnumeric(exptype) and \
####                                re.search('(,|^)' + source + '(,|$| )', selectedfieldsql) and \
####                                not (expfield == self.sqlgroups[group]['key'] or expfield == self.mainkey()) \
####                                and (self.pdgroups[group]['cdd_one_to_many'] == 'F'):
####                            self.objective = expfield
####                            self.objectivegroup = group
####                            self.objectiveset = True
####                            print('Setting objective:' + self.objectivegroup, self.objective, exptype)
####
####            selectedfieldsql = re.sub('(,|^)[^ ]+(,|$)', r'\1', selectedfieldsql)
####            selectedfieldsql = re.sub(',$', '', selectedfieldsql)
####
####            # print('s6', selectedfieldsql)
####
####            self.sqlgroups[group]['selectedtypes'] = selectedtypes
####            self.sqlgroups[group]['selectedsizes'] = selectedsizes
####
####            # Replace irregular key with main key, if required
####            if replacekeys and self.sqlgroups[group]['key'] != self.mainkey():
####                oldkey = self.sqlgroups[group]['key']
####                print('Replacing key', oldkey)
####                selectedfieldsql = re.sub(r'(?i)(\s|,|^)' + oldkey + '(\s|,|$)',
####                                          r'\1' + oldkey + ' as ' + self.mainkey() + r'\2', selectedfieldsql, 1)
####
####                self.sqlgroups[group]['key'] = self.mainkey()
####            # Select wrapper for the selected fields
####            self.sqlgroups[group]['select fields sql'] = 'select \n' + selectedfieldsql + '\nfrom (\n' + \
####                                                         (self.sqlgroups[group]['expanded_sql']) + '\n) __outer_select'
####
####            # Gets field type for final sql
####            self.sqlgroups[group]['finalfields'], self.sqlgroups[group]['finaltypes'], self.sqlgroups[group][
####                'finalsizes'] \
####                = get_field_info(self.sqlgroups[group]['select fields sql'], self.pddb)
            # print( 'final fields', self.sqlgroups[group]['finalfields'])

    def hacksql(self, group):
        # concatenate the 2 sql fields (to get round 4000 character limit)
        self.sqlgroups[group]['hacked'] = self.pdgroups[group]['ss_sql_text1'] + self.pdgroups[group]['ss_sql_text2']

        # Build tilde-separated hack list from all.hack + group.hack
        # hack= regex1,sub1,regex2,sub2 ...
        try:
            allhack = parser.get('pd2sql', '.hack')
        except:
            allhack = 'dummy~dummy'

        try:
            hack = allhack + '~' + parser.get('pd2sql', group + '.hack')
        except:
            hack = allhack

        # Execute hack replacements
        hacklist = hack.split('~')

        for x in range(0, int(len(hacklist) / 2)):
            self.sqlgroups[group]['hacked'] = re.sub(r'' + hacklist[x * 2], r'' + hacklist[x * 2 + 1],
                                                     self.sqlgroups[group]['hacked'])
            # For testing ~ | replacement only, only way to get a tilde into the data by hacking, as it is the hack separator!
            #self.sqlgroups[group]['hacked'] = re.sub(r'€', r'~',self.sqlgroups[group]['hacked'])


    def lookups(self, group):

        self.sqlgroups[group]['sourcefields'] = []
        self.sqlgroups[group]['lookups'] = []
        self.sqlgroups[group]['toplu'] = ''
        self.sqlgroups[group]['bottomlu'] = ''

        for l in self.pdlookups:
            print('LOOKUP', self.pdlookups[l]['cdd_name'], self.pdlookups[l]['cdf_fieldname'])
            if self.pdlookups[l]['cdd_name'] == group:

                source = self.pdlookups[l]['cdf_fieldname']
                lookup = self.pdlookups[l]['cdf_lookup_cdlf_fieldname']

                self.sqlgroups[group]['sourcefields'].append(self.pdlookups[l]['sourcefield'])  # .lower())
                self.sqlgroups[group]['lookups'].append(source)
                tmp_table = 'lu_' + str(self.pdlookups[l]['cdl_sd_id']) + '_'+ str(self.pdlookups[l]['cdf_id']) + '_'     + self.pdlookups[l]['sourcefield'] + '_'   + lookup

                #### top pattern looks like:
                #####                     , lu_pc_reg_id_2113_lu_desc.[lu_desc] as "Region"
                self.sqlgroups[group]['toplu'] = self.sqlgroups[group]['toplu'] + '\n, ' + tmp_table  + '.[' + lookup + ']  as "' + source + '__pdlookup"\n'
                #self.sqlgroups[group]['toplu'] = self.sqlgroups[group]['toplu'] + '\n, ' + tmp_table  + '.[' + lookup + ']  as "' + source + '"\n'

                # Where there is a lookup, copy the field data scross from source to final fieldname
                group_field = group + '__' + (source).lower()
                self.pdfields[group_field] = self.pdfields[group + '__' + (self.pdlookups[l]['sourcefield']).lower()] ##HERE123
                #### bottom pattern looks like:
                ####            left outer join ( SELECT DISTINCT ROLLENR as id,  cast(ROLLENR as varchar) as descr   FROM [SOSData].[dbo].[ABONNEMENT]
                ####            ) lu_2524_ROLLENR_descr
                ####            on orig.[ROLLENR] = lu_2524_ROLLENR_descr.[id]
                self.sqlgroups[group]['bottomlu'] = self.sqlgroups[group]['bottomlu'] + '\nleft outer join (' + \
                                                    self.pdlookups[l]['ss_sql_text1'] + self.pdlookups[l][
                                                        'ss_sql_text2'] + ') ' \
                                                    + tmp_table + '\n   on hacked.' + self.pdlookups[l]['sourcefield'] \
                                                    + ' = ' \
                                                    + tmp_table + '.[' + self.pdlookups[l][
                                                        'cdf_lookup_key_cdlf_fieldname'] + ']'
                # print 'Got lookups',self.sqlgroups[group]['lookups']


    def create_xml(self):
        root = ET.Element('dataobjects', xsi="http://www.w3.org/2001/XMLSchema-instance",
                          xsd="http://www.w3.org/2001/XMLSchema",
                          xmlns="http://services.analytics.portrait.pb.com/")

        # xml_topdataobjects = ET.SubElement(root, 'dataobjects', xsi="http://www.w3.org/2001/XMLSchema-instance",
        #                                xsd="http://www.w3.org/2001/XMLSchema",
        #                                xmlns="http://services.analytics.portrait.pb.com/")
        dataobject = self.write_group_xml(root, 'dataobject', [self.main],'main')

        xml_dataobjects = ET.SubElement(dataobject, 'dataobjects')
        junk = self.write_group_xml(xml_dataobjects, 'dataobject', self.onetoone,'one2one')

        xml_dataobjectcollections = ET.SubElement(dataobject, 'dataobjectcollections')
        junk = self.write_group_xml(xml_dataobjectcollections, 'dataobjectcollection', self.onetomany,'onetomany')

        outFile = open(data_dirnow + '\\ADSmetadata.xml', mode="wb")
        doc = ET.ElementTree(root)
        outFile.write(codecs.BOM_UTF8)
        doc.write(outFile, encoding="utf-8", xml_declaration=True)
        outFile.close()


    def write_group_xml(self, parent, name, list,level):
        # print( 'Doing', list)
        xml_object = None
        for group in list:
            xml_object = ET.SubElement(parent, name, tablename=group, displayname=group, name=group, visible="true")

            if level != 'onetomany':
                xml_primarykeys_g = ET.SubElement(xml_object, 'primarykeys')
                xml_primarykey_g = ET.SubElement(xml_primarykeys_g, 'primarykey', columnname=self.sqlgroups[group]['key'])


            if level != 'main':
                xml_foreignkeys_g = ET.SubElement(xml_object, 'foreignkeys')
                xml_foreignkey_g = ET.SubElement(xml_foreignkeys_g, 'foreignkey', columnname=self.sqlgroups[group]['key'])

            xml_fields_g = ET.SubElement(xml_object, 'fields')

            for f in range(len(self.sqlgroups[group]['finalfields'])):



                group_field = group + '__' + re.sub('__pdsrc', '', self.sqlgroups[group]['finalfields'][f].lower())
                try:

                    sourcename = self.sqlgroups[group]['finalfields'][f]
                    fieldname = self.sqlgroups[group]['finalfields'][f]
                    description = str(self.pdfields[group_field]['description'])
                except:

                    sourcename = self.sqlgroups[group]['finalfields'][f]
                    fieldname = self.sqlgroups[group]['finalfields'][f]
                    description = ''


                #sourcename = self.sqlgroups[group]['finalfields'][f]
                #fieldname = self.sqlgroups[group]['finalfields'][f]
                explorertype = explorer_type(self.sqlgroups[group]['finaltypes'][f])
                explorersize = explorer_size(self.sqlgroups[group]['finalsizes'][f])


                #print('field:',group_field,sourcename,fieldname,self.sqlgroups[group]['finalfields'][f])
                xml_field = ET.SubElement(xml_fields_g, 'field', columnname=sourcename, type=explorertype,
                                          name=fieldname)

                # Where the type is group, give me pdtype=”<explorertype>”.
                # Where the type (or if type=group, pdtype) is string, give me pdlength=”<nchars>”.

                if sourcename in self.sqlgroups[group]['lookups']:
                    xml_field.set('type', 'group')
                    xml_field.set('pdtype', explorertype)
                if explorertype == 'string' and explorersize!=None:
                    xml_field.set('pdlength', explorersize)

                # Flag sources and objective
                if re.match('.*__pdsrc', sourcename):
                    xml_field.set('pdsource', 'true')
                if sourcename == self.objective and group == self.objectivegroup:
                    xml_field.set('objective', 'true')

        return xml_object

    def write_statusfile(self):
        f = open(data_dir + '\\status.properties', 'w')
        f.write('[Python]\n')
        f.write('export=' + now + '\n')
        f.write('separator=,\n')
        f.write('quote="\n')
        f.write('status=pending\n')
        f.write('\n')
        f.write('domain=' + self.domain + '\n')
        f.close()

        # [Python]
        # export=20160226-033330
        # status=pending

















        # domain=1003



def explorer_size(size):
    if int(size) > 9999:
        ConvertedSize=None
    else:
        ConvertedSize=size

    return ConvertedSize

def explorer_type(type):
    if type == "<class 'int'>":
        ConvertedType = 'integer'
    elif type == "<class 'unicode'>":
        ConvertedType = 'string'
    elif type == "<class 'str'>":
        ConvertedType = 'string'
    elif type == "<class 'datetime.datetime'>":
        ConvertedType = 'datetime'

    elif type == "<class 'float'>":
        ConvertedType = 'float'
    elif type == "<class 'decimal.Decimal'>":
        ConvertedType = 'float'
    elif type == "<class 'long'>":
        ConvertedType = 'integer'
    elif type == "<class 'bool'>":
        ConvertedType = 'boolean'
    elif type == "<class 'bytearray'>":
        ConvertedType = 'integer'
    else:
        ConvertedType = type


    return ConvertedType



def isnumeric(pytype):
    numerics = ['integer','float']
    return (pytype in numerics)

def get_pdfield_info(self,group,db):
    pdfieldsql = "SELECT cdf_order_index, cdf_source_fieldname, cdf_fieldname ,  concat([CDF_SOURCE_FIELDNAME], ' as ', [CDF_FIELDNAME]) as rename, cdf_datatype, cdf_size \
                FROM   [PDSystem].[dbo].[CUST_DOMAIN_FIELD] cdf,  [PDSystem].[dbo].[CUST_DOMAIN_DATA] cdd \
                where cdd_cd_id=" + self.domain + " and cdd.cdd_id=cdf.cdf_cdd_id  and cdd.cdd_name=N'" + group + "'  order by cdf_order_index "
    pdf=querytodict(pdfieldsql,db,0)
    sourcelist = []
    renamelist = []
    typelist = []
    sizelist = []
    for i in (pdf):
        print('PDF',pdf[i])
        sourcelist.append(pdf[i]['cdf_source_fieldname'])
        renamelist.append(pdf[i]['cdf_fieldname'])
        typelist.append(pdf[i]['cdf_datatype'])
        sizelist.append(pdf[i]['cdf_size'])
        #sizelist.append(str(datasizes[i]))

    return (sourcelist, renamelist, typelist, sizelist)

def get_field_info(sql, db):
    pypyodbc.lowercase = False

    if testdb:
        connection = sqlite3.connect(db)

    else:
        connection = pypyodbc.connect(db)

    cur = connection.cursor()

    try:

        print(sql)

        x = cur.execute(sql)
    except:

        print('#######################################################################')
        print(sql)
        print('SQL error in get_field_info')

        raise

    connection.close()
    fields = [tuple[0] for tuple in x.description]
    datatypes = [tuple[1] for tuple in x.description]
    datasizes = [tuple[3] for tuple in x.description]

    fieldlist = []
    typelist = []
    sizelist = []
    for i in range(len(fields)):
        fieldlist.append(fields[i])
        typelist.append(str(datatypes[i]))
        sizelist.append(str(datasizes[i]))
    return (fieldlist, typelist, sizelist)


def querytolist(sql, db):
    if testdb:
        connection = sqlite3.connect(db)

        connection.row_factory = sqlite3.Row
    else:
        connection = pypyodbc.connect(db)

    x = connection.cursor().execute(sql)
    # x = connection.execute(sql)
    row = x.fetchall()
    results = []
    for i in row:
        results.append(i[0])
    connection.close()
    # cursor = cnxn.cursor()

    return results


def querytodict(sql, db, n):
    ## Takes a sql query and turns result into dict of dictionarys, keyed on nth column:
    #        d[key value][field], eg. d['rob']['home'] -> hove
    #print('DB is:', db)
    if testdb:
        connection = sqlite3.connect(db)

    else:
        print('DB',db)
        connection = pypyodbc.connect(db)

    cursor = connection.cursor()
    # cur.execute("set character_set_results = 'latin1'")
    try:
        x = cursor.execute(sql)

    except:
        print('#######################################################################')
        print(sql)
        print('SQL error in query to dict:')
        raise

    rows = x.fetchall()
    fields = [tuple[0] for tuple in x.description]
    types = [tuple[1] for tuple in x.description]
    sizes = [tuple[3] for tuple in x.description]

    d = {}

    for r in rows:
        # print(r)
        d[r[n]] = {}
        d[r[n]] = dict(zip(fields, r))
        # print(type(d[r[n]]['SS_SQL_TEXT1']))
    connection.close()

    return d


def test_sql(db, sql, name):
    connection = pypyodbc.connect(db)
    cursor = connection.cursor()
    countrecords = 'select count(*) from (' + sql + ' ) xxx'

    try:

        x = cursor.execute(countrecords)

        row = x.fetchone()
        print('SQL okay for', name, row[0], 'records')
        print(' sample is top '+ sample)
    except:

        print('#######################################################################')
        print(sql)
        print('SQL error in test SQL:')
        raise

    if write_flat_files:

        cur = cursor.execute(sql)

        # print( 'r3', r3)
        # r4=row2.decode('latin-1').encode('utf-8')
        # print( 'r4', r3)
        # print( row2.decode('unicode-escape'))
        if not os.path.exists(data_dirnow + '\\' + name + '_wrap'):
            os.makedirs(data_dirnow + '\\' + name + '_wrap')
        with open(data_dirnow + '\\' + name + '_wrap\\export.txt', 'w', encoding='utf-8') as f:

            ## FASTEST option, spew out CSV irrespective of delimiters ...
            #    UnicodeWriter(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n", escapechar='\\', quotechar='~',
             #                     delimiter='|' ).writerows(cursor)
            #with open('eggs.csv', 'w', newline='') as csvfile:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n", escapechar='\\', quotechar='"', delimiter=',').writerows(cursor)


            ## ... or SAFEST option, double up quoutes and delimiters in data
            #writer = csv.writer(f, quoting=csv.QUOTE_ALL, delimiter=str("|"), quotechar=str("~"))
            #for row in cur:
            #    writer.writerow(map(quote, row))

    connection.close()





def ExplorerXmlGroups(xml):
    # TODO check for unicode group names
    # ET is from: import xml.etree.ElementTree as ET
    tree = ET.parse(xml)
    namespace = '{http://services.analytics.portrait.pb.com/}'

    # Get Main group
    x = tree.find(namespace + 'dataobject')
    main = x.attrib['tablename']

    # Get one-to-ones
    onetoone = []
    x = tree.find(namespace + 'dataobject/' + namespace + 'dataobjects')
    for i in x:
        onetoone.append(i.attrib['tablename'])

    # Get one-to-manys
    onetomany = []
    x = tree.find(namespace + 'dataobject/' + namespace + 'dataobjectcollections')
    for i in x:
        onetomany.append(i.attrib['tablename'])

    return main, onetoone, onetomany



##########################################################################################################
##########################################################################################################


def main():

    pe = ExplorerDomain()


    end = time.strftime("%Y%m%d-%H%M%S")
    print('start',now)
    print('end  ',end)
#    sys.exit(sqldirnow)

# a,b,c=ExplorerXmlGroups('C:\\Users\\PBDIA00022\\PycharmProjects\\PD2SQL\\test\\cheese\\ADSmetadata.xml')
# print( 'a:',a)
# print( 'b:',b)
# print( 'c:',c)

# print( pe.groups())
# print( pe.main)
# pe.group_sql()
# print( pe.group_sql()['Giver']['sql'])

main()

print('Nearly Done!!')

print('Done!!')
print(errors)