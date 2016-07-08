#########################################################
#
#  Internal config:
#
#  replacekeys = True  # change them all to main table key, or leave as is
#  allowsources = True # maintain lookup source fields eg. gender as gender_pdsrc
#  allowstrings = True  # Allow strings that are not lookups to propagate, (eg. perhaps for Cards table, otherwise unusable in Explorer)
#  write_sql = True     # Write the expannded group sql
#  write_flat_files = True  # Write the flat files with the data in, in format suitable for FF2PE.py
#  top_percent = 0.1       # sample the data in flat file export

# import pyodbc  # pip install
import pypyodbc as pyodbc  # pip install
import codecs
import os
import re
import sys
import csv
import warnings
import xml.etree.ElementTree as ET
from configparser import ConfigParser

import pdsys_sql  # Local file with system table SQL

########################################  FROM ANWB 5th Feb
###
###   TO-DO
###   DONE suppress strings - now an option
###
###   DONE  xml subprocedure for main, o2o and o2m
###   Index query dictionaries by fieldname instead of number
###
###   DONE Top only xml declaration
###   DONE write unicode xml
###   TODO *xmlns:xsi stuff in xml namespaces
###
###   TODO test against non-latin1 fieldnames/groups
###         UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe6 in position 10: invalid continuation byte
###
###   TODO test what happens if you leave the keys as they are
###   DONE !! pick up types from lookup fields
###   DONE!! pass Explorer types across to metadata (don't translate)  and dbtype
###   DONE!! Groups - all lookups are group.  Non-lookup strings are not, therefore not visible.
###   DONE - first numeric non-key is set as Objective


replacekeys = True
allowsources = True
allowstrings = True
write_sql = True
write_flat_files = True
top_percent = 0.1

pe_properties = sys.argv[1]
sqldir = sys.argv[2]
parser = ConfigParser()
# Open the properties file with the correct encoding
with codecs.open(pe_properties, 'r', encoding='utf-8') as f:
    parser.read_file(f)

warnings.simplefilter('error', UnicodeWarning)

if not os.path.exists(sqldir):
    os.makedirs(sqldir)


class ExplorerDomain:
    def __init__(self):

        dsn = parser.get('pd2sql', 'dsn')
        self.pddb = 'DSN=' + dsn + ';unicode_results=True'

        tablesql = pdsys_sql.tablesql
        lookupsql = pdsys_sql.lookupsql
        fieldsql = pdsys_sql.fieldsql

        self.groupnamesql = "select cdd_name from CUST_DOMAIN_DATA where CDD_CD_ID='%s'"

        self.domain = parser.get('pd2sql', 'domain')

        self._groups = None

        # Get the objective from the properties file if it exists
        try:
            self.objective = parser.get('pd2sql', 'objective')
            self.objectiveset = True
            print(('Objective is', self.objective))
        except:
            self.objective = None
            self.objectiveset = False

        # Load in the tables & lookups data   # print(( self.pdgroups['Giver']))
        self.pdgroups = querytodict(self.pddb, tablesql % self.domain, 1)
        self.pdlookups = querytodict(self.pddb, lookupsql % self.domain, 1)
        self.pdfields = querytodict(self.pddb, fieldsql, 0)

        # Identify the main, onetoone, onetomany groups
        self.make_group_lists()

        # Generate SQL
        # print('aaa')
        self.group_sql(self.pddb)

        # print(( 'Main fields',self.sqlgroups[self.main]['finalfields']))
        self.create_xml()

    def groups(self):
        #  single list of PD groups to load
        if self._groups == None:
            try:
                groupsin = parser.get('pd2sql', 'groups')
                print(("Specified groups "))
                tmp = []

                for i in groupsin.split(','):
                    tmp.append(i)
                self._groups = tmp
            except:
                try:
                    xgroups = parser.get('pd2sql', 'xgroups')
                    print(("Excluding groups " + xgroups))
                    # Get all valid pdgroups
                    tmp = querytolist(self.groupnamesql % self.domain, self.pddb)

                    # then remove xtables
                    for i in xgroups.split(","):
                        if (i in tmp):
                            tmp.remove(i)

                    self._groups = tmp
                except:
                    print(("No groups or xgroups specified. Will load all."))
                    self._groups = querytolist(self.groupnamesql % self.domain, self.pddb)

        print(('Groups:', self._groups))

        return self._groups

    def make_group_lists(self):
        # Set attributes main, onetoone, onetomany
        self.onetoone = []
        self.onetomany = []

        for group in self.groups():
            print(group)
            if self.pdgroups[group]['cdd_parent_cdd_id'] == 'NULL' or self.pdgroups[group]['cdd_parent_cdd_id'] == None:
                print('aaa')
                self.main = group

            else:
                print(self.pdgroups[group])
                if self.pdgroups[group]['cdd_one_to_many'] == 'T':
                    self.onetomany.append(group)
                else:
                    self.onetoone.append(group)
        print('main:', self.main)
        print('onetoone:', self.onetoone)
        print('onetomany:', self.onetomany)

    def mainkey(self):
        return self.pdgroups[self.main]['cdd_key']

    def group_sql(self, db):
        try:
            return self.sqlgroups
        except:
            self.sqlgroups = {}

            for group in self.groups():
                print(('Processing group:', group))
                self.sqlgroups[group] = {}
                ## Get key for group
                if self.pdgroups[group]['cdf2'] == 'NULL' or self.pdgroups[group]['cdf2'] == None:
                    self.sqlgroups[group]['key'] = self.pdgroups[group]['cdd_key']
                else:
                    self.sqlgroups[group]['key'] = self.pdgroups[group]['cdf2']
                print('KEY,', group, self.pdgroups[group]['cdd_key'], self.pdgroups[group]['cdf1'])
                print(self.pdgroups[group])

                ## Replace key with mainkey if necessary, (and don't currently replace lookup source fieldnames)
                ## and any given group.hack or all.hack
                ## Populates: self.sqlgroups[g]['hacked']
                self.hacksql(group)

                ## Generate lookup references for sqlgroups[group]: toplu, bottomlu, sourcefields, lookups
                self.lookups(group)
                print(('got lookups'))
                ## Build expanded SQL components
                self.sqlgroups[group]['expanded_sql'] = 'select top ' + str(top_percent) + ' percent origsql2.* ' \
                                                        + self.sqlgroups[group]['toplu'] \
                                                        + ' from ( select * from (' \
                                                        + self.sqlgroups[group]['hacked'] \
                                                        + ' ) origsql  ) origsql2' \
                                                        + self.sqlgroups[group]['bottomlu']
                print('SQL TYPE', self.sqlgroups[group]['expanded_sql'])

                # Wrap in field selector
                self.select_fields(group, self.pddb)
                print('Selected fields', (self.sqlgroups[group]['select fields sql']))

                if write_sql:
                    sql = self.sqlgroups[group]['select fields sql']
                    f = open(sqldir + '\\' + group + '.sql', 'w')
                    f.write(sql)
                    f.close()
                    print('Written SQL for', group)

                if write_flat_files:
                    try:
                        os.makedirs(sqldir + '\\' + group + '_wrap')
                    except:
                        pass

                    connection = pyodbc.connect(db)
                    cursor = connection.cursor()
                    cursor.execute(sql)
                    with open(sqldir + '\\' + group + '_wrap\\export.txt', 'w') as f:
                        csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator='\n', quotechar='~',
                                   delimiter='|').writerows(cursor)
                    connection.close()

                    print('Written flat files for', group)

        return self.sqlgroups

    def select_fields(self, group, db):
        # Wraps core sql query in an outer select statement, suppressing source fields and strings, renaming lookups

        self.sqlgroups[group]['expandedfields'], self.sqlgroups[group]['expandedtypes'], self.sqlgroups[group][
            'expandedsizes'] \
            = get_field_info(self.sqlgroups[group]['expanded_sql'], self.pddb)
        # print(( 'expandedfields', self.sqlgroups[group]['expandedfields']))

        selectedfields = ''
        selectedtypes = []
        selectedsizes = []
        # print(( 'sourcefields:',self.sqlgroups[group]['sourcefields']))

        #### Build list of selected fields and types (ie. not sources for lookups, not a non-lookup string
        for i in range(len(self.sqlgroups[group]['expandedfields'])):
            expfield = self.sqlgroups[group]['expandedfields'][i]
            exptype = self.sqlgroups[group]['expandedtypes'][i]
            expsize = self.sqlgroups[group]['expandedsizes'][i]
            # print( 'exptype', expfield, exptype, self.objective, self.objectiveset,self.sqlgroups[group]['key'],  self.mainkey())
            if not (self.objectiveset) and isnumeric(exptype) and not (
                            expfield == self.sqlgroups[group]['key'] or expfield == self.mainkey()):
                self.objective = expfield
                self.objectivegroup = group
                self.objectiveset = True
                print('Setting objective:' + self.objective, exptype)

            # Potentially supress source fields, or tag __pdsrc
            if expfield in self.sqlgroups[group]['sourcefields']:
                if allowsources:
                    selectedfields = selectedfields + expfield + ' as ' + expfield + '__pdsrc,'
                    selectedtypes.append(exptype)
                    selectedsizes.append(expsize)
                else:
                    print('Excluding source field ', expfield)
                    pass

            # Potentially suppress lone (non-lookup) strings
            elif allowstrings or exptype != "<type 'str'>" or re.match(r'.*__pdd2sql_lookup', expfield):
                # and group+'__'+(self.sqlgroups[group]['expandedfields'][i]).lower() in self.pdfields:
                selectedfields = selectedfields + expfield + ','
                selectedtypes.append(exptype)
                selectedsizes.append(expsize)
            else:
                print('Excluding non-lookup string field ', expfield)

        ## Trim trailing comma
        selectedfields = re.sub(',$', '', selectedfields)

        ## Strip out lookups flag for final field list...
        self.sqlgroups[group]['selectedfields'] = re.sub('__pdd2sql_lookup', r'', selectedfields).split(',')

        ## ...but use 'blah blah as lookup__pdd2sql_lookup' in actual sql
        selectedfieldsql = re.sub('(([^,]*)__pdd2sql_lookup)', r'\1 as \2', selectedfields)

        self.sqlgroups[group]['selectedtypes'] = selectedtypes
        self.sqlgroups[group]['selectedsizes'] = selectedsizes
        print('Selected fields', self.sqlgroups[group]['selectedfields'])

        # Replace irregular key with main key, if required
        if replacekeys and self.sqlgroups[group]['key'] != self.mainkey():
            oldkey = self.sqlgroups[group]['key']
            print('Replacing key', oldkey, self.mainkey)
            selectedfieldsql = re.sub(r'(\s|,|^)' + oldkey + '(\s|,|$)',
                                      r'\1' + oldkey + ' as ' + self.mainkey() + r'\2', selectedfieldsql, 1)

        # Select wrapper for the selected fields
        self.sqlgroups[group]['select fields sql'] = 'select \n' + selectedfieldsql + '\nfrom (\n' + \
                                                     self.sqlgroups[group]['expanded_sql'] + '\n) __outer_select'

        # Gets field type for final sql
        self.sqlgroups[group]['finalfields'], self.sqlgroups[group]['finaltypes'], self.sqlgroups[group]['finalsizes'] \
            = get_field_info(self.sqlgroups[group]['select fields sql'], self.pddb)
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
        # print( 'allhacks:',hack)

        # Execute hack replacements
        hacklist = hack.split('~')
        print('Hacklist:', hacklist)
        for x in range(0, int(len(hacklist) / 2)):
            self.sqlgroups[group]['hacked'] = re.sub(r'' + hacklist[x * 2], r'' + hacklist[x * 2 + 1],
                                                     self.sqlgroups[group]['hacked'])

    def lookups(self, group):

        self.sqlgroups[group]['sourcefields'] = []
        self.sqlgroups[group]['lookups'] = []
        self.sqlgroups[group]['toplu'] = ''
        self.sqlgroups[group]['bottomlu'] = ''

        for l in self.pdlookups:
            if self.pdlookups[l]['cdd_name'] == group:
                self.sqlgroups[group]['sourcefields'].append(self.pdlookups[l]['sourcefield'])  # .lower())
                self.sqlgroups[group]['lookups'].append(self.pdlookups[l]['cdf_fieldname'])
                tmp_table = 'lu_' + str(self.pdlookups[l]['cdl_sd_id']) + '_' \
                            + self.pdlookups[l]['sourcefield'] + '_' \
                            + self.pdlookups[l]['cdf_lookup_cdlf_fieldname']

                #### top pattern looks like:
                ########                     , lu_pc_reg_id_2113_lu_desc.[lu_desc] as "Region"
                self.sqlgroups[group]['toplu'] = self.sqlgroups[group]['toplu'] + '\n, ' + tmp_table \
                                                 + '.[' + self.pdlookups[l]['cdf_lookup_cdlf_fieldname'] + ']  as "' \
                                                 + self.pdlookups[l]['cdf_fieldname'] + '__pdd2sql_lookup"\n'

                # Where there is a lookup, copy the field data scross from source to final fieldname
                group_field = group + '__' + (self.pdlookups[l]['cdf_fieldname']).lower()
                self.pdfields[group_field] = self.pdfields[group + '__' + (self.pdlookups[l]['sourcefield']).lower()]
                #### bottom pattern looks like:
                ####            left outer join ( SELECT DISTINCT ROLLENR as id,  cast(ROLLENR as varchar) as descr   FROM [SOSData].[dbo].[ABONNEMENT]
                ####            ) lu_2524_ROLLENR_descr
                ####            on orig.[ROLLENR] = lu_2524_ROLLENR_descr.[id]
                self.sqlgroups[group]['bottomlu'] = self.sqlgroups[group]['bottomlu'] + '\nleft outer join (' + \
                                                    self.pdlookups[l]['ss_sql_text1'] + self.pdlookups[l][
                                                        'ss_sql_text2'] + ') ' \
                                                    + tmp_table + '\n   on origsql2.' + self.pdlookups[l]['sourcefield'] \
                                                    + ' = ' \
                                                    + tmp_table + '.[' + self.pdlookups[l][
                                                        'cdf_lookup_key_cdlf_fieldname'] + ']'
                # print( 'Got lookups',self.sqlgroups[group]['lookups'])


    def create_xml(self):
        root = ET.Element('dataobjects', xsi="http://www.w3.org/2001/XMLSchema-instance",
                          xsd="http://www.w3.org/2001/XMLSchema",
                          xmlns="http://services.analytics.portrait.pb.com/")

        # xml_topdataobjects = ET.SubElement(root, 'dataobjects', xsi="http://www.w3.org/2001/XMLSchema-instance",
        #                                xsd="http://www.w3.org/2001/XMLSchema",
        #                                xmlns="http://services.analytics.portrait.pb.com/")
        dataobject = self.write_group_xml(root, 'dataobject', [self.main])

        xml_dataobjects = ET.SubElement(dataobject, 'dataobjects')
        junk = self.write_group_xml(xml_dataobjects, 'dataobject', self.onetoone)

        xml_dataobjectcollections = ET.SubElement(dataobject, 'dataobjectcollections')
        junk = self.write_group_xml(xml_dataobjectcollections, 'dataobjectcollection', self.onetomany)

        outFile = open(sqldir + '\\ADSmetadata.xml', 'wb')
        doc = ET.ElementTree(root)
        doc.write(outFile)  # , encoding='utf-8', xml_declaration=True)
        outFile.close()
        # f = open(sqldir+'\\ADSmetadata.xml')
        # for l in f:
        #    print( l.decode('unicode-escape'))
        # f.close()


    def write_group_xml(self, parent, name, list):
        # print( 'Doing', list)
        xml_object = None
        for group in list:
            xml_object = ET.SubElement(parent, name, tablename=group, displayname=group, name=group, visible="true")
            xml_primarykeys_g = ET.SubElement(xml_object, 'primarykeys')
            xml_primarykey_g = ET.SubElement(xml_primarykeys_g, 'primarykey', columnname=self.mainkey())
            xml_fields_g = ET.SubElement(xml_object, 'fields')

            for f in range(len(self.sqlgroups[group]['finalfields'])):
                group_field = group + '__' + self.sqlgroups[group]['finalfields'][f]
                try:
                    sourcename = self.pdfields[group_field]['fieldname']
                    fieldname = self.pdfields[group_field]['fieldname']
                    description = str(self.pdfields[group_field]['description'])
                except:
                    sourcename = self.sqlgroups[group]['finalfields'][f]
                    fieldname = self.sqlgroups[group]['finalfields'][f]
                    description = ''

                # print('FFF',f,self.sqlgroups[group]['finaltypes'][f], self.sqlgroups[group]['finalsizes'][f])
                fieldtype = str(self.sqlgroups[group]['finaltypes'][f]) + ' ' + str(
                    self.sqlgroups[group]['finalsizes'][f])
                explorertype = explorer_type(self.sqlgroups[group]['finaltypes'][f],
                                             sourcename in self.sqlgroups[group]['lookups'])

                # if re.sub('__pdsrc','',sourcename) in self.sqlgroups[g]['sourcefields']:
                #    xml_field = ET.SubElement(xml_fields_g, 'field', columnname=sourcename, type=explorertype, dbtype=fieldtype, pdsource="true")
                # else:
                #    xml_field = ET.SubElement(xml_fields_g, 'field', columnname=sourcename, type=explorertype, dbtype=fieldtype)
                # displayname=fieldname, name=fieldname,   # NOT NEEDED

                xml_field = ET.SubElement(xml_fields_g, 'field', columnname=sourcename, type=explorertype,
                                          dbtype=fieldtype)

                if re.match('.*__pdsrc', sourcename):
                    xml_field.set('pdsource', 'true')
                if sourcename == self.objective and group == self.objectivegroup:
                    xml_field.set('objective', 'true')

        return xml_object


def explorer_type(type, lookup):
    type = str(type)
    if lookup:
        ConvertedType = 'group'
    elif type == "<type 'int'>":
        ConvertedType = 'integer1'
    elif type == "<type 'unicode'>":
        ConvertedType = 'string'
    elif type == "<type 'str'>":
        ConvertedType = 'string'
    elif type == "<type 'datetime.datetime'>":
        ConvertedType = 'datetime'
    elif type == "<type 'float'>":
        ConvertedType = 'float'
    elif type == "<type 'long'>":
        ConvertedType = 'integer'

    ## It seems PYODBC can return type or class ...
    elif type == "<class 'int'>":
        ConvertedType = 'integer'
    elif type == "<class 'unicode'>":
        ConvertedType = 'string'
    elif type == "<class 'str'>":
        ConvertedType = 'string'
    elif type == "<class 'datetime.datetime'>":
        ConvertedType = 'datetime'
    elif type == "<class 'float'>":
        ConvertedType = 'float'
    elif type == "<class 'long'>":
        ConvertedType = 'integer'
    else:
        ConvertedType = type

    print('TYPE:', type, ConvertedType)
    return ConvertedType


def isnumeric(pytype):
    numerics = ["<type 'int'>", "<type 'decimal'>", "<type 'float'>"]
    return (pytype in numerics)


def get_field_info(sql, db):
    connection = pyodbc.connect(db)
    cur = connection.cursor()

    # print('SQL TYPE:',type(sql))
    # f = open(sqldir + '\\' + 'test' + '.sql', 'w')
    # f.write(sql)
    # f.close()
    try:
        print('Submitting SQL...')
        x = cur.execute(sql)
        print('SQL okay')
    except:
        print(sql)
        print('SQL error in get_field_info:')
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
        typelist.append(datatypes[i])
        sizelist.append(datasizes[i])
    return (fieldlist, typelist, sizelist)


def querytolist(sql, db):
    connection = pyodbc.connect(db)

    x = connection.execute(sql)
    row = x.fetchall()
    results = []
    for i in row:
        results.append(i[0])
    connection.close()
    # cursor = cnxn.cursor()

    return results


def querytodict(db, sql, n):
    ## Takes a sql query and turns result into dict of dictionarys, keyed on nth column:
    #        d[key value][field], eg. d['rob']['home'] -> hove
    connection = pyodbc.connect(db)
    cur = connection.cursor()

    try:
        x = cur.execute(sql)
    except:
        print(sql)
        print('SQL error in query():')
        raise

    rows = x.fetchall()
    fields = [tuple[0] for tuple in x.description]
    types = [tuple[1] for tuple in x.description]
    sizes = [tuple[3] for tuple in x.description]

    d = {}

    # print('New TABLE')
    # print(sql)
    for r in rows:
        d[r[n]] = {}
        # print('RRRR',r,)
        d[r[n]] = dict(zip(fields, r))
    connection.close()

    return d



##########################################################################################################
##########################################################################################################




pe = ExplorerDomain()



# a,b,c=ExplorerXmlGroups('C:\\Users\\PBDIA00022\\PycharmProjects\\PD2SQL\\test\\cheese\\ADSmetadata.xml')

# print( 'a:',a)
# print( 'b:',b)
# print( 'c:',c)



# print( pe.groups())
# print( pe.main)
# pe.group_sql()
# print( pe.group_sql()['Giver']['sql'])
