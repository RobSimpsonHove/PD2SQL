# -*- encoding: utf-8 -*-
########################################  FROM ANWB 26 Oct 2016
## Author: Rob Simpson

import os
import pypyodbc
import re
import time
import csv
import codecs
import warnings
import xml.etree.ElementTree as ET
import pdsys_sql as sql  # Local file with system table SQL

################## SETTINGS - usually configured in local.py ################
## Domain to extract
domain = '1007'

## MHSystem DSN
dsn = 'PDSystem'

## OPTIONAL groups or xgroups
## Neither returns all non-advanced groups in domain,
## 'groups' returns those listed, 'xgroups' returns all but those listed
# groups=TreatmentHistory

## Objective field, defaults to first numeric non-key
# objective=group.fieldname

## Hacks:  tilde-separated pair(s) of regex, used to adapt generated SQL
## hack['all']=regex1~replace1~regex2~replace2 (...)
## hack['group'] applies to group SQL only, hack['all'] applies to all group SQL

hack = {}
hack['all'] = '{MSSQL_?NOLOCK}~~\.dbo\.~.[dbo].'
hack['TransГroup'] = 'SandboxDatabase~[SandboxDatabase]'

## Where to write data to
data_dir = 'C:/PortraitAnalytics/data'
#data_dir=foo


#########  DEFAULTS
testsql = True
replacekeys = False
allowsources = False ## Not used!
allowstrings = True  ## Not used!
write_flat_files = True
sample = '100 ' # eg. '100' (records) or '100 percent'

#####  ANYTHING ABOVE HERE CAN BE RECONFIGURED IN local.py FILE ########
##### Import any local overrides for the variables above.
try:
    exec(open('local.py').read())
except IOError:
    pass



warnings.simplefilter('error', UnicodeWarning)

errors=''
now = time.strftime("%Y%m%d-%H%M%S")
data_dirnow= data_dir + '\\' + now



class ExplorerDomain:
    def __init__(self):

        self.pddb = 'DSN=' + dsn + ';unicode_results=True;CHARSET=UTF8'
        tablesql = sql.tablesql
        lookupsql = sql.lookupsql
        fieldsql = sql.fieldsql

        self.groupnamesql = "select cdd_name from CUST_DOMAIN_DATA where CDD_CD_ID='%s' and CDD_IS_SYSTEM_GROUP='F' and CDD_ADVANCED_USE_ONLY='F'"

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        if not os.path.exists(data_dirnow):
            os.makedirs(data_dirnow)

        self.domain = domain
        self._groups = None

        # Use the objective if it exists
        try:
            self.objectivegroup = objective.split('.')[0]
            self.objective = objective.split('.')[1]
            self.objectiveset = True
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
                groupsin = groups
                print("Specified groups ")
                tmp = []

                for i in groupsin.split(','):
                    tmp.append(i)
                self._groups = tmp
            except:
                try:

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
                print( 'Processing group:', group)
                self.sqlgroups[group] = {}

                ## Get key for group
                #print('keys:',group, self.pdgroups[group]['cdp_paramname'],self.pdgroups[group]['cdd_key'], self.pdgroups[group]['cdd_parent_cdd_id'] )
                # CDD_
                if self.pdgroups[group]['cdd_parent_cdd_id'] == 'NULL' or self.pdgroups[group]['cdd_parent_cdd_id'] == None:
                    self.sqlgroups[group]['key'] = self.pdgroups[group]['cdd_id_source_fieldname']

                elif self.pdgroups[group]['cdp_paramname'] == 'NULL' or self.pdgroups[group]['cdp_paramname'] == None:
                    #self.sqlgroups[group]['key'] = self.pdgroups[group]['cdd_key']
                    error('KEY error on '+group)
                    print(errors)
                    raise

                else:
                    self.sqlgroups[group]['key'] = self.pdgroups[group]['cdp_paramname']


                ## Replace key with mainkey if necessary, (and don't currently replace lookup source fieldnames)
                ## and any given group.hack or all.hack
                ## Populates: self.sqlgroups[g]['hacked']
                self.sqlgroups[group]['hacked'] = self.hacksql(group, hack)

                newsql=self.build_sql(group,self.pddb)
                print('NEWSQL:\n',newsql)

                if testsql or write_flat_files:
                    test_sql(self.pddb, newsql, group)

                f = open(data_dirnow + '\\' + group + '.sql', 'w', encoding="utf-8")
                f.write(newsql)
                f.close()
                # print( 'Done group SQL')

        return self.sqlgroups

    def build_sql(self,group,db):
        print('Building SQL for group ',group)

        pdfieldsql = sql.pdfieldsql
        pdfields=querytodict(pdfieldsql % (self.domain , group),db, 1)
        #print('pdfields',pdfields)

        topselect=''
        hackedselect=''
        toplu=''
        bottomlu=''

        self.sqlgroups[group]['lookups']=[]

        for f in pdfields:
            #print('f', pdfields[f])

            if pdfields[f]['cdf_type']=='DATA':
                if self.sqlgroups[group]['key']==pdfields[f]['cdf_source_fieldname']:
                    self.sqlgroups[group]['key']=pdfields[f]['cdf_fieldname']
                hackedselect=hackedselect+pdfields[f]['cdf_source_fieldname'] + ' as ' + pdfields[f]['cdf_fieldname'] + ', '

                if pdfields[f]['cdf_advanced_use_only']=='F' or self.sqlgroups[group]['key']==pdfields[f]['cdf_fieldname']:
                    topselect=topselect+ pdfields[f]['cdf_fieldname'] + ', '#

                if not (self.objectiveset) \
                        and isnumeric(pdfields[f]['cdf_datatype']) \
                        and pdfields[f]['cdf_advanced_use_only'] == 'F' \
                        and self.pdgroups[group]['cdd_one_to_many'] == 'F':
                    self.objective = pdfields[f]['cdf_fieldname']
                    self.objectivegroup = group
                    self.objectiveset = True
                    print('Setting objective:' + self.objectivegroup, self.objective, pdfields[f]['cdf_datatype'])

            if pdfields[f]['cdf_type']=='LOOKUP':
                topselect=topselect+pdfields[f]['cdf_fieldname'] + '__pdlookup as '+pdfields[f]['cdf_fieldname']+', '
                self.sqlgroups[group]['lookups'].append(pdfields[f]['cdf_fieldname'])
                lutable='lu_'+str(pdfields[f]['cdf_id'])+'_'+pdfields[f]['cdf_lookup_cdl_name']
                toplu=toplu+', '+lutable+'.'+pdfields[f]['cdf_lookup_cdlf_fieldname']+' as '+pdfields[f]['cdf_fieldname']+ '__pdlookup''\n'
                bottomlu=bottomlu+'left outer join ('+str(pdfields[f]['sqltext'])+') '+lutable+' on renamedhacked.'+pdfields[f]['cdf_lookup_key_cdf_fieldname']+'='+lutable+'.'+pdfields[f]['cdf_lookup_key_cdlf_fieldname']+'\n'



        ##  remove trailing commas
        topselect=re.sub(", $","",topselect)
        hackedselect = re.sub(", $", "", hackedselect)

        newsql1=  'select '+topselect+'\n'\
               + 'from (select top ' +  sample + '  renamedhacked.* ' \
               + '\n------- TOPLU\n'\
               + toplu \
               + '\n------- END TOPLU\n' \
               + ' from ( ' \
               + 'select '+hackedselect+' from ('\
               + '\n------- HACKED\n'\
               + self.sqlgroups[group]['hacked'] \
               + '\n    ) hacked'\
               + '\n------- END HACKED\n'\
               + ' ) renamedhacked  ' \
               + '\n------- BOTTOMLU\n'\
               + bottomlu\
               + ') __outer_select'

        # Gets field type for final sql
        self.sqlgroups[group]['cdffields'], self.sqlgroups[group]['cdftypes'], self.sqlgroups[group]['cdfsizes'] \
            = get_pdfield_info(self, topselect, group, self.pddb)

        # Gets field type for final sql
        self.sqlgroups[group]['odbcfields'], self.sqlgroups[group]['odbctypes'], self.sqlgroups[group]['xmltype'], self.sqlgroups[group]['odbcsizes'] \
            = get_odbcfield_info(newsql1, group, self.pddb)

        print(self.sqlgroups[group]['odbcfields'])
        print(self.sqlgroups[group]['odbctypes'])
        print(self.sqlgroups[group]['odbcsizes'])


        ## Check matching field types
        for f in range(len(self.sqlgroups[group]['odbcfields'])):
            #print('ffff',self.sqlgroups[group]['odbcfields'][f],self.sqlgroups[group]['cdffields'][f],self.sqlgroups[group]['odbctypes'][f],self.sqlgroups[group]['cdftypes'][f])
            self.sqlgroups[group]['xmltype'][f],self.sqlgroups[group]['odbcsizes'][f]=type_check(group,self.sqlgroups[group]['odbcfields'][f],self.sqlgroups[group]['cdftypes'][f],self.sqlgroups[group]['odbctypes'][f],self.sqlgroups[group]['odbcsizes'][f])

            if self.sqlgroups[group]['xmltype'][f] == 'boolean':
                print('BOOL')
                print(topselect)
                topselect = re.sub(' (' + self.sqlgroups[group]['odbcfields'][f] + ')(,|$)',
                                      r"(case when \1='F' then 0 when \1='T' then 1 end) as \1\2",
                                   topselect)
#                print(hackedselect)
#                hackedselect = re.sub('(,|^) (' + pdfields[f]['cdf_source_fieldname'] + ') as',
#                                      r'\1(case when \2=\'F\' then 0 when \2=\'T\' then 1 end) as',
#                                      hackedselect)
                print(topselect)

        newsql2 = 'select ' + topselect + '\n' \
                  + 'from (select top ' + sample + '  renamedhacked.* ' \
                  + '\n------- TOPLU\n' \
                  + toplu \
                  + '\n------- END TOPLU\n' \
                  + ' from ( ' \
                  + 'select ' + hackedselect + ' from (' \
                  + '\n------- HACKED\n' \
                  + self.sqlgroups[group]['hacked'] \
                  + '\n    ) hacked' \
                  + '\n------- END HACKED\n' \
                  + ' ) renamedhacked  ' \
                  + '\n------- BOTTOMLU\n' \
                  + bottomlu \
                  + ') __outer_select'
        #print(self.sqlgroups[group]['cdffields'])
        #print(self.sqlgroups[group]['cdftypes'])
        #print(self.sqlgroups[group]['cdfsizes'])

        return newsql2

    def hacksql(self, group, hack):
        # concatenate the 2 sql fields (to get round 4000 character limit)
        sql = self.pdgroups[group]['ss_sql_text1'] + self.pdgroups[group]['ss_sql_text2']

        # Build tilde-separated hack list from hack['all'] + hack['groupname']
        # hack= regex1~sub1~regex2~sub2 ...
        if 'all' in hack:
            allhack = hack['all']
        else:
            allhack = 'dummy~dummy'

        if group in hack:
            hack = allhack + '~' + hack[group]
        else:
            hack = allhack

        # Execute hack replacements
        hacklist = hack.split('~')

        for x in range(0, int(len(hacklist) / 2)):
            sql = re.sub(r'' + hacklist[x * 2], r'' + hacklist[x * 2 + 1], sql)
            print('HACK', hacklist[x * 2], hacklist[x * 2 + 1])
            # For testing ~ | replacement only, only way to get a tilde into the data by hacking, as it is the hack separator!
            #self.sqlgroups[group]['hacked'] = re.sub(r'€', r'~',self.sqlgroups[group]['hacked'])

        return sql


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

            for f in range(len(self.sqlgroups[group]['odbcfields'])):

                group_field = group + '__' + re.sub('__pdsrc', '', self.sqlgroups[group]['odbcfields'][f].lower())
                try:

                    sourcename = self.sqlgroups[group]['odbcfields'][f]
                    fieldname = self.sqlgroups[group]['odbcfields'][f]
                    description = str(self.pdfields[group_field]['description'])
                except:

                    sourcename = self.sqlgroups[group]['odbcfields'][f]
                    fieldname = self.sqlgroups[group]['odbcfields'][f]
                    description = ''


                #sourcename = self.sqlgroups[group]['odbcfields'][f]
                #fieldname = self.sqlgroups[group]['odbcfields'][f]
                explorertype = self.sqlgroups[group]['xmltype'][f]
                explorersize = explorer_size(self.sqlgroups[group]['odbcsizes'][f])


                #print('field:',group_field,sourcename,fieldname,self.sqlgroups[group]['odbcfields'][f])
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
                #if re.match('.*__pdsrc', sourcename):
                #    xml_field.set('pdsource', 'true')
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



def explorer_size(size):
    if int(size) > 9999:
        ConvertedSize=None
    else:
        ConvertedSize=size

    return ConvertedSize


def error(text):

    print('ERROR:', text)
    global errors
    errors = errors + '\n' + 'ERROR: ' + text

def type_check(group,field,pdtype,odbctype,odbcsize):
    print('GFPO', group,field,pdtype,odbctype)
    if pdtype in ['string','integer','float','datetime','boolean']:
        normalisedpdtype = pdtype
    elif pdtype == "int64":
        normalisedpdtype = 'integer'
        error('Warning: Int64 pdtype for field '+field+' in group '+group)
    elif pdtype == "date":
        normalisedpdtype = 'datetime'
    else:
        error('Error: Unknown pdtype for field'+field+' in group '+group+':'+pdtype)

    if odbctype == "<class 'str'>":
        normalisedodbctype = 'string'
    elif odbctype == "<class 'datetime.datetime'>":
        normalisedodbctype = 'datetime'
    elif odbctype == "<class 'float'>":
        normalisedodbctype = 'float'
    elif odbctype == "<class 'int'>":
        normalisedodbctype = 'integer'
    elif odbctype == "<class 'decimal.Decimal'>":
        normalisedodbctype = 'float'
    elif odbctype == "<class 'long'>":
        normalisedodbctype = 'integer'
    elif odbctype == "<class 'bool'>":
        normalisedodbctype = 'boolean'
    else:
        error('Error: Unknown odbctype for field'+field+' in group '+group+':'+odbctype)

    xmltype=normalisedodbctype
    xmllength=odbcsize


    if normalisedpdtype=='boolean':
        if normalisedodbctype == 'string': # This is correct
            # Boolean in PD is 'T' / 'F' in the data, Explorer needs to be told it is a Boolean, (and data needs to be converted to 1/0 in text file)
            xmltype = 'boolean'

        elif normalisedodbctype == 'boolean':
            # In this case we don't want to pass boolean to Explorer, because we are passing a string
            error('Warning: Boolean datatype converted to string for ' + field + ' in group ' + group + '.  In PD, Boolean should be string with values T,F')
            # Below will ensure data will load.  PYPYODBC writes ('False','True')
            xmltype = 'string'
            print('xmllength',xmllength)
            xmllength = '5'

    if xmltype!=normalisedpdtype and :
        error('Error: Type mismatch for field '+field+' in group '+group+': In PD is '+normalisedpdtype+', in DB is '+normalisedodbctype)


    return xmltype, xmllength


def isnumeric(pytype):
    numerics = ['integer','float']
    return (pytype in numerics)

def get_pdfield_info(self,select,group,db):
    pdfieldsql = "SELECT cdf_order_index, cdf_source_fieldname, cdf_fieldname ,  concat([CDF_SOURCE_FIELDNAME], ' as ', [CDF_FIELDNAME]) as rename, cdf_datatype, cdf_size \
                FROM   [PDSystem].[dbo].[CUST_DOMAIN_FIELD] cdf,  [PDSystem].[dbo].[CUST_DOMAIN_DATA] cdd \
                where cdd_cd_id=" + self.domain + " and cdd.cdd_id=cdf.cdf_cdd_id  and cdd.cdd_name=N'" + group + "'  order by cdf_order_index "
    pdf=querytodict(pdfieldsql,db,2)
    sourcelist = []
    renamelist = []
    typelist = []
    sizelist = []

    select=re.sub("[^, ]* as *","",select)
    select=re.sub(" ","",select)
    print(select)
    for f in select.split(','):
        print('Field:',f,pdf[f]['cdf_fieldname'],pdf[f]['cdf_datatype'],pdf[f]['cdf_size'])
        sourcelist.append(pdf[f]['cdf_fieldname'])
        typelist.append(pdf[f]['cdf_datatype'])
        sizelist.append(pdf[f]['cdf_size'])
    return (sourcelist, typelist, sizelist)

def get_odbcfield_info(sql, group, db):
    pypyodbc.lowercase = False

    connection = pypyodbc.connect(db)
    cur = connection.cursor()

    try:
        print(sql)
        x = cur.execute(sql)
    except:
        print('#######################################################################')
        print(sql)
        error('SQL error in get_field_info for group '+group)
        print(errors)
        raise

    connection.close()
    fields = [tuple[0] for tuple in x.description]
    datatypes = [tuple[1] for tuple in x.description]
    datasizes = [tuple[3] for tuple in x.description]

    fieldlist = []
    typelist = []
    xmltypelist = []
    sizelist = []
    for i in range(len(fields)):
        fieldlist.append(fields[i])
        typelist.append(str(datatypes[i]))
        xmltypelist.append(str(datatypes[i]))
        sizelist.append(str(datasizes[i]))
    return (fieldlist, typelist, xmltypelist, sizelist)


def querytolist(sql, db):
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
    connection = pypyodbc.connect(db)

    cursor = connection.cursor()
    # cur.execute("set character_set_results = 'latin1'")
    try:
        x = cursor.execute(sql)
    except:
        print('#######################################################################')
        print(sql)
        error('SQL error in query to dict:')
        print(errors)
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
        error('SQL error in test SQL:')
        print(errors)
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

            csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n", escapechar='\\', quotechar='"', delimiter=',').writerows(cursor)

    connection.close()


def main():

    pe = ExplorerDomain()


    end = time.strftime("%Y%m%d-%H%M%S")
    print('start',now)
    print('end  ',end)

main()

print('Nearly Done!!')
print(errors)
print('Done!!')
