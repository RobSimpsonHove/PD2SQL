# -*- encoding: utf-8 -*-
########################################  FROM UKPASUITE 26 Oct 2016
## Author: Rob Simpson

import os
import pypyodbc;

pypyodbc.lowercase = True
import re
import time
import csv
import codecs
import warnings
import xml.etree.ElementTree as ET
import sys

try:
    refreshgroups = sys.argv[1]
except:
    refreshgroups = None

if refreshgroups:
    print('Only exporting refresh groups:', refreshgroups)

################## DEFAULT SETTINGS - usually configured in local.py ########################
domain = '1001'  # Portrait Dialogue domain to target
dsn = 'PDSystem'  # Windows ODBC connection to PD System tables
data_dir = 'D:/PortraitAnalytics/data'  # Location for data extracts

write_flat_files = True
sample = '100 percent'  # eg. '100' (records) or '100 percent' for flat file export.
samplecusts = False
custlike = '%12'  # %12 is 1% sample, %123 is 0.1% sample

# groups='foo1,foo2,...'  # OPTIONAL: groups returns those listed
# xgroups='bar1,bar2,...'  # OPTIONAL: xgroups returns all those but listed
# objective=group.fieldname # OPTIONAL: Objective field, defaults to first numeric non-key

## Hacks:  tilde-separated pair(s) of regex, used to adapt generated SQL
hack = {}
## hack['all']=regex1~replace1~regex2~replace2 (...)
## hack['all'] = '\Z~where $key like \'%123\''  ## 0.1% sampledir
## hack['group'] applies to group SQL only, hack['all'] applies to all group SQL

# eg. hack['all'] = '{MSSQL_?NOLOCK}~~\.dbo\.~.[dbo].'
#hack['all'] = 'CUSTOMERID = :CustomerNumber~1=1'

#####  ANYTHING ABOVE HERE CAN BE RECONFIGURED IN local.py FILE #############
##### Import any local overrides for the variables above.
try:
    exec(open('local.py').read())
except IOError:
    pass

if database=="MSS":
    import pdsys_sqlMSS as sql  # Local file with system table SQL
elif database=="Oracle":
    import pdsys_sqlOra as sql  # Local file with system table SQL
else:
    print('No valid database specified in loacl.py')
    raise



import pdsys_sqlOra as sql  # Local file with system table SQL

testsql = True
replacekeys = False
allowstrings = False

warnings.simplefilter('error', UnicodeWarning)

errors = ''
now = time.strftime("%Y%m%d-%H%M%S")
data_dirnow = data_dir + '\\' + now


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
        self.pdgroups = querytodict(tablesql % (self.domain, self.domain), self.pddb, 1)
        self.pdlookups = querytodict(lookupsql % self.domain, self.pddb, 2)
        self.pdfields = querytodict(fieldsql, self.pddb, 0)

        # Identify the main, onetoone, onetomany groups
        self.make_group_lists()
        print('Main:',self.main)
        if refreshgroups and self.main in refreshgroups.split(','):
            print('ERROR: cannot do a partial refresh with main group '+self.main+'.  Main group needs a complete rerun')
            quit()
        print('OnetoOne:', self.onetoone)
        print('OnetoMany:', self.onetomany)

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
                print('Processing group:', group)
                self.sqlgroups[group] = {}

                ## Get key for group
                #print('keys:',group, self.pdgroups[group]['cdp_paramname'],self.pdgroups[group]['cdd_key'], self.pdgroups[group]['cdd_parent_cdd_id'] )
                # CDD_
                if self.pdgroups[group]['cdd_parent_cdd_id'] == 'NULL' or self.pdgroups[group][
                    'cdd_parent_cdd_id'] == None:
                    self.sqlgroups[group]['key'] = self.pdgroups[group]['cdd_id_source_fieldname']

                elif self.pdgroups[group]['cdp_paramname'] == 'NULL' or self.pdgroups[group]['cdp_paramname'] == None:
                    warning('KEY error on ' + group)
                    print(errors)
                    raise

                else:
                    self.sqlgroups[group]['key'] = self.pdgroups[group]['cdp_paramname']

                ## Replace any given group.hack or all.hack
                self.sqlgroups[group]['hacked'] = self.hacksql(group, hack)

                newsql = self.build_sql(group, self.pddb)
                # print('NEWSQL:\n',newsql)

                # Write SQL statement out
                f = open(data_dirnow + '\\' + group + '.sql', 'w', encoding="utf-8")
                f.write(newsql)
                f.close()

                if testsql:
                    test_sql(self, self.pddb, newsql, group)

                if write_flat_files:
                    if not(refreshgroups) or group in refreshgroups.split(','):
                        print('WRITING ',group)
                        write_flatfiles(self.pddb, newsql, group)

        return self.sqlgroups

    def build_sql(self, group, db):
        # print('Building SQL for group ',group)

        pdfieldsql = sql.pdfieldsql
        pdfields = querytodict(pdfieldsql % (self.domain, group), db, 1)
        # print('pdfields',pdfields)

        topselect = ''
        hackedselect = ''
        toplu = ''
        bottomlu = ''

        self.sqlgroups[group]['lookups'] = []
        self.sqlgroups[group]['origkey'] = self.sqlgroups[group]['key']

        # print('SETTING KEY1', group, self.sqlgroups[group]['key'])
        for f in pdfields:
            #print('f17', pdfields[f])

            if pdfields[f]['cdf_type'] == 'DATA':

                if self.sqlgroups[group]['key'] == pdfields[f]['cdf_fieldname']:
                    self.sqlgroups[group]['origkey'] = pdfields[f]['cdf_source_fieldname']
                    # print('SETTING KEY3', group, pdfields[f]['cdf_source_fieldname'])

                if self.sqlgroups[group]['key'] == pdfields[f]['cdf_source_fieldname']:
                    self.sqlgroups[group]['key'] = pdfields[f]['cdf_fieldname']
                    self.sqlgroups[group]['origkey'] = pdfields[f]['cdf_source_fieldname']
                    # print('SETTING KEY2',group,pdfields[f]['cdf_source_fieldname'])

                hackedselect = hackedselect + pdfields[f]['cdf_source_fieldname'] + ' as ' + pdfields[f][
                    'cdf_fieldname'] + ', '

                if (pdfields[f]['cdf_advanced_use_only'] == 'F' and (
                    allowstrings or pdfields[f]['cdf_datatype'] != 'string')) or self.sqlgroups[group]['key'] == \
                        pdfields[f]['cdf_fieldname']:
                    topselect = topselect + pdfields[f]['cdf_fieldname'] + ', '

                if not (self.objectiveset) \
                        and isnumeric(pdfields[f]['cdf_datatype']) \
                        and pdfields[f]['cdf_advanced_use_only'] == 'F' \
                        and self.pdgroups[group]['cdd_one_to_many'] == 'F':
                    self.objective = pdfields[f]['cdf_fieldname']
                    self.objectivegroup = group
                    self.objectiveset = True
                    print('Setting objective:' + self.objectivegroup, self.objective, pdfields[f]['cdf_datatype'])

            if pdfields[f]['cdf_type'] == 'LOOKUP':
                topselect = topselect + pdfields[f]['cdf_fieldname'] + '__lu as ' + pdfields[f]['cdf_fieldname'] + ', '
                self.sqlgroups[group]['lookups'].append(pdfields[f]['cdf_fieldname'])
                lutable = 'lu_' + str(pdfields[f][
                                          'cdf_id'])  # +'_'+pdfields[f]['cdf_lookup_cdl_name']  ## Too long an identifier for Oracle
                toplu = toplu + ', ' + lutable + '.' + pdfields[f]['cdf_lookup_cdlf_fieldname'] + ' as ' + pdfields[f][
                    'cdf_fieldname'] + '__lu''\n'
                bottomlu = bottomlu + 'left outer join (' + str(
                    pdfields[f]['sqltext']) + ') ' + lutable + ' on renamed.' + pdfields[f][
                               'cdf_lookup_key_cdf_fieldname'] + '=' + lutable + '.' + pdfields[f][
                               'cdf_lookup_key_cdlf_fieldname'] + '\n'

        ##  remove trailing commas
        topselect = re.sub(", $", "", topselect)
        hackedselect = re.sub(", $", "", hackedselect)
        # print('ZZZ')
        # print(self.sqlgroups[group]['hacked'])
        # self.sqlgroups[group]['hacked'] = re.sub("\$key",  origkey, self.sqlgroups[group]['hacked'])

        # print('XXX')
        # print(self.sqlgroups[group]['hacked'])
        newsql1 = 'select ' + topselect + '\n' \
                  + 'from (select   renamed.* ' \
                  + '\n------- TOPLU\n' \
                  + toplu \
                  + '\n------- END TOPLU\n' \
                  + ' from ( ' \
                  + 'select ' + hackedselect + ' from (' \
                  + '\n------- HACKED\n' \
                  + self.sqlgroups[group]['hacked'] \
                  + '\n------- END HACKED\n' \
                  + ' ) renamed  ' \
                  + '\n------- BOTTOMLU\n' \
                  + bottomlu \
                  + ') outer_select'

        # Gets field type for final sql
        self.sqlgroups[group]['cdffields'], self.sqlgroups[group]['cdftypes'], self.sqlgroups[group]['cdfsizes'] \
            = get_pdfield_info(self, topselect, group, self.pddb)

        # Gets field type for final sql
        self.sqlgroups[group]['odbcfields'], self.sqlgroups[group]['odbctypes'], self.sqlgroups[group]['xmltype'], \
        self.sqlgroups[group]['odbcsizes'], self.sqlgroups[group]['odbcprecision'], self.sqlgroups[group]['odbcscale'] \
            = get_odbcfield_info(self, newsql1, group, self.pddb)

        if self.sqlgroups[group]['odbcfields'] != 'ERROR':

            ## Check matching field types
            for f in range(len(self.sqlgroups[group]['odbcfields'])):

                self.sqlgroups[group]['xmltype'][f], self.sqlgroups[group]['odbcsizes'][f], output = type_check(group,
                                                                                                                self.sqlgroups[
                                                                                                                    group][
                                                                                                                    'odbcfields'][
                                                                                                                    f],
                                                                                                                self.sqlgroups[
                                                                                                                    group][
                                                                                                                    'cdftypes'][
                                                                                                                    f],
                                                                                                                self.sqlgroups[
                                                                                                                    group][
                                                                                                                    'odbctypes'][
                                                                                                                    f],
                                                                                                                self.sqlgroups[
                                                                                                                    group][
                                                                                                                    'odbcsizes'][
                                                                                                                    f],
                                                                                                                self.sqlgroups[
                                                                                                                    group][
                                                                                                                    'odbcprecision'][
                                                                                                                    f],
                                                                                                                self.sqlgroups[
                                                                                                                    group][
                                                                                                                    'odbcscale'][
                                                                                                                    f])
                if output:
                    warning(output)

                if self.sqlgroups[group]['xmltype'][f] == 'boolean':
                    ## Manipulate the data - 'T'->1, 'F-'>0
                    # print('Before:',topselect)
                    # print(self.sqlgroups[group]['odbcfields'][f])
                    # print(''+self.sqlgroups[group]['odbcfields'][f]+'__lu')
                    if re.search('' + self.sqlgroups[group]['odbcfields'][f] + '__lu', topselect):
                        # fieldname_pdlookus  -> (case when fieldname_pdlookus="F" then 0 ... )
                        topselect = re.sub(' (' + self.sqlgroups[group]['odbcfields'][f] + '__lu)',
                                           r"(case when \1='F' then 0 when \1='T' then 1 end)", topselect)
                    else:
                        topselect = re.sub(' (' + self.sqlgroups[group]['odbcfields'][f] + ')(,|$)',
                                           r"(case when \1='F' then 0 when \1='T' then 1 end) as \1\2", topselect)

                        # if self.sqlgroups[group]['xmltype'][f] == 'boolean':
                        #    topselect = re.sub(' (' + self.sqlgroups[group]['odbcfields'][f] + ')(,|$)',
                        #                       r"(case when \1='F' then 0 when \1='T' then 1 end) as \1\2",
                        #                       topselect)

            if samplecusts:
                sample_sql = ' where ' + self.sqlgroups[group]['origkey'] + ' like \'' + custlike + '\''
            else:
                sample_sql = ''

            newsql2 = 'select ' + topselect + '\n' \
                      + 'from (select   renamed.* ' \
                      + '\n------- TOPLU\n' \
                      + toplu \
                      + '\n------- END TOPLU\n' \
                      + ' from ( ' \
                      + 'select ' + hackedselect + ' from (' \
                      + '\n------- HACKED\n' \
                      + self.sqlgroups[group]['hacked'] \
                      + sample_sql \
                      + '\n------- END HACKED\n' \
                      + ' ) renamed  ' \
                      + '\n------- BOTTOMLU\n' \
                      + bottomlu \
                      + ') outer_select'

        else:
            newsql2 = 'ERROR'

        return newsql2

    def hacksql(self, group, hack):
        sql = self.pdgroups[group]['ss_sql_text'] + '\n    ) hacked ' \
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
            # print('HACK', hacklist[x * 2], hacklist[x * 2 + 1])
            # For testing ~ | replacement only, only way to get a tilde into the data by hacking, as it is the hack separator!
            # self.sqlgroups[group]['hacked'] = re.sub(r'€', r'~',self.sqlgroups[group]['hacked'])

        return sql

    def create_xml(self):
        root = ET.Element('dataobjects', xsi="http://www.w3.org/2001/XMLSchema-instance",
                          xsd="http://www.w3.org/2001/XMLSchema",
                          xmlns="http://services.analytics.portrait.pb.com/")

        dataobject = self.write_group_xml(root, 'dataobject', [self.main], 'main')

        xml_dataobjects = ET.SubElement(dataobject, 'dataobjects')
        junk = self.write_group_xml(xml_dataobjects, 'dataobject', self.onetoone, 'one2one')

        xml_dataobjectcollections = ET.SubElement(dataobject, 'dataobjectcollections')
        junk = self.write_group_xml(xml_dataobjectcollections, 'dataobjectcollection', self.onetomany, 'onetomany')

        outFile = open(data_dirnow + '\\ADSmetadata.xml', mode="wb")
        doc = ET.ElementTree(root)
        outFile.write(codecs.BOM_UTF8)
        doc.write(outFile, encoding="utf-8", xml_declaration=True)
        outFile.close()

    def write_group_xml(self, parent, name, list, level):
        # print( 'Doing', list)
        xml_object = None
        for group in list:
            xml_object = ET.SubElement(parent, name, tablename=group, displayname=group, name=group, visible="true")

            if level != 'onetomany':
                xml_primarykeys_g = ET.SubElement(xml_object, 'primarykeys')
                xml_primarykey_g = ET.SubElement(xml_primarykeys_g, 'primarykey',
                                                 columnname=self.sqlgroups[group]['key'])

            if level != 'main':
                xml_foreignkeys_g = ET.SubElement(xml_object, 'foreignkeys')
                xml_foreignkey_g = ET.SubElement(xml_foreignkeys_g, 'foreignkey',
                                                 columnname=self.sqlgroups[group]['key'])

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

                # sourcename = self.sqlgroups[group]['odbcfields'][f]
                # fieldname = self.sqlgroups[group]['odbcfields'][f]
                explorertype = self.sqlgroups[group]['xmltype'][f]
                explorersize = explorer_size(self.sqlgroups[group]['odbcsizes'][f])

                # print('field:',group_field,sourcename,fieldname,self.sqlgroups[group]['odbcfields'][f])
                xml_field = ET.SubElement(xml_fields_g, 'field', columnname=sourcename, type=explorertype,
                                          name=fieldname)

                # Where the type is group, give me pdtype=”<explorertype>”.
                # Where the type (or if type=group, pdtype) is string, give me pdlength=”<nchars>”.

                if sourcename in self.sqlgroups[group]['lookups']:
                    xml_field.set('type', 'group')
                    xml_field.set('pdtype', explorertype)
                if explorertype == 'string' and explorersize != None:
                    xml_field.set('pdlength', explorersize)

                # Flag sources and objective
                # if re.match('.*__pdsrc', sourcename):
                #    xml_field.set('pdsource', 'true')
                if level != 'onetomany' and explorertype != 'string':
                    xml_field.set('genius', 'true')

                if sourcename == self.objective and group == self.objectivegroup:
                    xml_field.set('objective', 'true')
                    xml_field.set('genius', 'false')

        return xml_object

    def write_statusfile(self):
        f = open(data_dir + '\\status.properties', 'w')
        f.write('[Python]\n')
        f.write('export=' + now + '\n')
        f.write('separator=,\n')
        f.write('quote="\n')
        f.write('status=pending\n')
        if refreshgroups:
            f.write('groups=' + refreshgroups + '\n')
        f.write('\n')
        f.write('domain=' + self.domain + '\n')
        f.close()

        # [Python]
        # export=20160226-033330
        # status=pending


def explorer_size(size):
    try:
        if int(size) > 9999:
            ConvertedSize = None
        else:
            ConvertedSize = size
    except:
        ConvertedSize = size

    return ConvertedSize


def warning(text):
    print(text)
    global errors
    errors = errors + '\n' + text


def isnumeric(pytype):
    numerics = ['integer', 'float']  # Not Int64, unsuitable for objective
    return (pytype in numerics)


def get_pdfield_info(self, select, group, db):
    # pdfieldsql2 = "SELECT cdf_order_index, cdf_source_fieldname, cdf_fieldname ,  concat(CDF_SOURCE_FIELDNAME, ' as ', [CDF_FIELDNAME]) as rename, cdf_datatype, cdf_size \
    #            FROM   PDSystem.dbo.CUST_DOMAIN_FIELD cdf,  PDSystem.dbo.CUST_DOMAIN_DATA cdd \
    #            FROM   PDSystem.dbo.CUST_DOMAIN_FIELD cdf,  PDSystem.dbo.CUST_DOMAIN_DATA cdd \
    #            where cdd_cd_id=" + self.domain + " and cdd.cdd_id=cdf.cdf_cdd_id  and cdd.cdd_name=N'" + group + "'  order by cdf_order_index "
    pdfieldsql2 = sql.pdfieldsql2
    print(pdfieldsql2)
    pdf = querytodict(pdfieldsql2 % (self.domain, group), db, 2)  ## Key on cdf_fieldname
    sourcelist = []
    typelist = []
    sizelist = []

    # trim select stalement to just resultant field names
    fields = re.sub("[^, ]* as *", "", select)
    fields = re.sub(" ", "", fields)
    for f in fields.split(','):
        # print('Field:',f,pdf[f]['cdf_fieldname'],pdf[f]['cdf_datatype'],pdf[f]['cdf_size'])
        sourcelist.append(pdf[f]['cdf_fieldname'])
        typelist.append(pdf[f]['cdf_datatype'])
        sizelist.append(pdf[f]['cdf_size'])
    return (sourcelist, typelist, sizelist)


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
    print('DB is:', db)
    print('SQL is:', sql)
    connection = pypyodbc.connect(db)

    cursor = connection.cursor()
    # cur.execute("set character_set_results = 'latin1'")
    try:
        x = cursor.execute(sql)
    except:
        print('#######################################################################')
        print(sql)
        warning('SQL error in query to dict:')
        print(errors)
        raise

    rows = x.fetchall()
    print(x.description)
    fields = [tuple[0] for tuple in x.description]
    types = [tuple[1] for tuple in x.description]
    sizes = [tuple[3] for tuple in x.description]

    d = {}

    for r in rows:
        # print(r)
        d[r[n]] = {}
        d[r[n]] = dict(zip(fields, r))
    connection.close()

    return d


def test_sql(self, db, sql, group):
    if not (sql == 'ERROR'):
        connection = pypyodbc.connect(db)
        cursor = connection.cursor()
        countrecords = 'select count(*),  count(distinct ' + self.sqlgroups[group]['key'] + ') from (' + sql + ' ) xxx'

        try:
            x = cursor.execute(countrecords)
            row = x.fetchone()

            print('SQL okay for', group, ': ', row[0], 'records')
            if group not in self.onetomany and row[0] != row[1]:
                warning('ERROR: duplicate keys for onetoone group ' + group)
                warning('ERROR: DROPPING group ' + group)
                foo = [x for x in self.onetoone if x not in [group]]
                self.onetoone = foo

                # print(' sample is top '+ sample)
        except:

            print('#######################################################################')
            print(sql)
            warning('SQL error in test SQL:')
            print(errors)
            raise


def write_flatfiles(db, sql, name):
    if not (sql == 'ERROR'):
        connection = pypyodbc.connect(db)
        cursor = connection.cursor()
        cur = cursor.execute(sql)

        if not os.path.exists(data_dirnow + '\\' + name + '_wrap'):
            os.makedirs(data_dirnow + '\\' + name + '_wrap')

        with open(data_dirnow + '\\' + name + '_wrap\\export.txt', 'w', encoding='utf-8') as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n", escapechar='\\', quotechar='"',
                       delimiter=',').writerows(cursor)

        connection.close()


def get_odbcfield_info(self, sql, group, db):
    connection = pypyodbc.connect(db)
    cur = connection.cursor()

    try:
        # print(sql)
        x = cur.execute(sql)
    except:
        print('#######################################################################')
        print(sql)
        print('#######################################################################')
        warning('ERROR: SQL error in get_odbcfield_info for group ' + group)
        warning('ERROR: DROPPING group ' + group)
        foo = [x for x in self.onetoone if x not in [group]]
        self.onetoone = foo
        foo = [x for x in self.onetomany if x not in [group]]
        self.onetomany = foo
        return ('ERROR', 'ERROR', 'ERROR', 'ERROR', 'ERROR', 'ERROR')

    print(x.description)
    connection.close()
    fields = [tuple[0] for tuple in x.description]
    datatypes = [tuple[1] for tuple in x.description]
    datasizes = [tuple[3] for tuple in x.description]
    dataprecision = [tuple[4] for tuple in x.description]
    datascale = [tuple[5] for tuple in x.description]

    fieldlist = []
    typelist = []
    xmltypelist = []
    sizelist = []
    precisionlist = []
    scalelist = []

    for i in range(len(fields)):
        fieldlist.append(fields[i])
        typelist.append(str(datatypes[i]))
        xmltypelist.append(str(datatypes[i]))
        sizelist.append(str(datasizes[i]))
        precisionlist.append(str(dataprecision[i]))
        scalelist.append(str(datascale[i]))
    return (fieldlist, typelist, xmltypelist, sizelist, precisionlist, scalelist)


def type_check(group, field, pdtype, odbctype, odbcsize, odbcprecision, odbcscale):
    errors = ''
    ## Set type for Explorer (Monet tables and PE-PD integration)
    pe_xmllength = odbcsize

    if odbctype == "<class 'str'>":
        if pdtype == 'boolean':
            ## This is a special case and we will frig the text file to make sure it loads correctly.
            ## Boolean in PD is 'T' / 'F' in the data, Explorer needs to be told it is a Boolean, (and data needs to be converted to 1/0 in text file)
            pe_xmltype = 'boolean'
        else:
            pe_xmltype = 'string'
    elif odbctype == "<class 'bool'>":
        pe_xmltype = 'string'
        pe_xmllength = '5'
    elif odbctype == "<class 'datetime.datetime'>":
        pe_xmltype = 'datetime'
    elif odbctype == "<class 'float'>":
        pe_xmltype = 'float'
    elif odbctype == "<class 'decimal.Decimal'>":
        if int(odbcprecision) <= 9 and odbcscale == '0':
            pe_xmltype = 'integer'
        else:
            pe_xmltype = 'float'
    elif odbctype == "<class 'int'>" or odbctype == "<class 'long'>":
        pe_xmltype = 'integer'
    else:
        print(
            'ERROR: Unknown odbctype for field ' + field + ' in group ' + group + ': ODBCTYPE is' + odbctype + '\n', )  # Should we bomb out, or set pe_xml* to None??
        errors = errors + 'ERROR: Unknown odbctype for field ' + field + ' in group ' + group + ':' + odbctype + '\n'  # Should we bomb out, or set pe_xml* to None??
        pe_xmltype = 'None'


        ## Check for where decimal ODBC was int64 in PD.
        # if pdtype=="int64" and pe_xmltype=="float":
        #   errors = errors+'Caution: Using float (for safety) to hold int64 data in PE for field ' + field + ' in group ' + group + '.\n'

    ## Check PDtype similar to PEtype for selections to work
    ## First establish pdtype family ...
    if pdtype in ['string', 'integer', 'float', 'datetime', 'boolean']:
        pdtypefamily = pdtype
    elif pdtype == "int64":
        pdtypefamily = 'integer'
    elif pdtype == "date":
        pdtypefamily = 'datetime'
    else:
        errors = errors + 'ERROR: Unknown pdtype for field ' + field + ' in group ' + group + ':' + pdtype + '\n'  # Can this ever happen?

    ## ... then check pdtype similar to petype

    if pdtypefamily != pe_xmltype:
        errors = errors + 'Warning: Field ' + field + ' in group ' + group + ' will not be usable in saved selections: In PD is type ' + pdtype + ' (requiring ' + pdtypefamily + ' in PE), but in PE it is type ' \
                 + pe_xmltype + ', in DB it is type ' + odbctype \
                 + ', precision ' + str(odbcprecision) \
                 + ', scale ' + odbcscale + '.\n'

    return pe_xmltype, pe_xmllength, errors


def main():
    start = now
    print('start', start)
    pe = ExplorerDomain()

    print('\n\nCollated errors:')
    print(errors)
    print('\nDone!!')
    print('start', start)
    print('end  ', time.strftime("%Y%m%d-%H%M%S"))


main()

