## For PYPYODBC
import pypyodbc

def get_odbcfield_info(self,sql, group, db):
    pypyodbc.lowercase = False

    connection = pypyodbc.connect(db)
    cur = connection.cursor()

    try:
        #print(sql)
        x = cur.execute(sql)
    except:
        print('#######################################################################')
        print(sql)
        print('#######################################################################')
        warning('ERROR: SQL error in get_odbcfield_info for group ' + group)
        warning('ERROR: DROPPING group ' + group)
        foo=[x for x in self.onetoone if x not in [group]]
        self.onetoone=foo
        foo=[x for x in self.onetomany if x not in [group]]
        self.onetomany=foo
        return('ERROR','ERROR','ERROR','ERROR','ERROR','ERROR')

    #print(x.description)
    connection.close()
    fields = [tuple[0] for tuple in x.description]
    datatypes = [tuple[1] for tuple in x.description]
    datasizes = [tuple[3] for tuple in x.description]
    dataprecision = [tuple[4] for tuple in x.description]
    datascale = [tuple[5] for tuple in x.description]

    fieldlist =  []
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


def type_check(group, field, pdtype, odbctype, odbcsize, odbcprecision, odbcscale ):
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
        if int(odbcprecision) <= 9 and odbcscale=='0':
            pe_xmltype = 'integer'
        else:
            pe_xmltype = 'float'
    elif odbctype == "<class 'int'>" or odbctype == "<class 'long'>":
        pe_xmltype = 'integer'
    else:
        print('ERROR: Unknown odbctype for field ' + field + ' in group ' + group + ': ODBCTYPE is' + odbctype +'\n',)  # Should we bomb out, or set pe_xml* to None??
        errors = errors+'ERROR: Unknown odbctype for field ' + field + ' in group ' + group + ':' + odbctype +'\n'  # Should we bomb out, or set pe_xml* to None??
        pe_xmltype = 'None'


    ## Check for where decimal ODBC was int64 in PD.
    #if pdtype=="int64" and pe_xmltype=="float":
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
        errors = errors+'ERROR: Unknown pdtype for field ' + field + ' in group ' + group + ':' + pdtype +'\n'  # Can this ever happen?

    ## ... then check pdtype similar to petype

    if pdtypefamily != pe_xmltype:
        errors = errors+'Warning: Field ' + field + ' in group ' + group + ' will not be usable in saved selections: In PD is type ' + pdtype + ' (requiring ' +pdtypefamily + ' in PE), but in PE it is type '\
        + pe_xmltype + ', in DB it is type '+odbctype\
        +', precision '+str(odbcprecision)\
        +', scale '+odbcscale+'.\n'

    return pe_xmltype, pe_xmllength, errors
