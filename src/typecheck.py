## For PYPYODBC

def type_check(group, field, pdtype, odbctype, odbcsize):
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
    elif odbctype == "<class 'float'>" or odbctype == "<class 'decimal.Decimal'>":
        pe_xmltype = 'float'
    elif odbctype == "<class 'int'>" or odbctype == "<class 'long'>":
        pe_xmltype = 'integer'
    else:
        errors = errors+'ERROR: Unknown odbctype for field' + field + ' in group ' + group + ':' + odbctype +'\n'  # Should we bomb out, or set pe_xml* to None??
        pe_xmltype = None


    ## Check for where decimal ODBC was int64 in PD.
    if pdtype=="int64" and pe_xmltype=="float":
        errors = errors+'Caution: Using float to hold int64 data in PE for safety for field' + field + ' in group ' + group + '.\n'


    ## Check PDtype similar to PEtype for selections to work
    ## First establish pdtype family ...
    if pdtype in ['string', 'integer', 'float', 'datetime', 'boolean']:
        pdtypefamily = pdtype
    elif pdtype == "int64":
        pdtypefamily = 'integer'
    elif pdtype == "date":
        pdtypefamily = 'datetime'
    else:
        errors = errors+'ERROR: Unknown pdtype for field' + field + ' in group ' + group + ':' + pdtype +'\n'  # Can this ever happen?

    ## ... then check pdtype similar to petype
    if pdtypefamily != pe_xmltype:
        errors = errors+'Warning: Field ' + field + ' in group ' + group + ' will not be usable in saved selections: In PD is type ' + pdtype + ' (matching ' +pdtypefamily + ' in PE), but in PE it is type ' + pe_xmltype + '.\n'

    return pe_xmltype, pe_xmllength, errors
