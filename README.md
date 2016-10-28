# PD2SQL Extracts data or SQL from Portrait Dialogue domain
# Use in conjunction with FF2PE to load flat files into Explorer

# Files:
*pd2sql.py* - Generates SQL, and/or flat files for selected groups in Portrait Dialogue

*pdsys_sql.py*  - Contains sql for extracting tables, lookups and fields from PDSYS

*local.py*   - local file only, overrides default configuration

# Usage:

> python pdsql.py 

# Output:

- (datadir)/status_properties                    Points to datestamp dirs, flat file format
- (datadir)/YYYYMMDD-HHMMSS/                     
- (datadir)/YYYYMMDD-HHMMSS/ADSmetadata.html     Identifies tables structures and relationships
- (datadir)/YYYYMMDD-HHMMSS/Group1_wrap/         Flat file separated data
- (datadir)/YYYYMMDD-HHMMSS/Group1_wrap/export.txt
- (datadir)/YYYYMMDD-HHMMSS/Group2_wrap/
- (datadir)/YYYYMMDD-HHMMSS/Group2_wrap/export.txt
and so on


# Script configuration (defaults):domain = '1001'   # Portrait Dialogue domain to target

- domain = '1001'   # Portrait Dialogue domain to target
- dsn = 'PDSystem'  # Windows ODBC connection to PD System tables
- data_dir = 'D:/PortraitAnalytics/data' # Location for data extracts

- write_flat_files = True
- sample = '100 percent' # eg. '100' (records) or '100 percent' for flat file export.

# OPTIONAL
-  groups='foo1,foo2,...'  ## OPTIONAL: groups returns those listed
-  xgroups='bar1,bar2,...'  ## OPTIONAL: xgroups returns all those but listed
-  objective=group.fieldname ## OPTIONAL: Objective field, defaults to first numeric non-key

- ##### Hacks:  tilde-separated pair(s) of regex, used to adapt generated SQL
- ##### hack['all']=regex1~replace1~regex2~replace2 (...)
- ##### hack['group'] applies to group SQL only, hack['all'] applies to all group SQL
- hack = {}
- hack['all'] = '{MSSQL_?NOLOCK}~ ~\.dbo\.~.[dbo].'
- hack['TransГroup'] = 'SandboxDatabase~[SandboxDatabase]'
 
