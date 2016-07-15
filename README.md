# PD2SQL Extracts data or SQL from Portrait Dialogue domain
# Use in conjunction with FF2PE to load flat files into Explorer

# Files:
*pd2sql.py* - Generates SQL, and/or flat files for selected groups in Portrait Dialogue

*pd2sql.properties*   - Configuration, points to Domain, database, and optionally selected groups

*pdsys_sql.py*        - Contains sql for extracting tables, lookups and fields from PDSYS

# Usage:

> python pdsql.py {path}/pd2sql.properties (datadir)

# Output:

- (datadir)/status_properties
- (datadir)/ADSmetadata                          Points to datestamp dirs, flat file format
- (datadir)/YYYYMMDD-HHMMSS/                     Identifies tables structures and relationships
- (datadir)/YYYYMMDD-HHMMSS/Group1_wrap/         Flat file separated data
- (datadir)/YYYYMMDD-HHMMSS/Group1_wrap/export.txt
- (datadir)/YYYYMMDD-HHMMSS/Group2_wrap/
- (datadir)/YYYYMMDD-HHMMSS/Group2_wrap/export.txt
and so on


# Script configuration (defaults):

-  replacekeys = True     # change them all to main table key, or leave as is
-  allowsources = False   # maintain lookup source fields eg. gender as gender_pdsrc
-  allowstrings = False   # Allow strings that are not lookups to propagate, (eg. perhaps for Cards table,
                          # otherwise these fields are invisible & unusable in Explorer)
-  write_sql = False      # Write SQL to results directory
-  write_flat_files = True  # Write the flat files with the data in, in format suitable for FF2PE.py
-  top_percent = 100      # sample the data in flat file export

I suggest running initially with (write_sql=True, top_percent=0.01) initially to check groups,
and that SQL is okay or needs a hack