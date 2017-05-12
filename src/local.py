
## Domain to extract
domain = '1002'  ## or appropriate domain id

## MHSystem DSN
# dsn = 'PDSystem'  ## whatever ODBC name to connect to PDSystem data
# or dsn='PDSystemTest;Uid=username;Pwd=password'
PDDsn='PDSYSTEM;Uid=MAJOR;Pwd=MAJOR'
## Domain data DSN
DataDsn='PDSYSTEM;Uid=MAJOR;Pwd=MAJOR'

## Where to write data to
data_dir='C:/Analytics/Data'  ## any appropriate location for data extracts visable to Explorer box

## Sample data
sample = '2'  ## eg. '100' (records) for flat file export.

database="Oracle"  ## Oracle or MSS

