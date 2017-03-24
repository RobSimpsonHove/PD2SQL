set logs=..\..\..\logs
set logbase=%logs%\pd2sql
set loglines=250000

mkdir %logs%
set PYTHONIOENCODING=utf-8
python pd2sql.py >> %logbase%.log 2>&1

REM chop all but last %loglines% rows off the log
PowerShell -Command "& {Get-Content -Tail %loglines% %logbase%.log -Encoding UTF8 | Out-File -FilePath %logbase%.tmp -Encoding UTF8}"
PowerShell -Command "& {Move -Force %logbase%.tmp %logbase%.log}"
