tablesql = """
select * from (
select
--sd.SD_SQL_NAME,
cdd.cdd_id,
cdd.cdd_name,
cdd.cdd_parent_cdd_id,
cdd.[CDD_ONE_TO_MANY],
ss.ss_database_plugin,
ss.SS_SQL_TEXT,
convert(nvarchar(4000),substring( ltrim(rtrim(ss.SS_SQL_TEXT)),1,4000)) as SS_SQL_TEXT1,
convert(nvarchar(4000),substring( ltrim(rtrim(ss.SS_SQL_TEXT)),4001,8000)) as SS_SQL_TEXT2,
cdd.CDD_ID_SOURCE_FIELDNAME as CDD_KEY
FROM [SQL_DEFINITION] as sd,
[SQL_STATEMENT] as ss,
[CUST_DOMAIN_DATA] as cdd
where sd.SD_ID = ss.SS_SD_ID
and  ss.SS_SD_ID= cdd.CDD_SD_ID
and cdd.CDD_IS_SYSTEM_GROUP='F'
and cdd.CDD_ADVANCED_USE_ONLY='F'
and cdd.CDD_CD_ID='%s'
) a
                left join
(
SELECT
cdf1.cdf_source_fieldname as cdf1,
cdf2.cdf_source_fieldname as cdf2,
cdf2.cdf_cdd_id
FROM [CUST_DOMAIN_PARAM]
inner join CUST_DOMAIN_DATA
on cdd_id = cdp_cdd_id
inner join cust_domain_field as cdf2
on cdf2.cdf_cdd_id = cdd_id
and cdf2.cdf_fieldname = CDP_PARAMNAME
inner join cust_domain_field as cdf1
on cdf1.cdf_cdd_id = cdd_parent_cdd_id
and cdf1.cdf_fieldname = CDP_BOUND_TO_FIELDNAME
--and cdf1.cdf_source_fieldname='${mainkey}'  NOT SURE ABOUT THIS
) cdf
on  cdf.CDF_CDD_ID=a.CDD_ID
where ss_database_plugin = 'SQLSERVER' or ss_database_plugin is null"""

lookupsql = """
SELECT distinct
--
d.cdd_name,
e.[CDF_FIELDNAME] as outfield
,a.[CDF_ID]
,a.[CDF_CDD_ID]
,a.[CDF_FIELDNAME]
,a.[CDF_SOURCE_FIELDNAME]
,e.CDF_SOURCE_FIELDNAME as sourcefield
,a.[CDF_ADVANCED_USE_ONLY]
,a.[CDF_TYPE]
,a.[CDF_LOOKUP_CDL_NAME]
,a.[CDF_LOOKUP_CDLF_FIELDNAME]
,a.[CDF_LOOKUP_KEY_CDF_FIELDNAME]
,a.[CDF_LOOKUP_KEY_CDLF_FIELDNAME]
,b.[cdl_name]
,b.[cdl_sd_id]
--,convert(varchar(4000),substring( c.SS_SQL_TEXT,1,4000)) as SS_SQL_TEX
,convert(varchar(4000),substring( ltrim(rtrim(c.SS_SQL_TEXT)),1,4000)) as SS_SQL_TEXT1
,convert(varchar(4000),substring( ltrim(rtrim(c.SS_SQL_TEXT)),4001,8000)) as SS_SQL_TEXT2
FROM [CUST_DOMAIN_FIELD] a
,[CUST_DOMAIN_LOOKUP] b
,[SQL_STATEMENT] c
,[CUST_DOMAIN_DATA] d
,[CUST_DOMAIN_FIELD] e
where d.CDD_ADVANCED_USE_ONLY='F'
and b.[cdl_name]=a.[CDF_LOOKUP_CDL_NAME]
and c.SS_SD_ID=b.[cdl_sd_id]
and d.CDD_ID=a.[CDF_CDD_ID]
and a.[CDF_LOOKUP_KEY_CDF_FIELDNAME]=e.cdf_fieldname
and d.CDD_ID=e.[CDF_CDD_ID]
and d.CDD_CD_ID=%s
"""

fieldsql = """
select
CDD_NAME+'__'+lower(cdf_source_fieldname) as group__field
,CDD_NAME as groupname
,cdf_source_fieldname as sourcename
,cdf_fieldname as fieldname
,CDF_DESC as description
,cdf_datatype as type
,cdf_size as size
FROM [CUST_DOMAIN_FIELD], CUST_DOMAIN_DATA
where cdf_cdd_id=CDD_ID
and CDF_TYPE='DATA'
"""
