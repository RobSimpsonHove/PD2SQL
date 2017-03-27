tablesql = """
select * from (
select
cdd.cdd_id,
cdd.cdd_name,
cdd.cdd_cd_id,
cdd.cdd_parent_cdd_id,
cdd.[CDD_ONE_TO_MANY],
ss.ss_database_plugin,
ss.SS_SQL_TEXT,
--cdf.cdf_fieldname as CDD_KEY,
--convert(nvarchar(4000),substring( ltrim(rtrim(ss.SS_SQL_TEXT)),1,4000)) as SS_SQL_TEXT1,
--convert(nvarchar(4000),substring( ltrim(rtrim(ss.SS_SQL_TEXT)),4001,8000)) as SS_SQL_TEXT2,
cdd.CDD_ID_SOURCE_FIELDNAME
FROM [SQL_DEFINITION] as sd,
[SQL_STATEMENT] as ss,
[CUST_DOMAIN_DATA] as cdd
--cust_domain_field as cdf

where sd.SD_ID = ss.SS_SD_ID
and  ss.SS_SD_ID= cdd.CDD_SD_ID
and cdd.CDD_IS_SYSTEM_GROUP='F'
and cdd.CDD_ADVANCED_USE_ONLY='F'
and cdd.CDD_CD_ID=%s
--and cdf.cdf_cdd_id=cdd.cdd_id
--and cdf.cdf_source_fieldname=cdd.CDD_ID_SOURCE_FIELDNAME
) a
left join
(select CDP_CDD_ID
      ,CDP_PARAMNAME
	  ,CDP_BOUND_TO_FIELDNAME from CUST_DOMAIN_PARAM) p
on p.cdp_cdd_id=a.CDD_ID
and (p.CDP_BOUND_TO_FIELDNAME='mh_customer_id' or p.CDP_BOUND_TO_FIELDNAME in
	(
	select  cdf_fieldname from CUST_DOMAIN_FIELD
	 join (
	--main group and keyfield
	select cdd.cdd_id, cdd.CDD_ID_SOURCE_FIELDNAME from CUST_DOMAIN_DATA cdd where cdd_id in (
	select CDD_ID from  [CUST_DOMAIN_DATA] where CDD_PARENT_CDD_ID IS NULL AND CDD_CD_ID=a.cdd_cd_id)
	) aa
	on aa.cdd_id=CDF_CDD_ID and aa.CDD_ID_SOURCE_FIELDNAME=cdf_source_fieldname
	))
	"""

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
where b.[cdl_name]=a.[CDF_LOOKUP_CDL_NAME]
and c.SS_SD_ID=b.[cdl_sd_id]
and d.CDD_ID=a.[CDF_CDD_ID]
and a.[CDF_LOOKUP_KEY_CDF_FIELDNAME]=e.cdf_fieldname
and d.CDD_ID=e.[CDF_CDD_ID]
and d.CDD_ADVANCED_USE_ONLY='F'
--and a.CDF_ADVANCED_USE_ONLY='F'
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
FROM [CUST_DOMAIN_FIELD] cdf, CUST_DOMAIN_DATA cdd
where cdf.cdf_cdd_id=cdd.CDD_ID
and CDF_TYPE='DATA'
"""


pdfieldsql="""
 select * from
(
select
cdd_name
,[cdf_id]
,cdd.cdd_cd_id
,[cdf_cdd_id]
,[cdf_fieldname]
,[cdf_desc]
,[cdf_advanced_use_only]
,[cdf_order_index]
,[cdf_datatype]
,[cdf_type]      ,[cdf_source_fieldname]     ,[cdf_required]      ,[cdf_size]
,[cdf_lookup_cdl_name]     ,[cdf_lookup_key_cdf_fieldname]    ,[cdf_lookup_key_cdlf_fieldname]      ,[cdf_lookup_cdlf_fieldname]
,[cdf_lookup_filt_cdf_fieldname]      ,[cdf_lookup_filt_cdlf_fieldname]
from cust_domain_field cdf
	,cust_domain_data cdd
where cdf.cdf_cdd_id=cdd.cdd_id
and cdd.cdd_cd_id=%s and cdd.cdd_name=N'%s'
) fields

left join  (select cdl_name, cdl_cd_id, [cdl_sd_id] from cust_domain_lookup ) cdl on fields.cdf_lookup_cdl_name=cdl.cdl_name and fields.cdd_cd_id=cdl.cdl_cd_id
left join  (select [ss_sd_id], [ss_sql_text] as sqltext from sql_statement ) ss on cdl.cdl_sd_id=ss.ss_sd_id
 """


pdfieldsql2=""" 
SELECT cdf_order_index
, cdf_source_fieldname
, cdf_fieldname 
,  CDF_SOURCE_FIELDNAME  || ' as ' || CDF_FIELDNAME  "rename"
, cdf_datatype
, cdf_size
FROM CUST_DOMAIN_FIELD cdf,  CUST_DOMAIN_DATA cdd                 
where cdd_cd_id=%s and cdd.cdd_id=cdf.cdf_cdd_id  
and cdd.cdd_name=N'%s'  
order by cdf_order_index
 """