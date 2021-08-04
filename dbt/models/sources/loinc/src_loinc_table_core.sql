select
  LOINC_NUM as loincNumber,
  COMPONENT as component,
  PROPERTY as property,
  TIME_ASPCT as timeAspect,
  SYSTEM as system,
  SCALE_TYP as scaleType,
  nullif(METHOD_TYP, '') as methodType,
  CLASS as class,
  CLASSTYPE as classType,
  LONG_COMMON_NAME as longCommonName,
  SHORTNAME as shortName,
  nullif(EXTERNAL_COPYRIGHT_NOTICE, '') as externalCopyrightNotice,
  STATUS as status,
  VersionFirstReleased as versionFirstReleased,
  VersionLastChanged as versionLastChanged,
  split(LOINC_NUM, '-')[offset(0)] as loincNumberAltId1,
  replace(LOINC_NUM, '-', '') as loincNumberAltId2
  
from {{ source('loinc', 'loinc_table_core') }}
