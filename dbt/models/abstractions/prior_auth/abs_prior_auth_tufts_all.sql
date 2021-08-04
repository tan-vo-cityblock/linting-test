
with 

{% if is_incremental() %}

  latest_received_dates as (

    select 
      fileName,
      max(receivedDate) as maxReceivedDate

    from {{ this }}

    group by fileName

  ),

{% endif %}

all_authorizations as (

  {% set file_list = ['Admission', 'Referral'] %}

  {% for file in file_list %}

    select
      '{{ file }}' as fileName, 
      parse_date('%Y%m%d', _table_suffix) as receivedDate,
      data.TRANS_NUMBER as authorizationNumber,
      data.AUTH_STATUS as authorizationStatus,
      parse_date('%Y%m%d', data.PA_CERTIFICATION_DATE) as authorizationCertificationDate,
      parse_date('%Y%m%d', data.Auth_START_DATE) as authorizationStartDate,
      parse_date('%Y%m%d', data.Auth_END_DATE) as authorizationEndDate,
      patient.patientId,
      patient.externalId,
      data.Mbr_First_Name || ' ' || data.Mbr_Last_Name as memberName,
      trim(upper(data.Mbr_Address_1) || ' ' || ifnull(upper(data.Mbr_Address_2), '')) as memberAddress,
      upper(data.Mbr_City) as memberCity,
      upper(data.Mbr_State) as memberState,
      data.Mbr_Zip as memberZip,
      data.REQ_PROVIDER_NPI as requestingProviderNpi,
      trim(ifnull(data.REQ_PROVIDER_FIRST_NAME, '') || ' ' || data.REQ_PROVIDER_LAST_NAME) as requestingProviderName,
      data.REQ_PROVIDER_ADDRESS_1 as requestingProviderAddress1,
      data.REQ_PROVIDER_ADDRESS_2 as requestingProviderAddress2,
      data.REQ_PROVIDER_City as requestingProviderCity,
      data.REQ_PROVIDER_State as requestingProviderState,
      data.REQ_PROVIDER_ZIP as requestingProviderZip,
      data.REQ_PROVIDER_PHONE as requestingProviderPhone,
      data.SERV_PROVIDER_NPI as servicingProviderNpi,
      trim(ifnull(data.SERV_PROVIDER_FIRST_NAME, '') || ' ' || data.SERV_PROVIDER_LAST_NAME) as servicingProviderName,
      data.SERV_PROVIDER_ADDRESS_1 as servicingProviderAddress1,
      data.SERV_PROVIDER_ADDRESS_2 as servicingProviderAddress2,
      data.SERV_PROVIDER_City as servicingProviderCity,
      data.SERV_PROVIDER_State as servicingProviderState,
      data.SERV_PROVIDER_ZIP as servicingProviderZip,
      data.SERV_PROVIDER_PHONE as servicingProviderPhone,
      data.DIAG_CODE as diagnosisCodes,
      data.Proc_Code as procedureCode,
      ifnull(data.Modifier_Code, "") as modifierCode,
      safe_cast(data.TOTAL_APPROVE_PROC_COUNT as int64) as approvedProcedureCount,
      data.Date_of_Admission as admissionDate,
      data.Facility_NPI_TIN as facilityNpiTin,
      safe_cast(data.approved_LOS as int64) as approvedLengthOfStay,
      data.Auth_BedType as authorizedBedType,
      data.Auth_BedType_Desc as authorizedBedTypeDescription,
      data.Auth_BedType_status as authorizedBedTypeStatus,
      parse_date('%Y%m%d', data.Auth_BedType_StartDate) as authorizedBedTypeStartDate,
      parse_date('%Y%m%d', data.Auth_BedType_EndDate) as authorizedBedTypeEndDate
      
    from {{ source('tufts_silver', 'PriorAuthorization' ~ file ~ '_*') }} p

    {% if is_incremental() %}

      where 
        parse_date('%Y%m%d', _table_suffix) > (

          select maxReceivedDate from latest_received_dates where fileName = '{{ file }}'

        )

    {% endif %}

    {% if not loop.last %} union distinct {% endif %}

  {% endfor %}

)

select * from all_authorizations
