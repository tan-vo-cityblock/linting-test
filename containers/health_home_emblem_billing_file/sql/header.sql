select
  'H' as recordType,
  'CITYBLOCK' as headerSenderId,
  'EMBFACETS' as headerReceiverId,
  format_datetime('%C%y%m%d' ,current_datetime()) as headerFileDate,
  format_datetime('%H%M%S' ,current_datetime()) as headerFileCreationTime,
  concat(format_datetime('%y%m%j' ,current_datetime()),format_datetime('%H' ,current_datetime())) as headerFileSequence,
  '546 Eastern Parkway' as headerAddress,
  'Brooklyn' as headerCity,
  'NY' as headerState,
  '112251604' as headerZip,
  '830902280' as headerTaxId,
  '1912491770' as headerNpi,
  '005541231' as headerMmisId,
  'Alicia Ortiz' as headerContactName,
  '5162973164' as headerContactPhone,
  (with billing_file as (
      select
      Member_ID as medicaidId,

      case
        when char_length(cast(Service_Date as string)) = 8 then cast(Service_Date as string)
        when char_length(cast(Service_Date as string)) = 7 then cast(concat('0', Service_Date) as string)
      end as serviceDate,

      Last_Transaction_Date_Time as lastTransaction

    from `cityblock-data.src_health_home.health_home_billing`

  ),

  final as (
    select
      concat(medicaidId,serviceDate,substr(lastTransaction,0,4)) as claimId

      from billing_file
  )

  select count(distinct(claimId))
  from final) as headerTotalClaims