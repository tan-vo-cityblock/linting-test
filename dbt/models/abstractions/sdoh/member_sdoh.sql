SELECT
	mawg.patientId, mawg.addressId,
	acs.* except(patientId, addressId), 
	air.* except(patientId, addressId, stateCode, countyCode, siteNum, parameterCode)
FROM {{ ref('member_address_with_geopoint') }} mawg
LEFT JOIN {{ ref('member_census_acs') }}  acs
ON mawg.patientId = acs.patientId and mawg.addressId = acs.addressId
LEFT JOIN {{ ref('member_air_quality') }} air
ON mawg.patientId = air.patientId and mawg.addressId = air.addressId
