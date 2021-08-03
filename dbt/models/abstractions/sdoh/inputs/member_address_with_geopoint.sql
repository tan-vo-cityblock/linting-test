WITH

patient as (
	SELECT id as patientId
	FROM {{ source('commons', 'patient') }}
),

patient_address as (
	SELECT patientId, addressId
	FROM {{ source('commons', 'patient_address') }}
	WHERE deletedAt IS NULL
),

address as (
	SELECT id as addressId, lat, long
	FROM {{ source('commons', 'address') }}
	WHERE deletedAt IS NULL
),

final as (
	SELECT p.patientId, a.addressId, a.lat, a.long, ST_GEOGPOINT(a.long, a.lat) as geopoint
	FROM patient p
	LEFT JOIN patient_address pa USING(patientId)
	LEFT JOIN address a USING(addressId)
)

SELECT * FROM final
