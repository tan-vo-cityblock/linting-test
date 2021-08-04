
-- calculating latest member id in a separate query to save on resourcing... over() clause was causing query to crash

--  result from the prep work sql, sorted version
with result as (
    select * from {{ ref('master_member_prep_data') }}
),

-- latest memberId per patientId
latest as (
    select 
        patientId as id,  
        memberId as latestMemberId, 
        row_number() over(partition by patientId order by eligYear desc, eligMonth desc) as rownum 
    from result
    where memberId is not null
),

memberids as (
    select 
        id, 
        latestMemberId
    from latest
    where rownum = 1     
)

select * from memberids