with patient as (

    select
    *,
    id as patientId

    from {{ source('commons', 'patient') }}
),

patient_info as (

    select
    *,
    ((case when isWhite then 1 else 0 end) +
    (case when isBlack then 1 else 0 end) +
    (case when isAmericanIndianAlaskan then 1 else 0 end) +
    (case when isAsian then 1 else 0 end) +
    (case when isOtherRace then 1 else 0 end) +
    (case when isHispanic then 1 else 0 end)) as indicatedRaceCount

    from {{ source('commons', 'patient_info') }}
),

current_address as (

    select
    id as primaryAddressId,
    lat,
    long,
    street1,
    street2,
    zip,
    state,
    city,
    description as addressDescription

    from {{ source('commons', 'address') }}
    where deletedAt is null
),

phones as (

    select
    memberId as patientId,
    phone,
    phoneType,
    rank() over (partition by memberId order by createdAt desc) as rank

    from {{ ref('abs_member_phones') }} p
),

current_phone as (

    select
    patientId,
    max(case when rank = 1 then phone end) as phone1,
    max(case when rank = 2 then phone end) as phone2,
    max(case when rank = 3 then phone end) as phone3,
    max(case when rank = 4 then phone end) as phone4,
    max(case when rank = 5 then phone end) as phone5,
    max(case when rank = 1 then phoneType end) as phoneType1,
    max(case when rank = 2 then phoneType end) as phoneType2,
    max(case when rank = 3 then phoneType end) as phoneType3,
    max(case when rank = 4 then phoneType end) as phoneType4,
    max(case when rank = 5 then phoneType end) as phoneType5

    from phones
    group by 1
),

emails as (

    select
    memberId as patientId,
    email,
    rank() over ( partition by memberId order by createdAt desc) as rank

    from {{ source('member_index', 'email') }} e
    where deletedAt is null
),

current_email as (

    select
    patientId,
    max(case when rank = 1 then email end) as email1,
    max(case when rank = 2 then email end) as email2

    from emails
    group by 1
),

questions_answers_current as (

    select
    distinct patientId,
    questionSlug,
    answerText,
    rank() over (partition by patientId, questionSlug order by patientAnswerCreatedAt desc) as rank

    from {{ ref('questions_answers_current') }}
    where questionSlug in ('do-you-work-currently', 'how-far-did-you-go-in-school', 'where-have-you-lived-most-in-the-past-2-months')
),

all_text_consents as (

    select
    patientId,
    case
      when documentType = 'verbalTextConsent' then 'verbal'
      when documentType = 'textConsentEmailConsentRecord' then 'email'
      else 'written'
    end as textConsentType,

    -- Number all text consents, prioritizing  written, then email, then verbal
    row_number() over (partition by patientId order by
      if(documentType = 'verbalTextConsent', 3, if(documentType = 'textConsentEmailConsentRecord', 2, 1))) as rank,
    createdAt as textConsentedAt

    from {{ source('commons', 'patient_document') }}

    where documentType in ('textConsent', 'textConsentV2', 'textConsentEmailConsentRecord', 'verbalTextConsent')
    and deletedAt is null
),

text_consent as (

    select
    patientId,
    textConsentType,
    textConsentedAt

    from all_text_consents
    --we only need one form of consent
    where rank = 1

),

all_email_consents as (

    select
    patientId,
    case
      when documentType = 'verbalEmailConsent' then 'verbal'
      else 'written'
    end as emailConsentType,

    -- Number all email consents, prioritizing written, then verbal
    row_number() over (partition by patientId order by if(documentType = 'verbalEmailConsent', 2, 1)) as rank,
    createdAt as emailConsentedAt

    from {{ source('commons', 'patient_document') }}

    where documentType in ('writtenEmailConsent', 'verbalEmailConsent')
    and deletedAt is null
),

email_consent as (

    select
    patientId,
    emailConsentType,
    emailConsentedAt

    from all_email_consents
    --we only need one form of consent
    where rank = 1

),

patient_info_with_race_ethnicity as (

    select
    patient_info.*,
    case
      when indicatedRaceCount > 1 and isBlack then 'Multiracial Black'
      when indicatedRaceCount > 1 then 'Multiracial Non-Black'
      when isHispanic and indicatedRaceCount <=1 then 'Hispanic'
      when isWhite and not isHispanic then 'Non-Hispanic White'
      when isBlack and not isHispanic then 'Non-Hispanic Black'
      when isAmericanIndianAlaskan and not isHispanic then 'American Indian or Alaska Native (Non-Hispanic)'
      when isAsian and not isHispanic then 'Asian (Non-Hispanic)'
      when isHawaiianPacific and not isHispanic then 'Native Hawaiian or Pacific Islander (Non-Hispanic)'
      when isOtherRace and not isHispanic then 'Non-Hispanic Other Race'
    end as raceEthnicity

    from patient_info

),

{# Due to autosaving issue, members may have multiple undeleted answers to radio questions #}

employment as (

    select
    patientId,
    array_agg(answerText order by length(answerText)) as answerText

    from questions_answers_current
    where questionSlug = 'do-you-work-currently' and rank = 1
    group by patientId

),

education as (

    select
    patientId,
    array_agg(answerText order by length(answerText)) as answerText

    from questions_answers_current
    where questionSlug = 'how-far-did-you-go-in-school' and rank = 1
    group by patientId

),

housing as (

    select
    patientId,
    array_agg(answerText order by length(answerText)) as answerText

    from questions_answers_current

    where questionSlug = 'where-have-you-lived-most-in-the-past-2-months' and rank = 1
    group by patientId

),

member_info as (

    select
    generate_uuid() as id,
    p.patientId,
    date_diff(current_date(), p.dateOfBirth, day)/365.25 as age,
    p.dateOfBirth,
    cast(adr.zip as string) as zipcode,
    pho.phone1,
    pho.phone2,
    pho.phone3,
    pho.phone4,
    pho.phone5,
    pho.phoneType1,
    pho.phoneType2,
    pho.phoneType3,
    pho.phoneType4,
    pho.phoneType5,
    pire.needToKnow,
    pire.gender,
    pire.transgender,
    pire.maritalStatus,
    pire.language,
    pire.raceEthnicity,
    pire.isBlack,
    pire.isWhite,
    pire.isHispanic,
    array_to_string(employment.answerText, '; ') as employment,
    array_to_string(education.answerText, '; ') as education,
    array_to_string(housing.answerText, '; ') as housing,
    canReceiveCalls,
    eml.email1 is not null as hasEmail,
    eml.email1,
    eml.email2,
    p.memberId,
    p.medicaidId,
    p.nmi,
    adr.lat,
    adr.long,
    adr.street1,
    adr.street2,
    adr.zip,
    adr.state,
    adr.city,
    adr.addressDescription,
    case
      when text_consent.textConsentType is not null then true
      else null
    end as hasTextConsent,
    text_consent.textConsentType,
    text_consent.textConsentedAt,
    case
      when email_consent.emailConsentType is not null then true
      else null
    end as hasEmailConsent,
    email_consent.emailConsentType,
    email_consent.emailConsentedAt,
    acuityRank

    from patient as p

    left join patient_info_with_race_ethnicity as pire
    using(patientId)

    left join employment
    using(patientId)

    left join education
    using(patientId)

    left join housing
    using(patientId)

    left join current_address as adr
    using(primaryAddressId)

    left join current_phone as pho
    using(patientId)

    left join current_email as eml
    using(patientId)

    left join text_consent
    using(patientId)

    left join email_consent
    using(patientId)

)

select *
from member_info
