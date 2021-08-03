{{
 config(
   materialized='table'
 )
}}

with users as (

  select userId,
         userRole,
         userPhone
  from {{ ref('user') }}
  where userPhone is not null
),

members as (

  select patientId           as memberId,
         firstName           as memberFirstName,
         lastName            as memberLastName,
         lower(firstName)    as memberFirstNameLower,
         lower(lastName)     as memberLastNameLower
  from {{ ref('member') }}
),

member_phone_numbers as (

  select patientId as memberId,
         phoneNumber
  from {{ ref('member_phone_numbers') }}
),

care_teams as (

  select userId,
         patientId                                                  as memberId,
         careTeamMemberAssignedAt,
         coalesce(careTeamMemberAssignedUntil, current_timestamp()) as careTeamMemberAssignedUntil
  from {{ ref('member_care_team_all') }}

),

sms_messages as (

  select m.id                                                                                           as messageId,
         -- If a message was sent from an unkown number that has since been added to a member's commons profile, fill in the memberId here
         coalesce(m.patientID, mpn.memberId)                                                            as memberId,
         m.userId,
         m.contactNumber,
         lower(m.body)                                                                                  as bodyLower,
         m.direction,
         m.providerCreatedAt                                                                            as sentAt,
         lead(m.direction) over (partition by m.patientId, m.userId order by providerCreatedAt)         as nextDirection,
         lead(m.providerCreatedAt) over (partition by m.patientId, m.userId order by providerCreatedAt) as nextSentAt
  from {{ source('commons', 'sms_message') }} m
  left join users u on m.contactNumber = u.userPhone
  left join member_phone_numbers mpn on m.contactNumber = mpn.phoneNumber
  where u.userId is null -- exclude messages from other care team members
  
),

care_team_member_messages as (

  select *,
         nextDirection != direction                 as directionChange,
         timestamp_diff(nextSentAt, sentAt, minute) as timeToNextMessageMinutes
  from sms_messages
  where memberId is not null
),

care_team_member_one_sided_conversations as (

  select *
  from care_team_member_messages
  where directionChange = False
        and timeToNextMessageMinutes < 1440 -- Only consider same-direction messages less than 24 hours apart.
),

unknown_number_messages as (

  select *
  from sms_messages
  where memberId is null
),

-- join on user's panel to limit the number of names to search for
unknown_number_panel_name_match_messages as (

  select ct.memberId,
         m.memberFirstName,
         m.memberLastName,
         unm.contactNumber                                                                                        as alternativeContactNumber,
         unm.userId,
         -- Note: some members are known to us only by their first initial
         (char_length(memberFirstNameLower) > 1
          and regexp_contains(bodyLower, memberFirstNameLower))                                                   as firstNameFound,
         (char_length(memberLastNameLower) > 1
          and regexp_contains(bodyLower, memberLastNameLower))                                                    as lastNameFound,
         (char_length(memberLastNameLower) > 1
          and regexp_contains(bodyLower, memberFirstNameLower || '[ ]' || memberLastNameLower))                   as firstLastNameFound,
         regexp_contains(bodyLower, "(its|it[']s|this|this[ ]is)[ ]" || memberFirstNameLower)                     as introductionFound,
         regexp_contains(bodyLower, 'son|daughter|mother|father|sister|brother|uncle|aunt|nephew|cousin|mom|dad') as familyTitleFound,
  from unknown_number_messages unm
  left join care_teams ct
    on unm.userId = ct.userId
      -- only consider members in user's panel at the time message was sent
       and unm.sentAt between ct.careTeamMemberAssignedAt and ct.careTeamMemberAssignedUntil
       and unm.direction = 'toUser'
       and ct.memberId is not null
  left join members m
    on ct.memberId = m.memberId

),

potential_conversations as (

  select ctm.userId,
         ctm.memberId,
         ctm.contactNumber,
         unm.contactNumber as alternativeContactNumber,
         ctm.direction,
         unm.direction     as responseDirection,
         ctm.nextDirection,
         ctm.sentAt,
         unm.sentAt        as responseSentAt,
         ctm.nextSentAt,
         ctm.timeToNextMessageMinutes
  from care_team_member_one_sided_conversations ctm
  left join unknown_number_messages unm
   on ctm.userId = unm.userId
      and ctm.direction != unm.direction
      and unm.sentAt between ctm.sentAt and ctm.nextSentAt
  where unm.messageId is not null
),

member_alternative_number_name_match_counts as (

  select memberId,
         memberFirstName,
         memberLastName,
         alternativeContactNumber,
         countif(introductionFound)                      as introductionCount,
         countif(firstNameFound)                         as firstNameMatchCount,
         countif(lastNameFound)                          as lastNameMatchCount,
         countif(firstLastNameFound)                     as fullNameMatchCount,
         countif(introductionFound and familyTitleFound) as familyIntroductionCount
  from unknown_number_panel_name_match_messages
  group by 1, 2, 3, 4

),

member_alternative_number_conversation_counts as (

  select pc.memberId,
         memberFirstName,
         memberLastName,
         alternativeContactNumber,
         count(distinct sentAt)        as potentialConversationCount,
         avg(timeToNextMessageMinutes) as avgTimeToNextMessageMinutes
  from potential_conversations pc
  left join members m on pc.memberId = m.memberId
  group by 1, 2, 3, 4
)

select coalesce(mannmc.memberId, mancc.memberId)                                 as memberId,
       coalesce(mannmc.memberFirstName, mancc.memberFirstName)                   as memberFirstName,
       coalesce(mannmc.memberLastName, mancc.memberLastName)                     as memberLastName,
       coalesce(mannmc.alternativeContactNumber, mancc.alternativeContactNumber) as alternativeContactNumber,
       coalesce(introductionCount, 0)                                            as introductionCount,
       coalesce(firstNameMatchCount, 0)                                          as firstNameMatchCount,
       coalesce(lastNameMatchCount, 0)                                           as lastNameMatchCount,
       coalesce(fullNameMatchCount, 0)                                           as fullNameMatchCount,
       coalesce(familyIntroductionCount, 0)                                      as familyIntroductionCount,
       coalesce(potentialConversationCount, 0)                                   as potentialConversationCount,
       avgTimeToNextMessageMinutes
from member_alternative_number_name_match_counts mannmc
full outer join member_alternative_number_conversation_counts mancc
  on mannmc.memberId = mancc.memberId
     and mannmc.alternativeContactNumber = mancc.alternativeContactNumber
  where introductionCount + firstNameMatchCount + lastNameMatchCount + familyIntroductionCount + potentialConversationCount > 0
