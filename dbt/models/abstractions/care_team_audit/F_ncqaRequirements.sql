with patientAnswer as (

  select
    patientId,
    answerValue,
    answerText,
    case 
      when questionSlug = 'do-you-have-any-difficulties-with-your-hearing' 
        then "F10a. Do you have any difficulties with your hearing?"
      when questionSlug = 'do-you-have-any-difficulties-with-your-vision' 
        then "F10b. Do you have any difficulties with your vision?"
      when questionSlug = 'are-there-caregivers-involved-in-your-care-or-in-place-to-help-you' 
        then "F11a. Are there caregivers (friends, family, others) involved in your care or in place to help you?"
      when questionSlug = 'do-you-have-enough-help-to-meet-your-needs'
        then"F11b. Do you have enough help to meet your needs?"
      when questionSlug = 'benefits-insurance-plan-not-covered-that-would-improve-care'
        then "F12. Are there benefits your insurance plan has not covered that would have improved your wellbeing or care?"
      when questionSlug = 'is-the-member-interested-in-any-wellness-activities-in-the-community'
        then "F13a. Is the member interested in any wellness activities in the community?"
      when questionSlug = 'lack-of-transportation-problem-for-health-care'
        then "F13b. In the last 4 months, have you ever had to go without health care because you didn’t have a way to get there?"
      when questionSlug = 'lack-of-transportation-problem-for-daily-living'
        then "F13c. In the last 4 months, has lack of transportation kept you from non-medical appointments, meetings, work, or from getting things needed for daily living?"
      when questionSlug = 'list-your-current-medications-including-dose-and-frequency'
        then "F2a. List current meds"
      when questionSlug = 'list-here-any-recent-past-medications-that-have-been-tried-and-stopped'
        then "F2b. List past meds"
      when questionSlug = 'describe-the-past-medical-concerns-and-treatment'
        then "F2c. Explore past medical diagnoses"
      when questionSlug = 'do-you-need-help-with-any-of-these-activities'
        then "F3. ADLs"
      when questionSlug like 'in-general-would-you-say-your-health-is%'
        then "F4a. In general, would you say your health is"
      when questionSlug = 'what-medical-issues-have-you-been-facing-or-dealing-with'
        then "F4b. Explore medical diagnoses"
      when questionSlug = 'do-you-have-any-concerns-related-to-mental-health-or-emotional-problems'
        then "F5a. Do you have any concerns related to mental health"
      when questionSlug = 'explore-with-the-member-their-current-mental-health-diagnoses'
        then "F5b. Explore mental health diagnoses"
      when questionSlug = 'other-diagnoses-mental-health'
        then "F5c. Any other MH diagnoses"
      when questionSlug = 'have-you-ever-used-drugs'
        then "F5d. Prior drug use"
      when questionSlug = 'how-often-do-you-have-a-drink-containing-alcohol'
        then "F5e. AUDIT: How often do you have a drink containing alcohol"
      when questionSlug = 'how-often-had-little-interest-or-pleasure-in-doing-things'
        then "F5f. PHQ9: Over the last 2 weeks, how often have you had little interest or pleasure in doing things"
      when questionSlug = 'have-you-used-drugs-other-than-those-required-for-medical-reasons'
        then "F5g. DAST: Have you ever used drugs other than those required for medical reasons"
      when questionSlug = 'over-the-last-2-weeks-how-often-have-you-felt-nervous-anxious-or-on-edge'
        then "F5h. GAD7: Over the last 2 weeks, how often have you felt nervous, anxious, or on edge?"
      when questionSlug = 'concern-about-cognitive-function-interfering-with-members-ability-to-take-care-of-health'
        then "F6a. Are you, Cityblock staff member, concerned about the member's cognitive function interfering with their ability to take care of their health?"
      when questionSlug = 'do-you-have-any-difficulties-with-learning-or-communicating'
        then "F6b. Do you have any difficulties with learning or communicating?"
      when questionSlug = 'how-often-do-you-need-someone-to-help-you-read-medical-materials'
        then "F6c. How often do you need someone to help you read medical materials?"
      when questionSlug = 'do-you-have-a-stable-place-to-stay-over-the-next-three-months'
        then "F7a. Are you worried or concerned that in the next two months you may not have stable housing that you own, rent, or stay in as a part of a household?"
      when questionSlug = 'where-have-you-lived-most-in-the-past-2-months'
        then "F7b. Where have you lived most in the past 2 months?"
      when questionSlug = 'who-do-you-live-with'
        then "F7c. Who do you live with?"
      when questionSlug = 'current-access-to-toilet-cooking-bed-enough-heat-or-ac'
        then "F7d. Do you currently have access to: a working toilet, a place to cook, a bed to sleep on, enough heat in the winter, and enough A/C in the summer?"
      when questionSlug = 'ate-less-because-lacked-money-for-food'
        then "F7e. In the last month, did you ever eat less than you felt you should because there wasn't enough money for food?"
      when questionSlug = 'there-is-a-large-selection-of-fresh-fruits-and-vegetables-in-my-neighborhood'
        then "F7f. There is a large selection of fresh fruits and vegetables in my neighborhood"
      when questionSlug = 'how-often-do-you-see-or-talk-to-people-that-that-you-care-about-and-feel-close-to'
        then "F7g. How often do you see or talk to people that that you care about and feel close to?"
      when questionSlug = 'how-often-stayed-inside-residence-because-health-problem'
        then "F7h. In the past week, how often did you stay inside where you live, a nursing home, or hospital, because of sickness, injury, or other health problem?"
      when questionSlug = 'in-the-past-week-how-often-have-you-taken-part-in-social-religious-or-recreation-activities'
        then "F7i. In the past week, how often have you taken part in social, religious, or recreation activities (meetings, church, movies, sports, parties)?"
      when questionSlug = 'is-there-anyone-you-can-really-count-on-when-you-need-help'
        then "F7j. Is there anyone you can really count on when you need help?"
      when questionSlug = 'are-you-in-a-relationship-currently'
        then "F7k. Are you in a relationship currently?"
      when questionSlug = 'do-you-have-any-children'
        then "F7l. Do you have any children?"
      when questionSlug = 'are-you-acting-as-a-caregiver-for-anyone'
        then "F7m. Are you acting as a caregiver for anyone?"
      when questionSlug = 'do-you-feel-physically-and-emotionally-safe-where-you-currently-live'
        then "F7n. Do you feel physically and emotionally safe where you currently live?"
      when questionSlug = 'are-there-caregivers-involved-in-your-care-or-in-place-to-help-you'
        then "F7o. Are there caregivers (friends, family or others) involved in your care or in place to help you?"
      when questionSlug = 'beliefs-doctor-should-know-or-not-been-respected'
        then "F9a. Are there any beliefs that your doctor should know about or beliefs that have not been respected by your doctor in the past?"
      when questionSlug = 'what-is-your-preferred-spoken-language'
        then "F9b. Preferred spoken language"
      when questionSlug = 'what-is-your-preferred-written-language'
        then "F9c. Preferred written language"
    end as question

 from {{ source('commons','patient_answer') }}

 where deletedAt is null

),

final as (

  select * 
  from patientAnswer
  where question is not null

)

select * from final
