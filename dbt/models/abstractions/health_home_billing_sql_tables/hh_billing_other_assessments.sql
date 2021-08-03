--additional assessments for the HH assessment report. This does not need a reporting date filter
--because initial assessments can be re-given to member
--If there was an initial assessment completion date in the quarter,
--include the latest response with the data of the initial assessment completion date (even if it's a different date in the quarter)
--add max and min to make sure we get the correct answer
with questions_filtered as (
select
patientid,
date(max(submissionCompletedAt)) AS Completed_Date,
max(case when questionslug = 'what-is-your-living-situation-today' and answerText = 'I have a steady place to live' then 1
    when questionslug = 'what-is-your-living-situation-today' and answerText = 'I have a place to live today, but I am worried about losing it in the future' then 2
    when questionslug = 'what-is-your-living-situation-today'
        and answerText = 'I do not have a steady place to live (I am temporarily staying with others, in a hotel, in a shelter, living outside on the street, on a beach, in a car, abandoned building, bus or train station, or in a park'
        then 3
    else 1 end) as living,
min( case when questionslug = 'do-you-have-problems-with-any-of-the-following-housing-conditions'
          and answerText = 'Pests such as bugs, ants, or mice' then 1
    else 5 end) as LIVING_PESTS,
min( case when questionslug = 'do-you-have-problems-with-any-of-the-following-housing-conditions'
          and answerText = 'MOLD' then 1
    else 5 end) as living_mold,
min( case when questionslug = 'do-you-have-problems-with-any-of-the-following-housing-conditions'
          and answerText = 'Lead paint or pipes' then 1
    else 5 end) as LIVING_LEAD,
min( case when questionslug = 'do-you-have-problems-with-any-of-the-following-housing-conditions'
          and answerText = 'Lack of heat' then 1
    else 5 end) as LIVING_heat,
min( case when questionslug = 'do-you-have-problems-with-any-of-the-following-housing-conditions'
     and answerText = 'Oven or stove not working' then 1
    else 5 end) as LIVING_OVEN,
min( case when questionslug = 'do-you-have-problems-with-any-of-the-following-housing-conditions'
     and answerText = 'Smoke detectors missing or not working' then 1
    else 5 end) as LIVING_SMOKE,
min( case when questionslug = 'do-you-have-problems-with-any-of-the-following-housing-conditions'
     and answerText = 'Water Leaks' then 1
    else 5 end) as LIVING_LEAKS,
min( case when questionslug = 'do-you-have-problems-with-any-of-the-following-housing-conditions'
     and answerText = 'None' then 1
    else 5 end) as LIVING_NONE,
min( case when questionslug = 'worried-food-would-run-out-before-more-money'
        and answerText = 'Often true' then 1
     when questionslug = 'worried-food-would-run-out-before-more-money'
        and  (answerText = 'Sometimes true' or answerText = 'Yes') then 2
     when questionslug = 'worried-food-would-run-out-before-more-money'
        and (answerText = 'Never true' or answerText = 'No')  then 3
    else 3 end) as FOOD_WORRIED,
min( case when questionslug = 'did-food-run-out-before-more-money'
        and answerText = 'Often true' then 1
     when questionslug = 'did-food-run-out-before-more-money'
        and (answerText = 'Sometimes true' or answerText = 'Yes') then 2
     when questionslug = 'did-food-run-out-before-more-money'
        and (answerText = 'Never true' or answerText = 'No') then 3
    else 3 end) as FOOD_MONEY,
min( case when ((questionslug = 'lack-of-transportation-problem-for-daily-living'
     and answerText like 'Yes%')
     OR (questionslug = 'lack-of-transportation-problem-for-health-care'
     and answerText like 'Yes%')) then 1
    else 5 end) as TRANSPORTATION,
min( case when questionslug = 'been-unable-to-get-utilities'
          and answerText = 'Yes' then 1
    else 5 end) as UTILITIES,
max( case when questionslug = 'how-often-do-friends-or-family-physically-hurt-you'
          and answerText = 'Never' then 1
     when questionslug = 'how-often-do-friends-or-family-physically-hurt-you'
          and answerText = 'Rarely' then 2
     when questionslug = 'how-often-do-friends-or-family-physically-hurt-you'
          and answerText = 'Sometimes' then 3
     when questionslug = 'how-often-do-friends-or-family-physically-hurt-you'
          and answerText = 'Fairly often' then 4
     when questionslug = 'how-often-do-friends-or-family-physically-hurt-you'
          and answerText = 'Frequently' then 5
    else 1 end) as SAFETY_PHYSICAL,
max( case when questionslug = 'how-often-do-friends-or-family-insult-or-talk-down-to-you'
          and answerText = 'Never' then 1
     when questionslug = 'how-often-do-friends-or-family-insult-or-talk-down-to-you'
          and answerText = 'Rarely' then 2
     when questionslug = 'how-often-do-friends-or-family-insult-or-talk-down-to-you'
          and answerText = 'Sometimes' then 3
     when questionslug = 'how-often-do-friends-or-family-insult-or-talk-down-to-you'
          and answerText = 'Fairly often' then 4
     when questionslug = 'how-often-do-friends-or-family-insult-or-talk-down-to-you'
          and answerText = 'Frequently' then 5
    else 1 end) as SAFETY_INSULT,
max( case when questionslug = 'how-often-do-friends-or-family-threaten-you-with-harm'
          and answerText = 'Never' then 1
     when questionslug = 'how-often-do-friends-or-family-threaten-you-with-harm'
          and answerText = 'Rarely' then 2
     when questionslug = 'how-often-do-friends-or-family-threaten-you-with-harm'
          and answerText = 'Sometimes' then 3
     when questionslug = 'how-often-do-friends-or-family-threaten-you-with-harm'
          and answerText = 'Fairly often' then 4
     when questionslug = 'how-often-do-friends-or-family-threaten-you-with-harm'
          and answerText = 'Frequently' then 5
    else 1 end) as SAFETY_THREATEN,
max( case when questionslug = 'how-often-do-friends-or-family-scream-or-curse-at-you'
          and answerText = 'Never' then 1
     when questionslug = 'how-often-do-friends-or-family-scream-or-curse-at-you'
          and answerText = 'Rarely' then 2
     when questionslug = 'how-often-do-friends-or-family-scream-or-curse-at-you'
          and answerText = 'Sometimes' then 3
     when questionslug = 'how-often-do-friends-or-family-scream-or-curse-at-you'
          and answerText = 'Fairly often' then 4
     when questionslug = 'how-often-do-friends-or-family-scream-or-curse-at-you'
          and answerText = 'Frequently' then 5
    else 1 end) as SAFETY_SCREAM
from {{ ref('questions_answers_all') }} AS questions_answers
where questions_answers.isLatestPatientAnswer
and submissionDeletedAt is null
group by 1
)

select * from questions_filtered