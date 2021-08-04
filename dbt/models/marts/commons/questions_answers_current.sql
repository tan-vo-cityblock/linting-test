select * from 
  {{ref ('questions_answers_all') }}
  where patientAnswerDeletedAt is null
