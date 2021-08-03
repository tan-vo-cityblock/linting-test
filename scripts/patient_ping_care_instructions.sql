SELECT
  CONCAT('Cityblock', ' - ', TRIM(u.firstName), ' ', TRIM(u.lastName)) AS COHORT_NAME,
  u.firstName AS ADM_CC_FIRST_NAME,
  u.lastName AS ADM_CC_LAST_NAME,
  u.phone AS ADM_CC_PHONE,
  CASE WHEN u.homeMarketId = '9065e1fc-2659-4828-9519-3ad49abf5126' THEN 'Cityblock is a new company partnering with ConnectiCare to provide members with primary and preventive care, behavioral health support, and social supports. Each member will have the support of a full care team and a designated Community Health Partner (CHP), who will be the member\'s main ally on their health journey.\nIf you have questions about this member\'s medications, chronic medical conditions, safety at home, or social risks, the member\'s CHP will be able to help.'
  WHEN u.homeMarketId IN ('76150da6-b62a-4099-9bd6-42e7be3ffc62') THEN
    'Cityblock is a care management organization collaborating with Tufts Unify to provide members with primary and preventive care, behavioral health support, and social supports. If you have questions about this member\'s medications, chronic medical conditions, safety at home, or social risks, please contact Cityblock at (508) 217-9030.'
  WHEN u.homeMarketId = '31c505ee-1e1b-4f5c-8e3e-4a2bc9937e04' THEN 'Cityblock is a new company partnering with Cardinal Innovations, Healthy Blue, and Blue Cross Blue Shield of North Carolina to provide members with primary and preventive care, behavioral health support, and social supports. Each member under care management will have the support of a full care team and a designated Community Health Partner (CHP), who will be the member’s main ally on their health journey.\nIf you have questions about this member\'s medications, chronic medical conditions, safety at home, or social risks, the member\'s CHP will be able to help. Please contact us at (336) 360-2407.'
  ELSE
    'Cityblock is a new company providing members with primary and preventive care, behavioral health support, and social supports. Each member will have the support of a full care team and a designated Community Health Partner (CHP), who will be the member\'s main ally on their health journey.\nIf you have questions about this member\'s medications, chronic medical conditions, safety at home, or social risks, the member\'s CHP will be able to help.'
  END
    AS DC_INSTRUCTIONS,
  u.firstName AS DC_CC_FIRST_NAME,
  u.lastName AS DC_CC_LAST_NAME,
  u.phone AS DC_CC_PHONE
FROM
  `commons_mirror.user` u
WHERE
  u.firstName IS NOT NULL
  AND u.lastName IS NOT NULL
  AND u.phone IS NOT NULL
  AND u.terminatedAt IS NULL
UNION ALL
SELECT
  'Cityblock Health' AS COHORT_NAME,
  'Cityblock' AS ADM_CC_FIRST_NAME,
  'Health' AS ADM_CC_LAST_NAME,
  '+12035184566' AS ADM_CC_PHONE,
  'Cityblock is a care management organization providing members with primary and preventive care, behavioral health support, and social supports. Each member will have the support of a full care team and a designated Community Health Partner (CHP), who will be the member\'s main ally on their health journey.\nIf you have questions about this member\'s medications, chronic medical conditions, safety at home, or social risks, the member\'s CHP will be able to help.' AS DC_INSTRUCTIONS,
  'Cityblock' AS DC_CC_FIRST_NAME,
  'Health' AS DC_CC_LAST_NAME,
  '+12035184566' AS DC_CC_PHONE
UNION ALL
SELECT
  'Cityblock Health MA' AS COHORT_NAME,
  'Cityblock' AS ADM_CC_FIRST_NAME,
  'Health' AS ADM_CC_LAST_NAME,
  '+15082179030' AS ADM_CC_PHONE,
  'Cityblock is a care management organization collaborating with Tufts Unify to provide members with primary and preventive care, behavioral health support, and social supports. If you have questions about this member\'s medications, chronic medical conditions, safety at home, or social risks, please contact Cityblock at (508) 217-9030.' AS DC_INSTRUCTIONS,
  'Cityblock' AS DC_CC_FIRST_NAME,
  'Health' AS DC_CC_LAST_NAME,
  '+15082179030' AS DC_CC_PHONE
UNION ALL
SELECT
  'Cityblock Health NC' AS COHORT_NAME,
  'Cityblock' AS ADM_CC_FIRST_NAME,
  'Health' AS ADM_CC_LAST_NAME,
  '+13363602407' AS ADM_CC_PHONE,
  'Cityblock is a new company partnering with Cardinal Innovations, Healthy Blue, and Blue Cross Blue Shield of North Carolina to provide members with primary and preventive care, behavioral health support, and social supports. Each member under care management will have the support of a full care team and a designated Community Health Partner (CHP), who will be the member’s main ally on their health journey. If you have questions about this member’s medications, chronic medical conditions, safety at home, or social risks, the member’s CHP will be able to help. Please contact us at (336) 360-2407.' AS DC_INSTRUCTIONS,
  'Cityblock' AS DC_CC_FIRST_NAME,
  'Health' AS DC_CC_LAST_NAME,
  '+13363602407' AS DC_CC_PHONE
