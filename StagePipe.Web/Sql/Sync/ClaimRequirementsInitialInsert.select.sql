SELECT
  cr.courtCaseId AS court_case_id_api,
  cr.participantId AS participant_id_api,
  cr.description
FROM
  ClaimRequirements cr
  JOIN CourtCases cc ON cc.courtCaseId = cr.courtCaseId
WHERE
  cc.status not in('draft', 'signing')
  AND cr.type = 'demand'