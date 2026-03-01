SELECT
  cca.courtCaseId AS court_case_id_api,
  cca.courtCaseParticipantId AS participant_id_api,
  cca.answer as description,
  cca.publishedAt as published_at
FROM
  CourtCaseAnswers cca
  JOIN CourtCases cc ON cc.courtCaseId = cca.courtCaseId
WHERE
  cca.status = 'published'
  AND cc.status not in('draft', 'signing')