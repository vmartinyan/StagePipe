SELECT
  objectId AS judicial_act_id_api,
  CASE
    WHEN objectType = 'courtCaseDecision' THEN 'interim'
    WHEN objectType = 'courtCaseVerdict' THEN 'final'
  END AS type,
  CASE
    WHEN status = 'valid' THEN 'in_force'
    WHEN status = 'invalid' THEN 'not_in_force'
  END AS status,
  startedAt as started_at,
  endedAt as ended_at,
  note
FROM
  CourtCaseActValidities