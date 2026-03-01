SELECT
  courtCaseDecisionId AS id_api,
  courtCaseId AS court_case_id_api,
  judgeId AS judge_id_api,
  'interim' AS type,
  type AS type_name,
  type_slug AS subtype_slug,
  isPublic AS is_public,
  acceptedAt AS published_at,
  CASE
    WHEN status = 'published' THEN 'published'
    WHEN status = 'draft' THEN 'draft'
    WHEN status = 'shouldBePublished' THEN 'signing'
  END AS status
FROM
  CourtCaseDecisions
UNION ALL
SELECT
  courtCaseVerdictId AS id_api,
  courtCaseId AS court_case_id_api,
  judgeId AS judge_id_api,
  'final' AS type,
  type AS type_name,
  NULL AS subtype_slug,
  TRUE AS is_public,
  publishedAt AS published_at,
  CASE
    WHEN status = 'published' or status = 'accepted' THEN 'published'
    WHEN status = 'draft' THEN 'draft'
  END AS status
FROM
  CourtCaseVerdicts