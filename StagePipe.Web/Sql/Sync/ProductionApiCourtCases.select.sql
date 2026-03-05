SELECT
  cc.courtCaseId AS id_api,
  UUID as uuid,
  code,
  CASE
    WHEN courtType = 'firstInstance' THEN 1
    WHEN courtType = 'appellate' THEN 2
    WHEN courtType = 'cassation' THEN 3
    ELSE NULL
  END AS instance_id,
  claimId AS claim_id_api,
  parentId AS parent_id_api,
  counterClaimId AS counter_claim_id_api,
  isCollegial AS is_collegial,
  appealId AS appeal_id_api,
  appealType AS appeal_type_api,
  FORMAT AS mode,
  CASE
    WHEN source = 'by_staff' THEN 'paper_based'
    ELSE 'electronic'
  END AS type,
  isPublic as is_public,
  ccs.name as statistical_classifier
FROM
  CourtCases cc
  LEFT JOIN CourtCaseStatisticalLines ccs on ccs.courtCaseId = cc.courtCaseId
WHERE
  cc.status NOT IN ('signing', 'draft') OR (cc.status = 'draft' AND cc.courtCaseStatisticsStatusTag = 'returned')
  