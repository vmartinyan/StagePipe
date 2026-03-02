SELECT
  cc.uuid as court_case_uuid,
  c.type,
  c.coefficient,
  ccc.updated_at AS modified_at
FROM
  court_case_complexity ccc
  JOIN complexities c ON c.id = ccc.complexity_id
  JOIN court_cases cc ON cc.id = ccc.court_case_id
  WHERE cc.status <> 'sent'