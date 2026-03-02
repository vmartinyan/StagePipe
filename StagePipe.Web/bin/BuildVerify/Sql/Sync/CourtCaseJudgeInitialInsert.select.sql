SELECT
  ccp.courtCaseId AS court_case_id_api,
  ccp.memberId AS judge_id_api,
  CASE
    WHEN ccp.participantNotice = 'head' THEN 'head'
    WHEN ccp.participantNotice = 'judge' THEN 'col_member'
    WHEN ccp.participantNotice = 'temporary' THEN 'authorized'
    ELSE 'assignee'
  END AS role,
  ccp.joinedAt as joined_at,
  ccp.leavedAt as leaved_at
FROM
  CourtCaseParticipants ccp
WHERE
  ccp.courtCaseParticipantTypeId = 6