SELECT
  ccp.courtCaseParticipantId AS id_api,
  ccp.courtCaseId AS court_case_id_api,
  t.identifier AS participant_identifier,
  CASE
    WHEN ccp.memberType = 'corporation' THEN corp.uuid
    WHEN ccp.memberType = 'citizen' THEN c.uuid
  END AS UUID,
  ccp.memberType AS member_type,
  ccp.joinedAt AS joined_at,
  ccp.leavedAt AS leaved_at
FROM
  CourtCaseParticipants ccp
  JOIN CourtCaseParticipantTypes t ON t.courtCaseParticipantTypeId = ccp.courtCaseParticipantTypeId
  JOIN Citizens c ON c.citizenId = ccp.memberId
  JOIN Corporations corp ON corp.corporationId = ccp.memberId
WHERE
  ccp.courtCaseParticipantTypeId IN (1, 2, 3, 4)