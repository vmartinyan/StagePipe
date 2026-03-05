select
	cc.courtCaseId as court_case_id_api,
	c.shortDescription as title,
	c.description
from
	CourtCases cc
left join Claims c on
	c.claimId = cc.claimId
where
	cc.status not in('draft', 'signing')