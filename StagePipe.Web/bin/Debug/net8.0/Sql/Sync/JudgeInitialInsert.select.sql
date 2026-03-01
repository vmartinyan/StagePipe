SELECT
	u.id as id_auth,
	u.uuid,
	p.psn,
	p.first_name,
	p.last_name,
	p.patronymic_name AS patronymic,
	p.birth_date,
	p.death_date,
	CASE
		WHEN p.gender = 'male' THEN 'M'
		WHEN p.gender = 'female' THEN 'F'
		ELSE ''
	END AS gender,
	Case
		WHEN ur.court_instance = 'first_instance' THEN 1
		WHEN ur.court_instance = 'appellate' THEN 2
		WHEN ur.court_instance = 'cassation' THEN 3
		ELSE ''
	END as instance_id
FROM users u
JOIN user_role ur
    ON ur.user_id = u.id
JOIN profiles p
    ON p.id = u.profile_id
WHERE ur.system_role_id = 1;
