SELECT
  c.uuid,
  c.citizenship,
  p.psn,
  p.firstName AS first_name,
  p.lastName AS last_name,
  p.patronymic,
  p.dateBirth AS birth_date,
  p.deathDate AS death_date,
  p.gender
FROM
  Citizens c
  LEFT JOIN Profiles p ON p.profileId = c.profileId