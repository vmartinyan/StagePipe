SELECT
  c.uuid,
  c.citizenship,
  c.taxId as tax_id,
  c.name,
  c.type,
  c.slug
FROM
  Corporations c