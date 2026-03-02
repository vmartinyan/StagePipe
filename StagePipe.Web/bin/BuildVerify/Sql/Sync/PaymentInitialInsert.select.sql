SELECT
  tr.payable_id AS court_case_id_api,
  tr.amount,
  tr.updatedAt AS payment_date,
  tr.order_number AS order_number,
  'e_payments' AS type
FROM
  e_payments_transactions tr
WHERE
  tr.status = 'confirmed'
UNION ALL
SELECT
  ext.payableId AS court_case_id_api,
  ext.amount,
  ext.updatedAt AS payment_date,
  ext.pin AS order_number,
  CASE
    WHEN ext.type = 'e-payment' THEN 'e_payments_ext'
    WHEN ext.type = 'client-treasury' THEN 'client_treasury'
  END AS type
FROM
  external_payments ext