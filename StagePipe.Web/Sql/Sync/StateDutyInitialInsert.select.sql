SELECT
  courtCaseId as court_case_id_api,
  pin,
  date,
  paidType as paid_type,
  checkNumber as check_number,
  receiptNumber as receipt_number
FROM
  StateDuty
  WHERE deletedAt IS NULL