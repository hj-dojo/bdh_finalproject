SELECT icd9_code, short_title, long_title
FROM  d_icd_diagnoses
WHERE  (
  LOWER(long_title) LIKE '%sepsis%' OR
  LOWER(long_title) LIKE '%septicemia%' OR
  LOWER(long_title) LIKE '%septicernia%' OR
  LOWER(long_title) LIKE '%meningococcemia%' OR
  LOWER(short_title) LIKE '%septic shock%'
  )
AND LOWER(long_title) NOT LIKE '%puerperal%'
AND LOWER(long_title) NOT LIKE '%labor%'
AND LOWER(long_title) NOT LIKE '%newborn%'
AND LOWER(long_title) NOT IN ('herpetic septicemia', 'anthrax septicemia')
