SELECT itemid, label
FROM D_ITEMS
WHERE dbsource = 'metavision'
AND category IN ('Routine Vital Signs', 'Respiratory', 'Neurological')
AND (
  LOWER(label) like '%blood pressure%' OR
  label like '%BP%' OR
  LOWER(label) like '%heart rate%' OR
  LOWER(label) like '%respiratory rate%' OR
  label like '%RR%' OR
  LOWER(label) like '%pulseoxymetry%' OR
  LOWER(label) like '%temperature fahrenheit%' OR
  LOWER(label) like '%temperature celsius%' OR
  LOWER(label) like '%gcs%')
AND LOWER(label) not like '%orthostatic%'
AND LOWER(label) not like '%>%'