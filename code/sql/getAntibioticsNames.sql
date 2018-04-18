SELECT distinct LOWER(drug)
FROM PRESCRIPTIONS
WHERE (
  LOWER(drug) LIKE '%amikacin%' or
  LOWER(drug) LIKE '%gentamicin%' or
  LOWER(drug) LIKE '%kanamycin%' or
  LOWER(drug) LIKE '%neomycin%' or
  LOWER(drug) LIKE '%netilmicin%' or
  LOWER(drug) LIKE '%tobramycin%' or
  LOWER(drug) LIKE '%paromomycin%' or
  LOWER(drug) LIKE '%streptomycin%' or
  LOWER(drug) LIKE '%spectinomycin%' or
  LOWER(drug) LIKE '%geldanamycin%' or
  LOWER(drug) LIKE '%herbimycin%' or
  LOWER(drug) LIKE '%rifaximin%' or
  LOWER(drug) LIKE '%loracarbef%' or
  LOWER(drug) LIKE '%ertapenem%' or
  LOWER(drug) LIKE '%doripenem%' or
  LOWER(drug) LIKE '%Imipenem%' or
  LOWER(drug) LIKE '%cilastatin%' or
  LOWER(drug) LIKE '%meropenem%' or
  LOWER(drug) LIKE '%cefadroxil%' or
  LOWER(drug) LIKE '%cefazolin%' or
  LOWER(drug) LIKE '%cefalexin%' or
  LOWER(drug) LIKE '%cefaclor%' or
  LOWER(drug) LIKE '%cefprozil%' or
  LOWER(drug) LIKE '%cefuroxime%' or
  LOWER(drug) LIKE '%cefixime %' or
  LOWER(drug) LIKE '%cefdinir%' or
  LOWER(drug) LIKE '%cefditoren%' or
  LOWER(drug) LIKE '%cefoperazone%' or
  LOWER(drug) LIKE '%cefotaxime%' or
  LOWER(drug) LIKE '%cefpodoxime%' or
  LOWER(drug) LIKE '%ceftazidime %' or
  LOWER(drug) LIKE '%ceftibuten%' or
  LOWER(drug) LIKE '%ceftriaxone%' or
  LOWER(drug) LIKE '%cefepime%' or
  LOWER(drug) LIKE '%ceftaroline%' or
  LOWER(drug) LIKE '%ceftobiprole%' or
  LOWER(drug) LIKE '%teicoplanin%' or
  LOWER(drug) LIKE '%vancomycin%' or
  LOWER(drug) LIKE '%telavancin%' or
  LOWER(drug) LIKE '%dalbavancin%' or
  LOWER(drug) LIKE '%oritavancin%' or
  LOWER(drug) LIKE '%clindamycin%' or
  LOWER(drug) LIKE '%lincomycin%' or
  LOWER(drug) LIKE '%daptomycin%' or
  LOWER(drug) LIKE '%azithromycin%' or
  LOWER(drug) LIKE '%clarithromycin%' or
  LOWER(drug) LIKE '%erythromycin%' or
  LOWER(drug) LIKE '%roxithromycin%' or
  LOWER(drug) LIKE '%telithromycin%' or
  LOWER(drug) LIKE '%spiramycin%' or
  LOWER(drug) LIKE '%aztreonam%' or
  LOWER(drug) LIKE '%furazolidone%' or
  LOWER(drug) LIKE '%nitrofurantoin%' or
  LOWER(drug) LIKE '%linezolid%' or
  LOWER(drug) LIKE '%posizolid%' or
  LOWER(drug) LIKE '%radezolid%' or
  LOWER(drug) LIKE '%torezolid %' or
  LOWER(drug) LIKE '%amoxicillin%' or
  LOWER(drug) LIKE '%ampicillin%' or
  LOWER(drug) LIKE '%azlocillin%' or
  LOWER(drug) LIKE '%dicloxacillin%' or
  LOWER(drug) LIKE '%flucloxacillin%' or
  LOWER(drug) LIKE '%mezlocillin %' or
  LOWER(drug) LIKE '%methicillin%' or
  LOWER(drug) LIKE '%nafcillin%' or
  LOWER(drug) LIKE '%oxacillin%' or
  LOWER(drug) LIKE '%penicillin%' or
  LOWER(drug) LIKE '%piperacillin%' or
  LOWER(drug) LIKE '%temocillin%' or
  LOWER(drug) LIKE '%ticarcillin%' or
  LOWER(drug) LIKE '%clavulanate%' or
  LOWER(drug) LIKE '%sulbactam%' or
  LOWER(drug) LIKE '%amoxicillin%' or
  LOWER(drug) LIKE '%ampicillin%' or
  LOWER(drug) LIKE '%sulbactam%' or
  LOWER(drug) LIKE '%piperacillin%' or
  LOWER(drug) LIKE '%tazobactam%' or
  LOWER(drug) LIKE '%bacitracin%' or
  LOWER(drug) LIKE '%colistin%' or
  LOWER(drug) LIKE '%polymyxin%' or
  LOWER(drug) LIKE '%ciprofloxacin%' OR
  LOWER(drug) LIKE '%enoxacin%' OR
  LOWER(drug) LIKE '%gatifloxacin%' OR
  LOWER(drug) LIKE '%gemifloxacin%' OR
  LOWER(drug) LIKE '%levofloxacin%' OR
  LOWER(drug) LIKE '%lomefloxacin%' OR
  LOWER(drug) LIKE '%moxifloxacin%' OR
  LOWER(drug) LIKE '%nadifloxacin%' OR
  LOWER(drug) LIKE '%nalidixic%' OR
  LOWER(drug) LIKE '%norfloxacin%' OR
  LOWER(drug) LIKE '%ofloxacin%' OR
  LOWER(drug) LIKE '%trovafloxacin%' OR
  LOWER(drug) LIKE '%grepafloxacin%' OR
  LOWER(drug) LIKE '%sparfloxacin%' OR
  LOWER(drug) LIKE '%temafloxacin%' OR
  LOWER(drug) LIKE '%mafenide%' OR
  LOWER(drug) LIKE '%sulfacetamide%' OR
  LOWER(drug) LIKE '%sulfadiazine%' OR
  LOWER(drug) LIKE '%sulfadimethoxine%' OR
  LOWER(drug) LIKE '%sulfamethizole%' OR
  LOWER(drug) LIKE '%sulfamethoxazole%' OR
  LOWER(drug) LIKE '%sulfanilimide%' OR
  LOWER(drug) LIKE '%sulfasalazine%' OR
  LOWER(drug) LIKE '%sulfisoxazole%' OR
  LOWER(drug) LIKE '%sulfonamidochrysoidine%' OR
  LOWER(drug) LIKE '%demeclocycline%' OR
  LOWER(drug) LIKE '%doxycycline%' OR
  LOWER(drug) LIKE '%metacycline%' OR
  LOWER(drug) LIKE '%minocyclinen%' OR
  LOWER(drug) LIKE '%oxytetracycline%' OR
  LOWER(drug) LIKE '%tetracycline%' OR
  LOWER(drug) LIKE '%clofazimine%' OR
  LOWER(drug) LIKE '%dapsone%' OR
  LOWER(drug) LIKE '%capreomycin%' OR
  LOWER(drug) LIKE '%cycloserine%' OR
  LOWER(drug) LIKE '%ethambutol%' OR
  LOWER(drug) LIKE '%ethionamide%' OR
  LOWER(drug) LIKE '%isoniazid%' OR
  LOWER(drug) LIKE '%pyrazinamide%' OR
  LOWER(drug) LIKE '%rifampicin%' OR
  LOWER(drug) LIKE '%rifabutin%' OR
  LOWER(drug) LIKE '%rifapentine%' OR
  LOWER(drug) LIKE '%streptomycin%' OR
  LOWER(drug) LIKE '%arsphenamine%' OR
  LOWER(drug) LIKE '%chloramphenicol%' OR
  LOWER(drug) LIKE '%fosfomycin%' OR
  LOWER(drug) LIKE '%fusidic%' OR
  LOWER(drug) LIKE '%metronidazole%' OR
  LOWER(drug) LIKE '%mupirocin%' OR
  LOWER(drug) LIKE '%platensimycin%' OR
  LOWER(drug) LIKE '%quinupristin%' OR
  LOWER(drug) LIKE '%dalfopristin%' OR
  LOWER(drug) LIKE '%thiamphenicol%' OR
  LOWER(drug) LIKE '%tigecycline%' OR
  LOWER(drug) LIKE '%tinidazole%' OR
  LOWER(drug) LIKE '%trimethoprim%'
  )
AND drug_type IN ('MAIN','ADDITIVE')
--AND route NOT IN ('OU','OS','OD','AU','AS','AD', 'TP')
AND LOWER(route) NOT LIKE '%ear%'
AND LOWER(route) NOT LIKE '%eye%'
AND LOWER(drug) NOT LIKE '%cream%'
AND LOWER(drug) NOT LIKE '%desensitization%'
AND LOWER(drug) NOT LIKE '%ophth oint%'
AND LOWER(drug) NOT LIKE '%gel%'
AND route IN ('IV','PO','PO/NG','ORAL', 'IV DRIP', 'IV BOLUS')
AND dose_unit_rx != 'Appl'
AND dose_unit_rx != 'DROP'
ORDER BY drug
