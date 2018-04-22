PROJECT NOTES
-------------

The deliverables provided will allow you to execute the Python classifier experiment as-is.  If you need to execute the ETL process
(which uses Apache Spark with Scala), there are additional instructions you will need to follow.


TO EXECUTE THE CLASSIFIER EXPERIMENT ONLY
-----------------------------------------

0) Prerequisites are
	-numpy (latest)
	-sklearn (latest)
1) Change directory to 'python'
2) Execute 'python3 experiment.py'

K-Fold cross-validation (basic and stratified) results will be printed to the console.
Classifier metrics for each tested prediction window (1-, 2-, 4-, 6-, and 8-hours) will be printed to the console.
Precision-Recall and AUC charts will be saved in the 'charts' folder.


TO EXECUTE THE SPARK/SCALA ETL PROCESS
--------------------------------------

0) Prerequisites (detailed in the SBT project file)
	-Spark 2.3
	-MIMIC-III database files available in the 'data' folder
		-CHARTEVENTS.CSV
		-ICUSTAYS.CSV
		-MICROBIOLOGYEVENTS.CSV
		-PATIENTS.CSV
		-PRESCRIPTIONS.CSV
2) Delete the entire 'output' folder (if this folder exists, the application assumes it can load pre-filtered Parquet files)
3) From the project root, run 'sbt compile run' for each prediction window (1, 2, 4, 6, 8)
	-In Main.scala, main(), set 'predWindow=1' initially.
		-The first execution will run for 45-60 minutes, and store filtered Parquet files
	-Upon completion, set 'predWindow' to the next value and execute again
		-Subsequent runs will complete in 1-2 minutes
4) In 'output/svmoutput', go through each 'featuresXhour' folder and rename the .libsvm file to 'features.libsvm'
5) Follow previous instructions to execute the Python classifier