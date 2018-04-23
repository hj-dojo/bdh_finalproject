Early Detection of Sepsis from Real-Time Patient Vitals
=======================================================

This project seeks to accurately detect the onset of Sepsis using routine patient vital signals.

## DATA
We use data from the MIMIC-III (Medical Information Mart for Intensive Care III) database that holds deidentified health-related data associated with over forty thousand patients who stayed in critical care units of the Beth Israel Deaconess Medical Center between 2001 and 2012.
The database includes information such as demographics, vital sign measurements made at the bedside (~1 data point per hour), laboratory test results, procedures, medications, caregiver notes, imaging reports, and mortality (both in and out of hospital).

We downloaded the data from [physionet](https://mimic.physionet.org/) after completing mandatory CITI training for “Data or Specimens Only Research”. We used the following tables for our project.

**PATIENTS:** This table contains some demographic information about the patients including date of birth, age etc. We will link this information with the ICUSTAYS data to determine the age of the patients. The date of birth has been shifted for patients older than 89, so we did not include patients older than 89 in our analysis.

**ICUSTAYS:** This table gives information regarding a patient’s admission to the ICU and includes timing information for admission to ICU and discharge. We joined it with the PATIENTS table to get the age and gender of patients. We also used to make sure we were also targeting patients with ICU stays. 

**PRESCRIPTIONS:** This table contains details of patient’s medication orders, we use it to retrieve information of whether antibiotics were prescribed for the patient.  

**MICROBIOLOGYEVENTS**:This table Contains microbiology information, including tests performed and sensitivities, we use it to retrieve information of whether blood cultures were taken from the patient.  

**CHARTEVENTS**:This table contains all the charted data available for a patient.This electronic chart includes patients’ routine vital signs and all additional information relevant to their care: ventilator settings, laboratory values, code status, mental status, and so on. We retrieve routine vitals data for our feature construction from here.

The respective MIMIC-III database files associated to the above tables should be copied to the **data** folder.
* CHARTEVENTS.CSV
* ICUSTAYS.CSV
* MICROBIOLOGYEVENTS.CSV
* PATIENTS.CSV
* PRESCRIPTIONS.CSV

## MODULES
The work we have done is organized into three distinct modules:

### Pre-Processing
We used a set of sql commands to generate our static input data files

* **getAntibioticsNames.sql:** This query was run on the MIMIC III postgreSQL database to retrieve the the list of antibiotics names within the prescrition table (`antibiotics.txt`). The initial list of antibiotics was taken from [wikipedia](https://en.wikipedia.org/wiki/List_of_antibiotics).
* **getRelevantVitals.sql:** This query was run to create the `vitals_definitions.csv` file that contains all the ITEM_IDs associated with routine vitals. This list was further trimmed to just include a subset selecting non-invasive and most common routine measurement types.
* **getSepsisICD9Codes.sql:** This query was used to retrieve the diagnosis code used to identify an real sepsis diagnosis. This could be used to validate the patients identifed to have onset of sepsis against patients with a diagnosis of sepsis, but was not used in the project.

These results from these queries are also stored as static files in the **data** folder.
### Big Data Pipeline
This consists of the Apache Spark code in Scala that is used to do the data preparation, labeling, feature construction and test of some Spark classification algorithms. The scala code is in the `src` folder within the `bdhproject.sepsis` package. 

We use **Apache Spark 2.3.0** and used the spark-core, spark-mllib and spark-sql libraries.

The code structure is shown below:
```
src
└── main
    └── scala
        └── bdhproject
            └── sepsis
                ├── classification
                │   └── Modeling.scala
                ├── features
                │   └── FeatureConstruction.scala
                ├── ioutils
                │   ├── CSVUtils.scala
                │   └── ParquetUtils.scala
                ├── main
                │   └── Main.scala
                ├── model
                │   └── Models.scala
                └── windowing
                    └── TimeframeOperations.scala
```

* `Main.scala`: This contains the logic for reading the static data files as well as filtering the data from the raw CSV files required for our model. It supports running in both standalone mode and Spark cluster model, and tries to retrieve master URL from the spark job configuration, and on failure defaults to local mode. Command line arguments are supported allowing output directory path, prediction and observation window durations, and reload flag (to reload raw data) to be specified. If the output directory doesn't exist, the application will reload the data even if the reload flag is not set.
* `ParquetUtils.scala`: This contains the logic to read from and write to parquet files. It is used to store the computed features into parquet files for use in subsequent runs.
* `Models.scala`: This contains the case classes used to store information about ICUStays (including patient demographics), Prescriptions, MicrobiologyEvents and ChartEvents
* `CSVUtils.scala`: This is based on the logic provided with the homeworks, but uses Dataframes instead of RDDs and is used to read our raw data files.
* `TimeframeOperations.scala`: This contains the logic for calculating index dates. 
* `FeatureConstruction.scala`: This contains the logic to construct our features. The data within the observation window (indexdate-predictionwindowhours-observationwindowhours to indexdate-predictionwindowhours) is first extracted. Two methods of feature construction was attempted, the first aggregated the routine vitals, the second selected the latest values within the observation window. We found better results using the latest values.
* `Modeling.scala`: This contains the different classification algorithms that were attempted. We first attempted simple logistic regression. Then we tried a few different classification methods with validation (train/test split of 0.67 to 0.33) and used cross validation with parameter grid for best parameter selection; using Area under ROC as selection metric. StandardScaler was used to normalize the features before running the models. Classification metrics are returned for each classification method. We tried LogisticRegrestion, GradientBoostedTrees, MultiLayerPerceptron and RandomForest. RandomForest gave the best results, with GBT close behind that.

#### How to Run the code:

This can be run as a simple scala program by runing the following (and will use default parameters)
```
cd <parentdir>/bdh_finalproject/code
sbt compile run
```
**NOTE** There will be an "ERROR SparkContext: Error initializing SparkContext" exception as the program attempts to read the masterurk from default Spark configuration. This should be ignored

To run it in cluster mode, first create the jar file:
```
sbt compile package
```
Then run (make sure to use **--reload 1** for first run ):
```
spark-submit --master <masterurl> --class bdhproject.sepsis3.main.Main
         <target jar> [--reload 0/1] [--savedir dirpath] [--predwindow timeinhours] [--obswindow timeinhours]
```

### Classification
This consists of the python code to run the selected classification algorithm and generates metrics and charts.

The logic is handled in three specific files:
* `experiment.py`: This contains the main logic and reads the features files in the given directory. If directory parameter is not specified, the default uses feature files included in the submission. It also takes an argument for the observation window duration that is used to label charts. Each of the features files in the directory are processed and charts and metrics are produced.
* `classifier.py`: This contains the logic for running the different classification algorithms (all except the selected one are commented out). It also holds the logic to retrieve the metrics and plot the PR and ROC curves.
* `validate.py`: This holds the logic to runs K-fold cross validation on the selected classification algorithm. We tried both stratifield K-fold and basic K-fold cross validation.

#### Prerequisites
* python 3 (tested with 3.6.3)
* numpy (latest, tested with 1.13.3)
* sklearn (latest, tested with 0.19.1 )
* matplotlib (latest, tested with 2.1.0 )

#### How to Run the code:
To recreate the reported results, we can use the features files included in the submitted files and run:
```
cd <parentdir>/bdh_finalproject/code/python
python3 experiment.py 
```
Classifier metrics for each tested prediction window (1-, 2-, 4-, 6-, and 8-hours) along with K-Fold cross-validation (basic and stratified) results will be printed to the console. Precision-Recall and AUC charts will be saved in the 'charts' folder.

### Running the Complete Pipeline

For the same observation window duration, but different prediction window sizes run the following with different predwindow arguments. The reload flad can be **0** after the first run:

```
spark-submit --master <masterurl> --class bdhproject.sepsis.main.Main
         target/scala-2.11/bdhproject_sepsis_2.11-1.0.jar --reload 0 --savedir output --predwindow 4 --obswindow 24
```
Then run specifying the same observation window size and the path to the output directory path to the svmlight files :
```
cd python
python experiment.py -p output/svmoutput<obswinduration> -o <obswinduration>

```
