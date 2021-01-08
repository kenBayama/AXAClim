# AXAClim


<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 658d1a1...  minor change in README.md
This Project is a project of processing phone company data.
=======
This Project is a project of processing data of a phone company data.
>>>>>>> cec6e26...  minor modification READ.me
<<<<<<< HEAD
=======
This Project is a project of processing phone company data.
>>>>>>> 873d68e...  minor modification READ.me
=======
>>>>>>> 658d1a1...  minor change in README.md


### Environnement ###

python = 3.7.9 

spark = 2.4.7

### Project Structure 
```
├── src/                                Contain all the source and the sample
│	├── conf/                           Documentation (.md/.rst files)
│	|   └── config.json                 Configuration path and file data for the spark job 
│	├── docs/                           Documentation (.md/.rst files)
│	├── dependencies/                   Dependencies files (.py) necessary for each spark jobs
│	├── data/
│	│   ├── raw/                        Raw Data for exercice 1 and 2
│	│   ├── processed/                  Processed Data. Storing the result of each exercice  (Create when the jobs run the first time)
│	│   │   ├── exercice1               
│	│   │   ├── exercice2
│	│   │   └── exercice3
│	└── jobs/                           Contain all the (.py) for the spark jobs
├── tests/                              Contain all the (.py) for unit test
│	└── test_data/                      
│	    ├── raw/                        Raw Data for exercice 1 and 2
│	    └── expected/                   Contains the expected result in avro format for each exercice  
│	        ├── exercice1
│	        ├── exercice2
│	        └── exercice3
├── README.md                           Intro to package
├── dependencies.zip                    Contains all the dependencies for the spark jobs
└── requirements.txt                    Lists dependencies
```


### Lauch the project #

-	Install and create a virtual environment :

			pip install virtualenv
			python3 -m venv datapipeline_env
			.\datapipeline_env\Scripts\activate

-	Install the required environnment for the project :
	
			pip install -r requirements.txt

	


-	Lauch **the three unit test** :
    
    From the root repo :
            python -m unittest discover tests

-	Lauch **the first exercice** :
		
            spark-submit --master local[*] --files src/conf/configs.json --py-files dependencies.zip .\src\jobs\csv_to_dataframe_job.py


***Execice1:*** :

The data is loaded from the src/data/raw folder

-	The purpose of the exercice is to clean the phone company client data


#### Processing csv_to_dataframe.csv 
The processing of the csv_to_dataframe.csv data induces the following transformations :

    Transformations : 

        Cleaning JSON column data by removing double quote 
            at the beggining and at the end to the string

        Exploding the JSON column into two distinct guid and poi columns

        Droping the JSON column

        Cleaning CLI_COEFF column by replacing commas by dots

        Cleaning CLI_TEL column BY removing the dots and the slashes

        Casting appropriate data types to the columns that requires it 

The result are stored in **the src/data/processed/exercice1 folder** in avro format



-	Lauch **the second exercice** :

            spark-submit --master local[*] --files src/conf/configs.json --py-files dependencies.zip --packages org.apache.spark:spark-avro_2.11:2.4.7 .\src\jobs\count_client_contract_job.py


***Exercice2:***

The data is loaded from the src/data/raw folder

-	The purpose of the exercice is to restructure the company client-contracts portofolio 
    data in order to have for each portofolio the number of contract which arrive 
    at its term for eachmonth of the coming year

   
#### Processing 02_campagne.parquet
The Processing of the 02_campagne.parquet data induces the following transformations :
    
    Transformations : 

        Aggregating the total number of client per portofolio 

        Aggregating the total number of contracts per portofolio

        Aggregating number of contracts per portofolio and per terms of contracts

        Pivoting the term of contracts for each portofolio
            and store the number of contract per portofolio
            and per terms of contract in newly created columns

        Renaming the newly created columns

        Joining all the resulting a dataFrame

The result are stored in **the src/data/processed/exercice2 folder** in avro format


-	Lauch **the third exercice** :

            spark-submit --master local[*] --files src/conf/configs.json --py-files dependencies.zip --packages org.apache.spark:spark-avro_2.11:2.4.7 .\src\jobs\top_client_per_portofolio_job.py


***Exercice3:***

The data is loaded from the src/data/processed/exercice2 folder

-	The purpose of the exercice is to filter the data to keep for each category of portofolio the top three elements
    with the greater number of clients :

In two steps.

Step 1 : 
-	Preprocessing the data fron exercice2 result in order to make the usable for the exercice3 question
    According to my understanding of the exercice2, I couldn't produce the data necessary to answer the exercice3 question.
    I modified the result of the exercice2 in order to be able to do so 

Step 2 :
-	 Processing the data to obtain the desired results

#### Step 1 : Preprocessiong src/data/processed/exercice2

The Preprocessing of the src/data/processed/exercice2 parquet data induces the following transformations  :

    Transformations : 

        Creating an UDF that creat an array of int 
            based on the data from an other column

        Creating a column N on which the udf n_to_array is applied

        Exploding the data in the n column and stored it in the MOCK column
            The rows are duplicated

        Multiplying the number of client  by an ascending value of integer 

        Droping the MOCK and N column  

#### Step 2 : Processing the result of the preprocessing phase

The Preprocessing of the results of the preprocesing induces the following transformations  :

    Transformations : 

        Partitioning data based on portofolio

        Ordering by the number of client  in descending order

        Creating a column to number each row

        Filtering the rows based on their numbering lesser than or equal 3 

        Droping the column used for numbering 

The result are stored in **the src/data/processed/exercice3 folder** in avro format


### Choice Explanations  ###


    Avro has been chosen as the storing file format for it is a row format. 
    It is mush efficient when reading all the dataset is needed, 
    which is why I used this format for storing the result of exercice 2, 
    as in exercice 1 we read all the data produced 
    I did the same for and all the data used for the unit tests. 
    Moreover, considering the size of the dataset produced, avro take less space. 
