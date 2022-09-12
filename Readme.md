# Data Engineer Tech Test #

Environment setup for Apache beam batch job:
   1) installed apache beam in vscode using `pip install apache-beam`
   2) creating virtual environment using `python -m venv .venv` to run the pipeline.
   3) if you need to run the code you have to run `pip install -r requirements.txt`
   3) created project name called `VM_pipeline`
   4) downloaded the data from given location and saved as `transactions.csv`

Files created:
   1) `transactions.csv` file contains input data stored in the local file
   2) `transactions.py` file is the main file where all the requirements developed and executed
   3) `composite_transform.py` file is to transform all the pcoll in single composite transform
   4) `test.py` file contains all the necessary unit test cases
   5) `Readme.md` file contains notes for the batch file process.
   6)`output/results.csv` file for the optput data.
   7) `output/composite_transform_results.csv` file is the output for composite transform data.
   8) `requirements.txt` `pip install -r requirements.txt` command will install all the required packages and their dependencies which shown in the txt file.


# transactions.csv:
  Input location where the sample data stored in local from the given path.

# transaction.py:
  Step 1: Importing all the libraries.
  Step 2: Reading the data from file `transactions.csv`
  Step 3: defining the Class function "FilterTransactionAmountF" to find out all `transaction_amount` greater than `20`
  Step 4: defining the Class function "FilterTransactionDateF" to find out all transactions made before the year `2010`
  Step 5: defining the Class function to split the data and filter only data and the transaction_amount column for achieving Sum the total by `date`
  Step 6: execute the below command to run the code
          ``python transaction.py``
  Step 7: Writing the output data into the location `output/results.csv`

# composite_transform.py:
  Step 1: Importing apache beam and all the libraries from transaction.py file.
  Step 2: Reading the data from file `transactions.csv`.
  Step 3: Creating the composite transform in the subclass of pTransform class and override the  expand method to spacify the actual processing logic.
  Step 4: In expand, we take a pcollection as an input and applying all transforms to it in one go.
  Step 5: Now process data has all transforms included in it.
  Step 6: execute the below command to run the code
          ``python composite_transform.py``
  Step 7: Finally writing the output data the location into `output/composite_transform_results`


# test.py
  Step 1: Importing apache beam and all the libraries from transaction.py file.
  Step 2: Giving the input values into class 'VMTest'.
  Step 3: Defining the expected results.
  Step 4: Created a test pipeline.
  Step 5: Created an input PCollection.
  Step 6: Applied the transform under test.
  Step 7: Assert on the results.
  Step 8: execute the below command to execute the test results.
          ``python -m unittest test.VMTest``
  Step 9: repeat the same steps for executing all the three transforms.

# Readme.md
  Sample notes available for the e2e project.

# output/results.csv
  Output is written at this location.


    
