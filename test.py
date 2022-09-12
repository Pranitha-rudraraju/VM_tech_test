
# importing the function from main(transactions.py)

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import unittest
import apache_beam as beam
from transaction import FilterTransactionAmountFn, FilterTransactionDateFn, FilterDataTimeColumnsFn

# ************************ Testing the data for amount >20 ********************************

class VMTest(unittest.TestCase):
  def test_amount(self):
    # Our static input data, which will make up the initial PCollection.
    input_data = [
       "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,15.99",
      "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
      "2017-08-31 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
      "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,0.0003",
      "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,1020",
      "2008-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,130.12"
    ]
   
    filter_amount_expected_results = ["2017-08-31 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
                        "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,1020",
                        "2008-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,130.12"]
    
    # Create a test pipeline.
    with TestPipeline() as p:
      # Create an input PCollection.
      input = p | beam.Create(input_data)
      # Apply the transform under test.
      filter_amount_test = input | beam.ParDo(FilterTransactionAmountFn())
   
      # Assert on the results.
      assert_that(
        filter_amount_test,
        equal_to( filter_amount_expected_results ))

# ************************ transactions made before the year `2010` ********************************

  def test_date(self):
            # Our static input data, which will make up the initial PCollection.
    input_data = [
       "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,15.99",
      "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
      "2017-08-31 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
      "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,0.0003",
      "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,1020",
      "2008-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,130.12"
    ]
   
    filter_date_expected_results = ["2017-08-31 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
                        "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,1020"
                        ] 

    # Create a test pipeline.
    with TestPipeline() as p:
      # Create an input PCollection.
      input = p | beam.Create(input_data)
      # Apply the Count transform under test.
      filter_amount_test = input | beam.ParDo(FilterTransactionAmountFn())
      filter_date_test = filter_amount_test | beam.ParDo(FilterTransactionDateFn())
      # Assert on the results.
       
      assert_that(
        filter_date_test,
        equal_to( filter_date_expected_results ))

# ************************ Sum the total by `date` ********************************

  def test_total_amount(self):
            # Our static input data, which will make up the initial PCollection.
    input_data = [
       "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,15.99",
      "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
      "2017-08-31 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
      "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,0.0003",
      "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,1020",
      "2008-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,130.12"
    ]
   
    filter_date_amount_expected_results = [("2017-08-31" ,3122.22)]
                                                                   

    # Create a test pipeline.
    with TestPipeline() as p:
      # Create an input PCollection.
      input = p | beam.Create(input_data)
      # Apply the total_amount by date transform under test.
      filter_amount_test = input | beam.ParDo(FilterTransactionAmountFn())
      filter_date_test = filter_amount_test | beam.ParDo(FilterTransactionDateFn())
      filter_column_date_amount_test = filter_date_test | beam.ParDo(FilterDataTimeColumnsFn()) | beam.CombinePerKey(sum)

      # Assert on the results.
       
      assert_that(
        filter_column_date_amount_test,
        equal_to( filter_date_amount_expected_results ))
      
      

  