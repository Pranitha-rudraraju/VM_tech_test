import apache_beam as beam
from transaction import FilterTransactionAmountFn, FilterTransactionDateFn, FilterDataTimeColumnsFn


class ProcessData(beam.PTransform):  
    def expand(self, pcoll):    # Transform logic goes here.    
        return pcoll | beam.ParDo(FilterTransactionAmountFn()) | beam.ParDo(FilterTransactionDateFn()) | beam.ParDo(FilterDataTimeColumnsFn()) | beam.CombinePerKey(sum) 
      

def run():    
    with beam.Pipeline() as pipeline:
      #data = beam.io.ReadFromText("gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv")
      data = pipeline | "Read data from GCS" >> beam.io.ReadFromText("transactions.csv", skip_header_lines=1) # # sample data downloaded and saved in the local file (transactions.txt) to read as i was not able to do on work pc due to networking issue. 
      transformed_data = data | ProcessData()
      output = transformed_data | beam.io.WriteToText('output/composite_transform_results', file_name_suffix= '.csv', header = 'date, total_amount') #| beam.Map(print)      


if __name__ == "__main__":
    run()
