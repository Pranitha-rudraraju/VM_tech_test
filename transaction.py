from ast import Lambda
import apache_beam as beam
import time
from datetime import datetime, timezone

class FilterTransactionAmountFn(beam.DoFn):
    def process(self, element):
        row = element.split(',')
        transaction_amount = float(row[3])
        if transaction_amount > 20:
            yield element
        else:
            return

class FilterTransactionDateFn(beam.DoFn):
    def process(self, element):
        row = element.split(',')
        transaction_datetime = time.strptime(row[0], "%Y-%m-%d %H:%M:%S %Z")
        if transaction_datetime.tm_year >= 2010:
            yield element
        else:
            return

class FilterDataTimeColumnsFn(beam.DoFn):
    def process(self, element):
        row = element.split(',')
        transaction_datetime = str(datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc).date())
        transaction_amount = float(row[3])
        return [(transaction_datetime,transaction_amount)]

def run():
    with beam.Pipeline() as pipeline:
        data = pipeline | "Read data from GCS" >> beam.io.ReadFromText("transactions.csv", skip_header_lines=1) # | beam.Map(print)
        # data = beam.io.ReadFromText("gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv")
        transaction_amount_filtered_data = data | "Filtering out transaction amounts < 20" >> beam.ParDo(FilterTransactionAmountFn())  #| 'Count all elements' >> beam.combiners.Count.Globally() | beam.Map(print)
        date_filtered_data = transaction_amount_filtered_data | "Filtering out transaction dates before 2010" >> beam.ParDo(FilterTransactionDateFn())  #| 'Count all elements' >> beam.combiners.Count.Globally() | beam.Map(print)
        total_amount = date_filtered_data | "Sum of transactions by date" >> beam.ParDo(FilterDataTimeColumnsFn()) | beam.CombinePerKey(sum) #| beam.Map(print)
        output = total_amount | beam.io.WriteToText('output/results', file_name_suffix= '.csv', header = 'date, total_amount') # | beam.Map(print)     

if __name__ == "__main__":
    run()