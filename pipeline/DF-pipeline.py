import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.combiners import CountCombineFn

class Extraction(beam.DoFn):  # Best practice: Capitalize class names
    def process(self, element, *args, **kwargs):
        # FIX 1: ReadFromText reads raw text strings. Split the string by comma first.
        columns = element.split(',')
        
        try:
            row = {
                'customer_id': int(columns[0].strip()),
                'is_active': str(columns[5].strip())
            }
            yield row
        except (IndexError, ValueError) as e:
            # Prevents the entire pipeline from crashing on a bad data row
            print(f"Skipping malformed row: {element}. Error: {e}")

def run():
    parse = argparse.ArgumentParser()
    parse.add_argument("--input", required=True, help="GCP input file")
    parse.add_argument("--output", required=True, help="BigQuery Table")

    known_args, pipeline_args = parse.parse_known_args()

    options = PipelineOptions(pipeline_args)

    # FIX 2: Set the property using assignment '=', do not call it as a function ()
    options.view_as(StandardOptions).runner = "DataflowRunner"

    table_schema = {
        'fields': [
            {'name': 'is_active', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'total_users', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ]
    }

    # FIX 3: Pass your 'options' object into the pipeline
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read the csv file" >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            # FIX 4: Instantiate the class by adding parenthesis: Extraction()
            | "Extracting the Data" >> beam.ParDo(Extraction())

            #| "Write Data" >> beam.Map(print)

            
            # Now this lambda safe execution will never receive a NoneType
            | 'MapToKVPair' >> beam.Map(lambda row: (row['is_active'], 1))
            | 'SumPerClass' >> beam.CombinePerKey(sum)

            | "Write Data" >> beam.Map(print)
            
            
            | "Writing into BigQuery" >> beam.io.WriteToBigQuery(
                table=known_args.output,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
