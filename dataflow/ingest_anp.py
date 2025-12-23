import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import requests
import csv
import json
import io
from datetime import datetime

ANP_URL = "https://dados.gov.br/dados/conjuntos-dados/precos-de-combustiveis"

class FetchANPData(beam.DoFn):
    def process(self, element):
        response = requests.get(element, timeout=30)
        response.raise_for_status()

        content = response.content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))

        for row in reader:
            row["ingestion_timestamp"] = datetime.utcnow().isoformat()
            yield row


def run():

    service_account = "PROJECT"

    pipeline_options = {
        "runner": "DataflowRunner",
        "project": "PROJECT",
        "region": "us-east1",
        "job_name": f"anp-ingestion-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "staging_location": "gs://bk_anp_raw-dataflow-staging/staging",
        "temp_location": "gs://bk_anp_raw-dataflow-temp/temp",
        "service_account_email": service_account,
        "save_main_session": True,
        "install_library": ["requests"],
    }

    options = PipelineOptions(flags=[], **pipeline_options)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Create URL" >> beam.Create([ANP_URL])
            | "Fetch ANP Data" >> beam.ParDo(FetchANPData())
            | "To JSON" >> beam.Map(json.dumps)
            | "Write Bronze" >> beam.io.WriteToText(
                "gs://bk_anp_raw/output/anp",
                file_name_suffix=".json"
            )
        )


if __name__ == "__main__":
    run()
