import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json


# -----------------------
# CONFIGURATION
# -----------------------
PROJECT_ID = "integral-vim-486413-k2"
REGION = "us-central1"
JOB_NAME = "smartmeter-preprocess2"
TEMP_LOCATION = "gs://integral-vim-486413-k2-bucket/temp"
SERVICE_ACCOUNT_EMAIL = "dataflow-worker-sa@integral-vim-486413-k2.iam.gserviceaccount.com"

INPUT_SUBSCRIPTION = "projects/integral-vim-486413-k2/subscriptions/smartMeterReadings-dataflow-sub"
OUTPUT_TOPIC = "projects/integral-vim-486413-k2/topics/smartMeterReadingsProcessed"


# -----------------------
# HELPER FUNCTIONS
# -----------------------

def filter_valid(element):
    """Filter out records with missing or invalid measurements"""
    try:
        if element.get("temperature") is None:
            return False
        if element.get("pressure") is None:
            return False

        # Ensure they are numeric
        float(element["temperature"])
        float(element["pressure"])
        return True
    except:
        return False


def convert_units(element):
    """Convert temperature (C → F) and pressure (kPa → psi)"""
    try:
        temp_c = float(element["temperature"])
        pressure_kpa = float(element["pressure"])

        return {
            "temperature": temp_c * 1.8 + 32,
            "pressure": pressure_kpa / 6.895
        }
    except:
        return None


# -----------------------
# PIPELINE
# -----------------------

def run():

    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.region = REGION
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.temp_location = TEMP_LOCATION
    google_cloud_options.staging_location = TEMP_LOCATION
    google_cloud_options.service_account_email = SERVICE_ACCOUNT_EMAIL

    standard_options = options.view_as(StandardOptions)
    standard_options.runner = "DataflowRunner"
    standard_options.streaming = True

    with beam.Pipeline(options=options) as p:

        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
                subscription=INPUT_SUBSCRIPTION
            )
            | "DecodeJSON" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
            | "FilterInvalid" >> beam.Filter(filter_valid)
            | "ConvertUnits" >> beam.Map(convert_units)
            | "FilterFailedConversions" >> beam.Filter(lambda x: x is not None)
            | "EncodeToJSON" >> beam.Map(lambda x: json.dumps(x).encode("utf-8"))
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=OUTPUT_TOPIC)
        )


if __name__ == "__main__":
    run()
