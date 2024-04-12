# Download the Parquet data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Download "Yellow Taxi Trip Records" for each month you want
# For the example, I downloaded the latest 12 months
# Download the lookup file https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
# Store them in a nytaxi sub-folder

GCP_LOCATION=us-east1

# Login
gcloud init --console-only

# Create a GCS bucket
gsutil mb -p sas-gelsandbox -l ${GCP_LOCATION} gs://sas-innovate/

# My GCP account
export MYGCPACCOUNT=<your-gcp-email-account@example.com>
# Set service account
export GCPSRVACCOUNT=<your-service-account>@<project>.iam.gserviceaccount.com

# Add the role storage.admin to the user account for the bucket
gsutil iam ch user:${MYGCPACCOUNT}:roles/storage.admin gs://sas-innovate/
# Add the role storage.admin to this service account for the bucket
gsutil iam ch serviceAccount:${GCPSRVACCOUNT}:roles/storage.admin gs://sas-innovate/

# Check the permissions set on the bucket
gsutil iam get gs://sas-innovate/ > /tmp/bucket_permissions.txt
cat /tmp/bucket_permissions.txt

# Upload nytaxi data (parquet and csv) to GCS
gsutil -m cp -r nytaxi gs://sas-innovate/data/

# Create a BigQuery dataset
bq mk --location=${GCP_LOCATION} --dataset --project_id sas-gelsandbox \
    --description "SAS Innovate BigQuery Dataset" \
    sas-gelsandbox:sas_innovate

# Load the Parquet file into 1 BigQuery table
bq load --source_format=PARQUET \
    sas-gelsandbox:sas_innovate.yellow_taxi_trips \
    gs://sas-innovate/data/nytaxi/*.parquet

# Load the csv file into 1 BigQuery table
bq load --autodetect \
    --source_format=CSV \
    sas-gelsandbox:sas_innovate.yellow_taxi_zone_lookup \
    gs://sas-innovate/data/nytaxi/taxi_zone_lookup.csv
