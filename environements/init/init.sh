# Small script to initialize ressources before being able to use Terraform and Cloud Build.
TERRAFORM_BUCKET=$1-gcs-tfstate

gsutil mb -l eu -p $1 gs://$TERRAFORM_BUCKET # Create Terraform bucket
gsutil versioning set on gs://$TERRAFORM_BUCKET # Set versioning on

gsutil mb -l eu -p $1 gs://$1-gcs-cloud-build-logs # Create Cloud Builds Logs bucket
gsutil versioning set on gs://$1-gcs-cloud-build-logs # Set versioning on
gsutil lifecycle set ./log-bucket-config.json gs://$1-gcs-cloud-build-logs