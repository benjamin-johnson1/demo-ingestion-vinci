# demo-ingestion-vinci
ingestion project for generated airports data

## Repository Structure

 - **environments/** : Terraform root modules for each the environment (demo)
 - **modules/**      : Terraform sub-modules (contains the main 'usecase' module and init module)
 - **schemas/**      : BigQuery tables schemas
 - **dags/**          : pyhton files defining DAGs

## Branching model

- **main**: Main branch, represents the latest state of the project
- **env/demo**: developement environement 

## How to contribute
- Each new feature should be push to env/demo derived from the '**main**' branch.
- Once the feature is ready, it is merged on the '**main**' branch through a Pull Request.
- Before merging, make sure to review your Apply Trigger in Cloud build

## Monitoring & Alerting
Moniroting is handled directly by airflow with email_on_failure parameter
Monitoring can be review in the audit table of bigquery after an ingestion or directly on the airflow environement. 
Alerting policy is not activated but it could be the case if the project grows and bring other dependencies

## Usage

## PART 1 : Project CI/CD Initialization

### Step 1. Create new repository

If you want to initialize this project on a GCP project, you can create a new Github repository by clonning this repository .
Here is a detailed description of the repository:


### Step 2. Prepare your GCP Project + setup your local machine

create a new GCP project 

#### Install Cloud SDK
See official documentation:
> https://cloud.google.com/sdk/docs/install
#### Install Terraform
See official documentation:
> https://www.terraform.io/downloads.html

#### Generate Application Default Credentials
Run the following command to generate an ADC used by the Terraform google provider:

`> gcloud auth application-default login`

All calls to Google Cloud APIs will be authenticated with the account used to generate the ADC.


### Step 3. Manual setup of your project + CI/CD pipeline

you must perform manual steps.

#### Initialization

* 1 - In the environements > demo > main.tf comment the module "demo_ingestion" part (for now)

* 2 - You must use a the cloudbuild-init.yaml file to run the initialization. To do so, you have to create manually a trigger in Cloud build that will be linked to your repo and wich will use cloudbuild-init.yaml as a config file. 

* 3 - In this Trigger you must set_up the variables : 
    _APPLY_CHANGES: 'true'
    _ENV: 'demo'
    _INIT: 'true'
    It will create the bucket to store the state of the terraform. And the service account "deploy" that you will use after to deploy your ressources.

* 4 - After the run you can remove the manual trigger as the terraformed ones will be created. You can uncomment the module part that will be able to run with your new "deploy" service account

* 5 - Whenever there is a new pull request from env/demo from on main, a CI/CD pipeline will be triggered, you can follow the build execution logs via the GCP web UI.

## Run Airflow Locally

### we will use local composer feature 

the benefits of that feature is that we can have a like to like environement with what we will have in our prod environement with a minimum effort. We can also use new versions to see the impacts before pushing to prod.
See official documentation:
> https://cloud.google.com/composer/docs/composer-2/run-local-airflow-environments

Repo to run Composer locally: 
> https://github.com/GoogleCloudPlatform/composer-local-dev

Advices : clone the repo on an other folder then execute the commands where you have your local dags

#### Steps 
* 1 - run the following commands to access your google cloud environement
```
gcloud auth application-default login
```
* 2 - Install the composer-local-dev in an other repository
```
git clone https://github.com/GoogleCloudPlatform/composer-local-dev.git
cd demo-ingestion-vinci/environements/demo_ingestion/dags
pip install ../../../../composer-local-dev
```

* 3 - Create composer environement
```
composer-dev create \
  --from-image-version composer-2.12.1-airflow-2.10.5 \
  --project demo-ingestion-vinci \
  --port 8080 \
  --dags-path ./demo-ingestion-vinci/environements/demo_ingestion/dags \
  demo

```

* 4 - you can add to gitignore
```
/composer/composer-local/*
!/composer/composer-local/requirements.txt
__pycache__
```
* 5 - you can edit requiremnts.txt  
```
# Core dependencies
apache-airflow>=2.5.0
apache-airflow-providers-google>=8.0.0
google-cloud-storage>=2.7.0
google-cloud-bigquery>=3.3.5
google-cloud-pubsub>=2.13.0

# Typing support
typing-extensions>=4.4.0

# Optional but recommended for better error handling
tenacity>=8.1.0

# For datetime operations
python-dateutil>=2.8.2

# For environment variable management (optional but recommended)
python-dotenv>=0.21.0
```


* 6 - run and re-run
```
composer-dev start composer-local
composer-dev restart composer-local
```
you may experienced issue with the initialization of the database and you may need to restart it. if you update requirement.txt you can also restart to apply the changes

* 7 you can start the dag

## Improvements : 

* 1 addition of assertions to check the quality of the data
* 2 addition of a deleting bucket if we have ingested wrong data
* 3 addition of a deduplication layer in airport transformation (following a primary key for exemple) to save cost if the project goes to prod
* 4 as per the diagram, addition of a pub sub between gloud storage and composer if we want to make the flow event driven. if we want more flexibility we can use a cloud function or a cloud run

Thank you for the review ! i have decided to do this exercice in Composer airflow even if i was more confortable with workflows or dataform to demonstrate my capacity to handle a new solution in less than a day and to match the exiercice . I hope it was intersting to review this code !