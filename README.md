# Lending Data Analytics Project

## Problem Description

This project addresses the challenges in the lending industry by creating an end-to-end data pipeline for processing and analyzing lending club data. It enables financial institutions to:

- Process and clean large volumes of lending data for analysis
- Identify patterns in loan defaults and repayments
- Create risk profiles for borrowers
- Generate loan scores to assess the quality of loans
- Provide actionable insights through analytics dashboards

The platform helps lending institutions make better-informed decisions, reduce default risks, and optimize their lending strategies through data-driven insights.

## Architecture
![Architecture](./public/architecture.png)

## Data Pipeline Flow

```mermaid
flowchart LR
    Load[1. Data Loading] --> CleanCust[2. Customer\nData Cleaning]
    CleanCust --> CleanLoan[3. Loan\nData Cleaning]
    CleanLoan --> CleanRepay[4. Repayment\nData Cleaning]
    CleanRepay --> CleanDef[5. Defaulter\nData Cleaning]
    CleanDef --> CleanDefDetailed[6. Detailed Defaulter\nData Cleaning]
    CleanDefDetailed --> BQTables[7. BigQuery\nTable Creation]
    BQTables --> UnifiedView[8. Unified View\nCreation]
    UnifiedView --> FilterBad[9. Bad Data\nFiltering]
    FilterBad --> LoanScore[10. Loan\nScoring]
```

## Cloud Infrastructure

This project is fully developed in the Google Cloud Platform (GCP) with Infrastructure as Code (IaC) principles using:

- **Google Cloud Storage (GCS)**: Data lake for raw and processed data
- **Google Dataproc**: Managed Spark service for data processing
- **BigQuery**: Data warehouse
- **Metabase**: Data visualization and dashboards
- **Terraform**: Infrastructure as Code for reproducible environments
- **Kestra**: Workflow Orchestraton
- **Docker**: Containers

## Data Ingestion and Workflow Orchestration

The project implements a comprehensive batch data processing pipeline with:

1. Data ingestion from source to GCS data lake
2. Multiple transformation steps with dependencies
3. Orchestrated workflow using Kestra

![Workflow](./public/workflow.jpeg)


The data pipeline is structured as a sequence of PySpark jobs:

```mermaid
flowchart LR
    Load[1. Data Loading] --> CleanCust[2. Customer\nData Cleaning]
    CleanCust --> CleanLoan[3. Loan\nData Cleaning]
    CleanLoan --> CleanRepay[4. Repayment\nData Cleaning]
    CleanRepay --> CleanDef[5. Defaulter\nData Cleaning]
    CleanDef --> CleanDefDetailed[6. Detailed Defaulter\nData Cleaning]
    CleanDefDetailed --> BQTables[7. BigQuery\nTable Creation]
```

Each step is a PySpark job submitted to Dataproc with appropriate dependencies using Kestra orchestrator.

## Data Warehouse Schema

```mermaid
erDiagram
    CUSTOMER ||--o{ LOAN : takes}
    LOAN ||--o{ REPAYMENT : has}
    LOAN ||--o{ DEFAULTER : may_have}
```

- Tables are partitioned by date fields (e.g., loan issue date)
- Clustered by frequently queried fields (e.g., member_id, loan_status)
- Star schema design with fact and dimension tables for efficient querying

This structure optimizes query performance for the analytics use cases, reducing query costs and improving response time.

## Data Transformations

The project uses Spark for comprehensive data transformations:

- Data cleaning and normalization
- Feature engineering for analytics
- Complex aggregations and calculations
- Creation of unified views for analytics

PySpark scripts handle all transformations with a focus on scalability and performance:

```mermaid
flowchart TB
    subgraph "Data Transformation Process"
        direction TB
        
        raw[Raw Data] --> clean[Data Cleaning]
        clean --> norm[Data Normalization]
        norm --> feature[Feature Engineering]
        feature --> agg[Aggregations]
        agg --> score[Loan Scoring]
        
        classDef process fill:#f9f,stroke:#333,stroke-width:2px
        class clean,norm,feature,agg,score process
    end
```

## Dashboard

The analytics dashboard in Metabase provides:

1. Loan Performance Overview
   - Distribution of loans by status, grade, and purpose
   - Temporal trends in loan issuance and repayment

2. Risk Analysis Dashboard
   - Default rate analysis by demographic segments
   - Loan score distribution and risk categorization

![Dashboard](./public/dashboard.png)




## Reproducibility

### Prerequisites

- Google Cloud Platform account with billing enabled
- `gcloud` CLI installed and configured
- Terraform installed (for infrastructure setup)


### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/abhayra12/lending_data_analytics.git
   cd lending-data-analytics
   ```

2. **Set up GCP credentials**
   ```bash
   # Set up a service account with appropriate permissions and download the JSON key
   # Save it as gcp-creds.json in the project root
   export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/gcp-creds.json"
   ```

3. **Deploy infrastructure with Terraform**
   ```bash
   cd terraform
   terraform init
   terraform apply
   ```

4. **Upload initial data to GCS**
   
   I have created a consolidated script for downloading Kaggle data and uploading scripts to GCS:
   
   ```bash
   # Install required dependencies
   pip install -r requirements.txt
   
   # Download Kaggle data and upload to GCS
   python scripts/gcs_upload.py lending_ara --kaggle
   
   # Upload Python scripts to GCS
   python scripts/gcs_upload.py lending_ara --scripts
   
   # Or do both at once
   python scripts/gcs_upload.py lending_ara --all
   ```

   This script:
   - Downloads the Lending Club dataset from Kaggle directly to your local `data/` folder
   - Automatically finds the CSV files in the downloaded dataset
   - Copies the target CSV file (accepted_2007_to_2018Q4.csv) to your data directory
   - Uploads the CSV file to the GCS bucket
   - Optionally uploads all Python scripts to the `code/` folder in the GCS bucket
   
   Note: You need to have Kaggle credentials configured before running this script.

5. **Run the data pipeline**

   I have created a Kestra workflow for the data pipeline.
   Start the Kestra Service by running the following command:
   ```bash
   cd docker/kestra
   docker compose up -d
   ```
   Go to the Kestra UI by running the following command:
   ```bash
   open http://localhost:8080
   ```
   copy the flows from `docker/kestra/flows` to the Kestra UI.
   execute the flows in the kestra ui.

   ![Workflow](./public/kestra-flow.png)
   

6. **Access the dashboard**
   Start the Metabase Service by running the following command:
   ```bash
   cd docker/metabase
   docker compose up -d
   ```
   Go to the Metabase UI by running the following command:
   ```bash
   open http://localhost:3000
   ```
   Create a new Metaabase account and login.
   Select the `Lending Club` database and the `loan_data` table.
### Troubleshooting

- If you encounter issues with Dataproc job submission, check the cluster logs
- For BigQuery access issues, verify service account permissions
- For data related issues, check the intermediate outputs in GCS

## Future Enhancements

- Add real-time data processing for immediate insights

## Contributors

- Abhay https://github.com/abhayra12