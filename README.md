# Data Architect Skills Test
## Section 1: Data Warehouse Design & Performance
#### 1. Design a star schema:
***a. Fact table: FactPatientEncounters:***
```
CREATE TABLE FactPatientEncounters (
    EncounterID BIGINT PRIMARY KEY,
    PatientID INT NOT NULL,
    ProviderID INT NOT NULL,
    HospitalID INT NOT NULL,
    DiagnosisID INT NOT NULL,
    DateID INT NOT NULL,
    BillingAmount DECIMAL(10,2),
    EncounterDuration INT,
    CONSTRAINT FK_Patient FOREIGN KEY (PatientID) REFERENCES DimPatient(PatientID),
    CONSTRAINT FK_Provider FOREIGN KEY (ProviderID) REFERENCES DimProvider(ProviderID),
    CONSTRAINT FK_Hospital FOREIGN KEY (HospitalID) REFERENCES DimHospital(HospitalID),
    CONSTRAINT FK_Diagnosis FOREIGN KEY (DiagnosisID) REFERENCES DimDiagnosis(DiagnosisID),
    CONSTRAINT FK_Date FOREIGN KEY (DateID) REFERENCES DimDate(DateID)
);
```
***b. Dimension tables: DimDate, DimPatient, DimProvider, DimDiagnosis, DimHospital***
```
CREATE TABLE DimDate (
    DateID INT PRIMARY KEY,
    Date DATE NOT NULL,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    DayOfWeek VARCHAR(10),
    IsHoliday BIT
);
CREATE TABLE DimPatient (
    PatientID INT PRIMARY KEY,
    PatientMRN VARCHAR(20),
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DateOfBirth DATE,
    Gender CHAR(1),
    IsActive BIT
);
CREATE TABLE DimProvider (
    ProviderID INT PRIMARY KEY,
    ProviderName VARCHAR(100),
    Specialty VARCHAR(50),
    LicenseNumber VARCHAR(20),
    IsActive BIT -- Tracks active provider status
);
CREATE TABLE DimDiagnosis (
    DiagnosisID INT PRIMARY KEY,
    DiagnosisCode VARCHAR(10), 
    DiagnosisDescription VARCHAR(200),
    Category VARCHAR(50)
);
CREATE TABLE DimHospital (
    HospitalID INT PRIMARY KEY,
    HospitalName VARCHAR(100),
    Region VARCHAR(50),
    State VARCHAR(2),
    FacilityType VARCHAR(50)
);
```
***Purpose:*** FactPatientEncounters stores metrics (e.g., BillingAmount) for aggregations, while dimension tables enable filtering and drill-downs (e.g., by region, diagnosis, or quarter). The schema supports queries like:
```
SELECT h.Region, d.DiagnosisDescription, SUM(f.BillingAmount) AS TotalBilling
FROM FactPatientEncounters f
JOIN DimHospital h ON f.HospitalID = h.HospitalID
JOIN DimDiagnosis d ON f.DiagnosisID = d.DiagnosisID
WHERE f.DateID IN (SELECT DateID FROM DimDate WHERE Year = 2025)
GROUP BY h.Region, d.DiagnosisDescription;
```
This query delivers insights (e.g., “Total billing: $1.2M for heart disease in Northeast”) for Tableau dashboards.
#### 2. Two performance strategies:
***1. Clustered Columnstore Index:***
```
CREATE CLUSTERED COLUMNSTORE INDEX CCI_FactPatientEncounters
ON FactPatientEncounters;
```
***2. Table Partitioning with Materialized Views:***
```
CREATE PARTITION FUNCTION PF_EncounterDate (INT)
AS RANGE RIGHT FOR VALUES (20230101, 20240101, 20250101);
CREATE PARTITION SCHEME PS_EncounterDate
AS PARTITION PF_EncounterDate TO (FG_2022, FG_2023, FG_2024, FG_2025);
ALTER TABLE FactPatientEncounters
ADD CONSTRAINT PK_FactPatientEncounters PRIMARY KEY (EncounterID, DateID)
ON PS_EncounterDate(DateID);

CREATE VIEW VW_EncountersByRegion
WITH SCHEMABINDING
AS
SELECT h.Region, d.Year, COUNT_BIG(*) AS EncounterCount, SUM(f.BillingAmount) AS TotalBilling
FROM dbo.FactPatientEncounters f
JOIN dbo.DimHospital h ON f.HospitalID = h.HospitalID
JOIN dbo.DimDate d ON f.DateID = d.DateID
GROUP BY h.Region, d.Year;
CREATE UNIQUE CLUSTERED INDEX IX_VW_EncountersByRegion ON VW_EncountersByRegion (Region, Year);

```
#### 3. Using AWS Glue to load S3 data into SQL Server:
***a. Schema Enforcement:***
I’d start by setting up an AWS Glue Crawler to scan S3 buckets containing CSV files. The crawler would infer the schema (e.g., EncounterID as integer, BillingAmount as decimal) and store metadata in the Glue Data Catalog. To enforce consistency, I’d review and refine the catalog’s schema to match the SQL Server star schema, ensuring columns like BillingAmount are defined as DECIMAL(10,2). In the Glue ETL job, I’d use PySpark to validate data against this schema, filtering out invalid rows (e.g., non-numeric BillingAmount values). For example, I’d check for valid decimal formats:
```
from pyspark.sql.functions import col
df = dynamic_frame.toDF()
df_valid = df.filter(col("BillingAmount").cast("decimal(10,2)").isNotNull())
```
***b. Handling Late-Arriving Data:***
To handle late-arriving data, such as delayed hospital files or updates to existing records, I’d design an idempotent pipeline. I’d create a staging table in SQL Server to hold incoming data, using EncounterID as a unique key for deduplication. The Glue ETL job would transform S3 data and load it into this staging table, checking for existing EncounterID values to avoid duplicates. I’d then use a SQL Server MERGE operation to upsert data into FactPatientEncounters:
```
MERGE INTO FactPatientEncounters AS target
USING Staging_Encounters AS source
ON target.EncounterID = source.EncounterID
WHEN NOT MATCHED THEN
    INSERT (EncounterID, PatientID, ProviderID, HospitalID, DiagnosisID, DateID, BillingAmount, EncounterDuration)
    VALUES (source.EncounterID, source.PatientID, source.ProviderID, source.HospitalID, source.DiagnosisID, source.DateID, source.BillingAmount, source.EncounterDuration);
```
#### 4. Recommendation as an analytics engineer:
I recommend SSAS Tabular as the analytics engine. It integrates seamlessly with SQL Server on AWS RDS, leveraging my star schema and T-SQL skills for quick setup. Its in-memory processing delivers sub-second query times for aggregations (e.g., billing by region for 100M rows), ideal for Tableau dashboards. DAX enables complex calculations like year-over-year trends, supporting rich research insights. SSAS’s row-level security ensures HIPAA-compliant access to sensitive healthcare data, and it uses existing RDS infrastructure, avoiding extra costs. 
***Redshift Scenario:*** Redshift is better for petabyte-scale, unstructured data, and real-time. Its MPP architecture handles massive ad-hoc queries, but its cluster management adds complexity compared to SSAS’s simpler setup.

## Section 2: Production Issue Troubleshooting
#### 1. First three actions to troubleshoot:
**Check the Glue Job Logs and Run Status:**
- I’d hop into the AWS Glue console, look at the job run history for last night (around August 19, 2025), and grab the detailed logs from CloudWatch to spot error messages—maybe a timeout, schema mismatch, or missing S3 file. I’d also confirm the run status to see if it failed outright or just hung.

**Inspect Recent Changes and S3 Source:**
- I’d review the latest Glue script updates, IAM role tweaks, or S3 file changes (e.g., new files or corrupted data) that might’ve tripped it up. A quick peek at the S3 bucket with Athena or the console would check if the expected data for August 19 is there and intact.

**Verify Environment and Permissions:**
- I’d ensure the Glue job’s IAM role has the right permissions for S3 and Redshift, check network connectivity (like VPC settings), and confirm the Redshift cluster and S3 bucket are healthy using the AWS console. Any hiccup there could explain the blank dashboards.

#### 2. Approach to recover missing data and avoid duplication:
First, I’d query Redshift with something like ```SELECT COUNT(*) FROM billing_exceptions WHERE load_date = '2025-08-19'``` to confirm yesterday’s data is missing—pinpointing the gap.
To recover, I’d rerun the Glue job, using parameters from the CloudWatch logs (e.g., the failure timestamp) to filter the S3 batch with ```since``` and ```until```. I’d test it in a dev environment first to avoid messing up prod, especially if it was a timeout that caused the flop. If it’s a big batch, I’d keep an eye on runtime.
For duplication, I’d stage the data in a temp Redshift table with a COPY command, then run a MERGE like:
```
MERGE INTO main.billing_exceptions AS target
USING staging.billing_exceptions AS source
ON target.encounter_id = source.encounter_id
WHEN MATCHED THEN UPDATE SET target.amount = source.amount
WHEN NOT MATCHED THEN INSERT (encounter_id, amount, load_date) VALUES (source.encounter_id, source.amount, source.load_date);
```
If there’s any overlap, I’d toss in ```dropDuplicates()``` in the Glue PySpark script on ```encounter_id```. I’d log the rerun results to CloudWatch, double-check the row count with ```SELECT COUNT(*) FROM billing_exceptions WHERE load_date = '2025-08-19'```, and refresh the Tableau dashboard to make sure the "Billing Exceptions" report pops back up.

#### 3. Draft email to stakeholders:
***Subject:*** Update on Overnight Glue Job Failure - Billing Exceptions Report Impacted

Hi Team,
I wanted to give you a quick heads-up on an issue we hit this morning. The overnight AWS Glue job failed to load data into Redshift, leaving the "Billing Exceptions" report on our dashboards blank. This affects visibility into yesterday’s (August 19, 2025) billing metrics, but other reports are still running fine.

***Impact:*** This could delay spotting billing anomalies by 1-2 days if not resolved soon, potentially impacting follow-up actions. No data loss occurred—the S3 source files are intact.

***Resolution:*** I’ve already dug into the Glue job logs in CloudWatch and confirmed the failure (likely a timeout or S3 file issue). I’m rerunning the job with targeted parameters to recover the missing data, testing it in dev first to avoid hiccups. I’ll stage the data in a temp Redshift table and use a MERGE to prevent duplicates, with validation checks before refreshing the dashboards. Expect full recovery by 6:00 PM today, barring any surprises—I’ll keep you posted if it shifts.

Please let me know if you have questions or want to jump on a quick call. I’ll follow up with a status update once it’s resolved.

Best,

Sai Gowtham Yanala

Data Architect
## Section 3: Business & Team Collaboration
#### 1. Three clarifying questions:
- ***Priority and Timeline:*** What’s the priority for this request, and is there a specific deadline? Given we’re mid-sprint, would it be okay to plan this for the next sprint if it’s not urgent?
- ***Delivery Format:*** Would you prefer this as an ad-hoc Excel report or a recurring Tableau dashboard with metrics and charts? Any specific visuals or data points you’d like to see?
- ***Data Scope and Definition:*** When you say ‘weekly summaries,’ are you referring to claims submitted, created, or processed each week? Also, how far back would you like the data to go—e.g., the past month or year?

#### 2. Jira story:
***Title:*** Weekly Denied Claims Summary by Provider Group and Region

***Description:*** Business user want to see weekly summaries of denied insurance claims broken down by provider group and region, so that user can identify denial trends and address root causes effectively.

***Acceptance Criteria:***
1. The report/dashboard should display weekly aggregated counts and/or amounts of denied claims, grouped by provider group and region, for the requested date range.
2. The output should be available in the preferred format (Excel extract or Tableau dashboard) as confirmed by the business user, and must refresh weekly with updated data.

#### 3. Coordination with PM and QA:
* ***With the Project Manager (PM):***
    * I’d share the clarifying questions with the PM to confirm scope and urgency, especially since we’re mid-sprint.
    * If it’s high priority, I’d work with the PM to assess sprint capacity and possibly re-prioritize backlog items. If not urgent, I’d document it in the backlog for the next sprint.
    * I’d confirm whether this is a one-time ad-hoc request or requires a recurring production-ready solution (affects effort and timeline).
* ***With the QA Engineer:***
    * I’d collaborate with QA to define test scenarios: e.g., verifying weekly aggregation logic, correct grouping by provider group and region, and validating against sample source data.
    * I’d provide them with details on expected output format and sample datasets so they can create validation test cases.
    * I’d also confirm with QA how often regression testing needs to happen if this becomes a recurring dashboard/report.

## Section 4: CI/CD & Test-Driven Development
#### 1. CI/CD pipeline description:
**a. Executes unit tests:** The pipeline starts with a Jenkins Pipeline: CI triggered on PR creation. The CI stage includes "CI Checks" with "Unit Test (pytest)," alongside Linting, Test Coverage, and Security Vuln scans. The pytest unit tests, as shown, execute to validate the ETL job’s logic (e.g., the validate_row() function checking patient_id and encounter_date), ensuring TDD principles are followed. A green checkmark indicates successful test completion before proceeding.

**b. Deploys code to Lambda or EC2:**  After CI approval, the Jenkins Pipeline: CD handles deployment. It starts with a "Docker Container Build Process" that builds and pushes the image to ECR. The primary deployment uses "Deploy Lambda (SAM | Serverless)" via AWS SAM for the Python ETL job, optimized for scalability and assuming execution under 15 minutes. If the process exceeds 15 minutes—due to complex workflows or large data volumes—a Step Function orchestrates the steps. For even more demanding cases, like multiple instances or steps taking over 15 minutes, EC2 is leveraged for its robust compute capacity, ensuring the pipeline adapts to varying loads.

**c. Uses secure credential handling:** The pipeline integrates Secrets Manager across all environments—Dev, Stage, and Prod. Credentials for SQL Server, S3 access, or other services are securely stored and retrieved during deployment and runtime. The diagram shows Secrets Manager linked to Lambda, SQL Server, and other components, ensuring sensitive data (e.g., database passwords) is handled securely, which is critical for a healthcare data pipeline.

**d. Sends alerts on failure:** Failure across CI, CD, and the environment stages (Dev, Stage, Prod) are sent to Slack. This setup uses to notify the team in real-time, ensuring quick response to issues like a failed ETL job or deployment.

<p align="center">
  <img src="CICD%20Arch.jpg" alt="My Diagram" width="600">
</p>

#### 2. Pytest unit test for validate_row():
**a. Non-null patient_id:**
```
# src/validation.py
from datetime import datetime

def validate_row(row: dict) -> bool:
    patient_id = row.get("patient_id")
    if not patient_id or not str(patient_id).strip():
        return False
    date_str = row.get("encounter_date")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return False
    return True
 ``` 
 **b. Valid encounter_date in YYYY-MM-DD format:**
 ```
 def test_validate_row_valid():
    row = {"patient_id": "P12345", "encounter_date": "2025-08-01"}
    assert validate_row(row) is True
```    
 **c. Include one valid and one invalid test case:**
```
def test_validate_row_invalid():
    row = {"patient_id": "", "encounter_date": "08/01/2025"}
    assert validate_row(row) is False
```
**test_validate_full_code:**
```
# tests/test_validate_row.py
import pytest
from src.validation import validate_row

def test_validate_row_valid():
    row = {"patient_id": "P12345", "encounter_date": "2025-08-01"}
    assert validate_row(row) is True

def test_validate_row_invalid():
    row = {"patient_id": "", "encounter_date": "08/01/2025"}
    assert validate_row(row) is False
```
## Section 5: T-SQL Proficiency
#### 1. T-SQL query for top five providers:
```
SELECT TOP 5 WITH TIES
    p.ProviderID,
    p.Name AS ProviderName,
    COUNT(*) AS MissedAppointments
FROM Appointments a
INNER JOIN Providers p ON a.ProviderID = p.ProviderID
WHERE a.AppointmentDate >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
    AND a.AppointmentDate < CAST(GETDATE() AS DATE)
    AND a.Status = 'Missed'
    AND a.Status IS NOT NULL
    AND a.AppointmentDate IS NOT NULL
GROUP BY p.ProviderID, p.Name
ORDER BY MissedAppointments DESC;
```
#### 2. Optimization strategy:
```
CREATE NONCLUSTERED INDEX IX_Appointments_Missed
ON Appointments (Status, AppointmentDate, ProviderID);
```
The index prioritizes Status for selectivity (Missed is a small subset), followed by AppointmentDate for range queries and ProviderID for joins, minimizing table scans on SQL Server (AWS RDS). I’d coordinate with the ETL team to align partitioning with nightly data loads, minimizing overhead. Alternatively, a filtered index on Status = 'Missed' could reduce index size further:
```
CREATE NONCLUSTERED INDEX IX_Appointments_Missed_Filtered
ON Appointments (AppointmentDate, ProviderID)
WHERE Status = 'Missed';
```
#### 3. TRY...CATCH block:
Assuming an ErrorLog table (ErrorMessage, ErrorLine, ErrorProcedure, ErrorDate, ErrorSeverity):
```
BEGIN TRY
    BEGIN TRANSACTION;
    SELECT TOP 5 WITH TIES
        p.ProviderID,
        p.Name AS ProviderName,
        COUNT(*) AS MissedAppointments
    FROM Appointments a
    INNER JOIN Providers p ON a.ProviderID = p.ProviderID
    WHERE a.AppointmentDate >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
        AND a.AppointmentDate < CAST(GETDATE() AS DATE)
        AND a.Status = 'Missed'
        AND a.Status IS NOT NULL
        AND a.AppointmentDate IS NOT NULL
    GROUP BY p.ProviderID, p.Name
    ORDER BY MissedAppointments DESC;
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    INSERT INTO ErrorLog (ErrorMessage, ErrorLine, ErrorProcedure, ErrorDate, ErrorSeverity)
    VALUES (
        ERROR_MESSAGE(),
        ERROR_LINE(),
        ERROR_PROCEDURE(),
        GETDATE(),
        ERROR_SEVERITY()
    );
    THROW;
END CATCH;
```
## Section 6: AI-Augmented Development
#### 1. Two use cases:
- One use case is during the initial design of ETL pipelines— for instance, when building a Python script to process data from S3 into SQL Server, I might use AI to generate a rough draft of the code structure, including error handling or data validation logic. This saves time on boilerplate code, letting me focus on customizing it to our specific schema and performance needs, like integrating with AWS Glue for schema enforcement.
- Another use case is query optimization and troubleshooting. If I'm dealing with a slow-running T-SQL query in a large dataset, say aggregating patient encounters, I could prompt the AI to suggest indexing strategies or rewritten joins based on a description of the tables involved. It's like having a quick brainstorming partner, but I always test and refine the suggestions in our environment to ensure they align with security and compliance standards.

#### 2. AI prompt:
Explain a FULL OUTER JOIN in SQL as if you're mentoring a beginner analyst who's just starting out. Break it down step by step: what it does, how it compares to LEFT and RIGHT JOINs, and potential pitfalls like handling NULL values. Use a healthcare-themed example with two tables—one for 'Doctors' (DoctorID, Name, Specialty) and one for 'Shifts' (ShiftID, DoctorID, Date)—and show sample data, the SQL query, and what the output might look like in a table format to visualize matches and mismatches.
#### 3. Two precautions in regulated environments:
- Always anonymize or use synthetic data when feeding inputs into AI tools, to avoid exposing any protected health information (PHI) or sensitive details that could violate regulations like HIPAA.
- Conduct thorough human reviews and testing of any AI-generated outputs, such as code or query suggestions, to verify accuracy, security, and compliance before implementing them in production systems.
## Section 7: Documentation
- **Purpose**: This ETL pipeline extracts patient encounter data from S3, validates and transforms it, and loads it into a SQL Server staging table to support downstream analytics workflows, such as Tableau dashboards for executive reporting.

- **Data sources and targets**: The source is CSV flat files stored in an S3 bucket (e.g., s3://hospital-data/encounters/). The target is a SQL Server staging table (e.g., dbo.StagingEncounters) hosted on AWS RDS, designed to temporarily hold validated data before further processing.

- **Core transformations**: The pipeline reads files using pandas/boto3, validates rows (e.g., ensuring non-null patient_id and YYYY-MM-DD formatted encounter_date), converts dates to datetime objects, and batches valid rows for insertion. It also handles incremental updates by tracking timestamps or watermarks.

- **Performance considerations**: Batch inserts (e.g., 1000 rows) minimize round-trips to SQL Server. The pipeline leverages Lambda for serverless execution, scaling with demand, and uses ECR for reusable Docker images. If execution exceeds 15 minutes or involves complex workflows, Step Functions or EC2 are employed, with partitioning considered for large datasets to optimize query performance.

- **Error handling approach**: Invalid rows (e.g., missing patient_id or malformed dates) are skipped, logged to a file, and uploaded to S3. The pipeline uses try-except blocks for connection errors, with retries, and Slack alerts notify the team of failures across CI/CD and environments, ensuring quick resolution.

- **Key assumptions**: Files are CSV with consistent headers; data volume is manageable within 15 minutes for Lambda, escalating to Step Functions or EC2 if needed; SQL Server credentials are securely stored in Secrets Manager.

## Bonus Section: Python + SQL Integration Challenge
```
"""
pip install sqlalchemy pyodbc boto3
# macOS: brew install unixodbc
# Driver: ODBC Driver 18 for SQL Server
# ENV needed: MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD
"""

import os, csv, json, io
from typing import Dict, Iterable, List, Tuple

import boto3
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy import event

def validate_row(row: Dict) -> bool:
    "this function validates data"
    if not row.get("patient_id") or not str(row["patient_id"]).strip():
        return False
    d = row.get("encounter_date")
    if not d or len(d.split("-")) != 3:  # quick YYYY-MM-DD check
        return False
    return True

def stream_s3_records(bucket: str, prefix: str, filetype: str = "jsonl") -> Iterable[Dict]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            if filetype.lower() == "jsonl":
                for line in io.BytesIO(body):
                    line = line.decode("utf-8").strip()
                    if line:
                        yield json.loads(line)
            elif filetype.lower() == "csv":
                r = csv.DictReader(io.StringIO(body.decode("utf-8")))
                for row in r:
                    yield row
            else:
                raise ValueError("Use filetype='jsonl' or 'csv'.")

def make_engine() -> Engine:
    driver = "ODBC Driver 18 for SQL Server"
    server = os.environ["MSSQL_SERVER"]      # e.g. "tcp:myserver.db.windows.net,1433"
    database = os.environ["MSSQL_DATABASE"]  # e.g. "claimsdb"
    uid = os.environ.get("MSSQL_USER")
    pwd = os.environ.get("MSSQL_PASSWORD")
    odbc = (
        f"Driver={driver};Server={server};Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
        + (f"UID={uid};PWD={pwd};" if uid and pwd else "")
    )
    from urllib.parse import quote_plus
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}",
                           fast_executemany=True, pool_pre_ping=True)

    @event.listens_for(engine, "before_cursor_execute")
    def _enable_fast_executemany(conn, cursor, statement, params, context, executemany):
        if executemany and hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True
    return engine

def insert_batches(
    engine: Engine,
    rows: Iterable[Dict],
    table: str,
    cols: List[str],
    batch_size: int = 1000,
    invalid_log_path: str = "invalid_rows.csv",
) -> Tuple[int, int]:
    valid_batch, valid_count, invalid_count = [], 0, 0

    if not os.path.exists(invalid_log_path):
        with open(invalid_log_path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=cols + ["_error"]).writeheader()

    def flush():
        nonlocal valid_batch, valid_count
        if not valid_batch:
            return
        placeholders = ", ".join(f":{c}" for c in cols)
        stmt = text(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})")
        with engine.begin() as conn:  # one transaction per batch
            conn.execute(stmt, valid_batch)
        valid_count += len(valid_batch)
        valid_batch = []

    for row in rows:
        if validate_row(row):
            valid_batch.append({c: row.get(c) for c in cols})
            if len(valid_batch) >= batch_size:
                flush()
        else:
            invalid_count += 1
            with open(invalid_log_path, "a", newline="", encoding="utf-8") as f:
                out = {c: row.get(c) for c in cols}
                out["_error"] = "validation_failed"
                csv.DictWriter(f, fieldnames=cols + ["_error"]).writer.writerow(out)

    flush()
    return valid_count, invalid_count

if __name__ == "__main__":
    BUCKET = "my-ingest-bucket"
    PREFIX = "claims/2025-08-16/"   # partition or folder
    FILETYPE = "jsonl"              # or "csv"

    TABLE = "dbo.PatientEncounters"
    COLS = ["patient_id", "encounter_date", "claim_amount", "status_code"]

    engine = make_engine()
    rows = stream_s3_records(BUCKET, PREFIX, filetype=FILETYPE)
    inserted, skipped = insert_batches(engine, rows, TABLE, COLS, batch_size=2000)
    print(f"Inserted: {inserted:,} | Skipped (invalid): {skipped:,}")
```
