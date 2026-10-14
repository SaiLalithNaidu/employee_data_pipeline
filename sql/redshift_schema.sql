-- =========================================================
-- Amazon Redshift Serverless Schema: Employee Data Pipeline
-- Database: dev | Workgroup: employee-data-workgroup
-- =========================================================

-- 1. Main Production Table
CREATE TABLE IF NOT EXISTS public.employees (
    employee_id          INTEGER NOT NULL,
    employee_name        VARCHAR(255),
    email                VARCHAR(255),
    department           VARCHAR(100),
    salary               DOUBLE PRECISION,
    joining_date         DATE,
    team                 VARCHAR(100),
    location             VARCHAR(100),
    status               VARCHAR(50),
    processed_timestamp  TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (employee_id)
)
DISTSTYLE AUTO
SORTKEY (joining_date, department);

-- 2. Staging Table (For safe atomic loads)
CREATE TABLE IF NOT EXISTS public.employees_staging (
    LIKE public.employees
);

-- =========================================================
-- Standard COPY Command (Load from Processed S3 Parquet)
-- =========================================================
-- COPY public.employees_staging
-- FROM 's3://employee-data-platform-sai-2026/processed-bucket/employees/'
-- IAM_ROLE 'arn:aws:iam::682595228520:role/RedshiftEmployeeDataRole'
-- FORMAT AS PARQUET;

-- =========================================================
-- Safe Atomic Swap Transaction
-- =========================================================
-- BEGIN TRANSACTION;
-- TRUNCATE TABLE public.employees;
-- INSERT INTO public.employees SELECT * FROM public.employees_staging;
-- COMMIT;

-- =========================================================
-- Quality Validation Queries
-- =========================================================
-- 1. Total record count:
-- SELECT COUNT(*) AS total_count FROM public.employees;

-- 2. Data quality checks (null/invalid audit):
-- SELECT 
--     COUNT(*) AS total_count,
--     COUNT(CASE WHEN employee_id IS NULL THEN 1 END) AS missing_ids,
--     COUNT(CASE WHEN email IS NULL THEN 1 END) AS missing_emails,
--     COUNT(CASE WHEN salary IS NULL OR salary <= 0 THEN 1 END) AS invalid_salaries
-- FROM public.employees;
