-- Databricks notebook source
CREATE TABLE IF NOT EXISTS adventure_works_catalog.governance.data_lineage (
  RunID STRING,
  ProcessName STRING,
  Layer STRING,
  TableSaved STRING,
  StartDate TIMESTAMP,
  EndDate TIMESTAMP,
  QtyRead BIGINT,
  QtyWritten BIGINT,
  Status STRING,
  ErrorMsg STRING
) USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS adventure_works_catalog.governance.data_quality (
  RunID STRING,
  ProcessName STRING,
  TableName STRING,
  ColumnName STRING,
  Layer STRING,
  CheckType STRING,
  TotalRows LONG,
  FailedRows LONG, 
  Status STRING,
  CheckDate TIMESTAMP
) USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS adventure_works_catalog.governance.ADF_logs (
    ADFRunID STRING,          
    PipelineName STRING,       
    TriggerName STRING,        
    TriggerTime TIMESTAMP,     
    StartTime TIMESTAMP,       
    EndTime TIMESTAMP,
    Status STRING,             
    ErrorMessage STRING       
)
USING DELTA;
