# Case Study 3: Strategic Data System for Trusted Partner Eligibility & Identity Verification


## Summary

This document outlines a zero trust architecture to securely ingest, curate, and serve partner eligibility data for identity verification. It starts with a data clean room design at the perimeter, which handles diverse data formats and immediately tokenizes PII. From there, the document details the scalable and reliable data modeling and curation pipelines needed to build a definitive source of truth. It concludes with a high level look at the downstream identity verification service that relies on this curated data.


## Assumptions
While the document supports the ingestion of multiple formats of the partner data, it also assumes there is some coordinated configuration work (like defining raw schemas, expected file formats etc.) that is needed to set up before they start sending the data. 
The design assumes there are no resource constraints when building compliance frame works like data cleanroom, encryption services, etc. 
The proposed design heavily relies on some out of the box cloud services. I can talk about tangential topics like non-cloud or on-prem services for some of the components if needed.
The tokenization component assumes the PII schema is uniform for all partners in the base design. I added caveats when there is a divergence from the original accepted schema. 
The service SLAs are not defined in the problem statement, so these are the assumptions I made 
Identity verification service
Latency < 50 ms
Availability - 99.99%
Data freshness - < 5 mins on an average 
Data pipelines
Latency < 1 minute
Kafka lag - 10000 messages 
Data completeness 
100% completeness requirement 
99% completeness on logs or other non critical data 
Data quality
No nulls for primary keys 
< 5% nulls for lookup keys like phone number 


## High Level design

<img width="640" height="942" alt="image" src="https://github.com/user-attachments/assets/aaa16ff1-ca47-4262-86ee-d51198c32213" />


      
The high level design consists of different layers to this architecture with based on their functions 

The core architectural components consists of 
Secure ingestion layer 
Data Processing layer 
Identity Verification service 

	






## Detailed component design 

***Outline your strategic vision for integrating and managing this critical eligibility data. Your response should detail how you would establish clear data quality standards and PII governance requirements, including assessing data quality and identifying appropriate privacy controls (e.g., anonymization, pseudonymization) and compliance measures.***


My idea of managing the architecture with critical eligibility data is based on assuming zero trust and strongly defined data contracts. The identity verification API depends on the data processed by this system, so the architecture should not assume the partner data is clean and should be resilient enough for handling cases where there are formatting issues, duplicate data records etc. 


From my point of view, the best way to achieve this is to decouple the raw data governance from the data processing. This is to ensure the PII data is deterministically tokenized and bad data is quarantined before it is ingested into the data lake (or warehouse). 


There are 3 major components as a part of this data governance layer

- **PII Governance**: The partner data is assumed to be under strict regulatory and compliance frameworks like HIPAA etc. so one of the goals of this governance layer is to reduce the blast radius of the private data. The layer consists of 4 key zones based on their functions  
  - ***Isolated partner data processing zone***: All data received from the partners land in this private data cleanroom and this has a restricted manual read access. Only transient compute jobs to tokenize this data can access this data 
  - ***Tokenization***: The encryption service in this zone deterministically hash the PII data fields getting the data ready to be ingested into the data lake. The raw PII and the corresponding tokens are stored in the highly secure vault by the encryption engine. 
Compliance zone: Occasionally we may need to discard the data for a user, so this zone handles the deletion requests.
  - ***Data Anonymization***: This zone handles the service to anonymize the data further after tokenizing, effectively making it ready for any downstream ML or analytics consumers. This is an optional service
  - Why choose encrypted tokenization instead of hashing? 
	  - standard hashing for predictable PII data like SSN is not secure because of the uniform 
	nature of the number format. So key based encryption adds better security around PII data. 
	  - operational reason: we may need raw PII data at the final API layer. For example, when loading the user profile in the app, we may want to display their email, and last 4 digits of SSN. so we need reversible tokenization. Other regulatory rights like the user’s right to know what relevant data we possess regarding them.
  - Are there alternatives for tokenization?
    - We can explore 2 tier alias mapping pattern, where the data processing layer is isolated from the encryption key rotations. The token mappings in this case reside in the highly secure storage vault and the same surrogate key token is used in the downstream data layer, irrespective of the encrypted key version. The advantages of this is zero downtime during key rotation, data continuity in the analytical layer, and overall blast radius containment for key rotations. 
  
- **Data quality standards**: To make sure the downstream data processing layer gets cleaner raw data, we can enforce some data quality standards for them. These include mainly

  - ***Data formats***: enforce standard formats and data types for specific data like SSN should be xxx-xx-xxxx or xxxxxxxxx. Date or timestamp fields should be in a consistent format, etc.
  - ***Completeness***: There should not be any missing data for primary keys or identifiers. An event stream or bulk historical data having a null PK value is set to fail fast 
  - ***Data consistency***: Some timestamp fields need to follow chronological order, records to be deleted have to be flagged is_deleted etc.
- **Data Quality assessment**: The strategy has to be resilient enough to handle bad data, sometimes we need to fail fast, and some times we can quarantine the data and let the pipelines continue 
  - ***Partner schema registry***: The design assumes during the integration phase, the schema is added to the central schema registry, and all incoming data has to confirm to this strongly typed schema. Handling structural mismatches can be configured based on the individual fields missing (or added) 
  - ***Data profiling and monitoring***: At the governance layer, we don’t need to have a lot of anomaly detections other than the volume of data that can incur a lot of kafka backlogs etc. 

The below diagram shows the high level components in the governance layer 


<img width="800" height="874" alt="image" src="https://github.com/user-attachments/assets/69496b8e-6927-4a82-a5e2-84285327c1ee" />


<br>



</br>


<br>



</br>





***Propose a comprehensive data integration strategy that robustly supports both the initial bulk ingestion and ongoing change data capture (CDC) for incremental updates, defining key performance and freshness requirements.***


Both the bulk and CDC event streams are processed by the same tokenizaztion service in the clean room. The data is then processed into the data lake by an etl process like spark jobs etc. 


The integration strategy includes 4 main layers:

- **Bulk ingestion layer (batch)**
  Client data is dumped into the restricted storage buckets. If they chose to encrypt the data files, the bulk ingestion service should handle decrypting these files before running them through the data assessment and tokenization. The tokenization for these initial cold start files run on a different cluster. These dedicated transient containers are added so as to protect the streaming layer from competing for resources.
  
  Additionally this process not only supports the initial bulk loads but also micro batches where clients dump data in batches instead of real time CDC streams. For these scenarios, we need to setup cluster pool for hourly or daily processing jobs depending on the load.

- **Streaming Layer for CDC:**
  Partners capable of sending events are provided with the kafka endpoint to asynchronously consume the streaming data. The payload must include updated_at (or similar) timestamp to make sure we process these payloads in chronological order when consuming the stream.
  
  Depending on the volume of the data, streaming still supports micro batches provided the partner is able to send these events to a kafka endpoint. This is a design choice based on the specific requirements.

- **Performance and freshness requiremenets:**
  Freshness on the streaming data: The SLA for data freshness latency assumption made for the Identity verification service is < 5 mins, so all the streaming data needs to be tokenized, ingested, resolved in the data processing layer and published to the API database in under 5 minutes
  
  Batch ingestion latency: Depending on the historical backfill data dumps or hourly batch processing, the latency varies from ~1 hour to 24 hours
  
  Backpressure reliability: To make sure no partner data is lost, the kafka buffer will be sized to around 48 hours making sure the streaming is restored once the maintenance or incident that broke the pipeline is resolved.

- **Coordinate streaming and batch layers:**
  This is an optional layer that supports data backfills via batch processing. For this the standard watermark cutover strategy can be used to resolve the data and prevent from double processing.


The above strategy relies on the unified datalake that acts as a central repository for the partner’s eligibility data. Both streaming and batch pipelines follow the same logic and use the same codebase. We can follow lambda architecture to separate the batch and streaming pipelines (for CDC updates). The unified processing reduces the additional checks we need to perform to make sure the identity data is fresh and up to date. Assuming the frequency of requiring batch processing to be really low, having a single ingestion source code is a lot easier to maintain in the long run. 



























***Describe your plan for an automated data cleansing and curation process that ensures consistency, accuracy, and continuous compliance, along with a high-level design for the identity verification system that leverages this cleansed and curated data as its source of truth, articulating its availability and reliability requirements.*** 

Once the data passes the clean room processing layer, it lands in the integration layer where the pipeline jobs transform the data into the resolved and curated data set. The pipeline consists of a data model and multiple data transformation stages. The data model in the integration layer has 3 main zones, raw data zone, standardized data zone, and curated data zone. The description of the pipeline is below



**1. Automated Data Cleansing and Curation Pipeline**
The data pipeline consists of a data model and multiple data transformation stages. The entire pipeline runs on a distributed framework like Spark. 
Data processing stages
  - ***Ingestion and tokenization***: 
    - The ingestion service runs transient workers that consume kafka streams and validates the schema against the registry. Based on the configurable ingestion strategy, the invalid records are dropped or the pipeline is blocked. It calls the tokenization service once done
    - The tokenization engine calls the key encryption service that manages the encryption keys, and replaces the PII with the encrypted token 
    - Both these tasks can be done in a single spark streaming job, and since there is no data shuffle involved, it is considered a map only operation without any reduce step. This can be scaled for bulk data loads without any issue by adding more compute. 
    - Once the tokenization is done, the job writes data to the raw data layer. 
  - ***Standardizaed layer***: 
    - The job to transform raw to standard layer transforms the data in a simple way. There is no business logic involved here, so the purpose of standardizing the raw data is to cast fields to appropriate data types, enforce logic for chronological fields like dates, normalizing casing for strings etc. 
    - The job writes invalid records to a dead  letter queue for later processing or debugging. Again, the process to control what to do with the the invalid data can be done via configuration 
    - The configuration also defines the load strategies for various payloads. For example, user engagement can be incremental fact table that we get from the stream, but for user dimension updates we may get an hourly file which is a full data set. We can configure the load strategy by the table type in the yaml file for each partner separately. 
  - ***Curated layer***: 
    - This layer has a curated dataset where the data is clean and has the golden record. There are no duplicates in the data and 
    - The job that processes the standarized data and resolves the data is where the business logic resides. This job is probably the most computationally expensive since we need to resolve the data against the existing dataset. 
    - To make sure we process and use the latest data point from various partners, we need to maintain the state via checkpoint. This can be configurable depending on the partner data’s priority or timestamp of the received record, so the watermark is need to make sure we don’t accidentally use the stale data. 
    - Once the data is resolved by the job, the job writes the data using the UPSERT operation 
  - ***Pipeline resiliency:***
    - Checkpointing and Write-Ahead Logs (WAL): As the Spark job processes data, it writes its exact position (the Kafka offset or S3 file marker) to a checkpoint directory. If the compute cluster suddenly dies, a new one spins up, reads the checkpoint, and resumes from where it left off. No data is lost, and no data is processed twice.
    - Idempotency: Because we use INSERT operation based on the unique token + partner_id, our processing is idempotent. If a network glitch causes the engine to process the same 5 minute micro batch twice, the database state remains perfectly correct.
  - ***Orchestration and scaling*** 
    -  An orchestration tool like Apache Airflow schedules the micro-batches, triggers the data quality circuit breakers between layers, and alerts relevant engineers if the DLQ spikes
    - Auto scaling: We configure the compute clusters with aggressive auto-scaling policies tied to consumer lag. If the Kafka topic suddenly backs up with 5 million CDC events, the processing cluster automatically provisions more worker nodes to chew through the backlog, then scales back down to save money.

**2. Identity Verification System**
Operational Data Store: A highly scaled and low latency NoSQL database (e.g., DynamoDB). The analytical curated data layer syncs resolved identities here via CDC, isolating the fast read application from the heavy compute data lake.
- ***API Gateway***: The entry point for client applications where it receives user inputs (name, SSN etc) and orchestrates the verification flow
- ***Rela time tokenization module***: Hashes incoming user inputs on the fly using the exact same KMS logic as the ingestion pipeline. This module uses the same tokenization API to encrypt the incoming data
- ***Lookup API***: Performs <50ms point lookups against the data store using the tokenized string. It returns a definitive eligibility status without the serving layer ever persisting raw PII.

**3. Availability and Reliability Requirements**
- *Availability (99.99%)*: Both the Identity Verification API and the data store are deployed in an active multi region architecture to ensure uninterrupted service during regional cloud outages
- *Data Freshness (5 minutes SLA)*: End-to-end pipeline latency guarantees that streaming partner updates are processed, resolved in the Gold layer, and queryable in the ODS within 5 minutes.
- *System Reliability (No data loss)*: Ingestion endpoints utilize partitioned kafka buffers with 7 day retention. If downstream processing fails, data queues safely and auto-resumes once compute is restored, preventing dropped records.


<br>
</br>

<br>
</br>



***DDL for standardized layer***
 

```sql
-- The MERGE statement writes directly to the Delta Transaction Log (_delta_log).
-- This guarantees ACID compliance even if multiple partners update concurrently.

MERGE INTO prod_catalog.idv_gold.resolved_identities AS target
USING (
    -- STAGE 1: STATELESS CLEANSING
    WITH Cleaned_Incoming_Events AS (
        SELECT 
            tokenized_ssn,
            partner_id,
            -- STANDARDIZATION: Force upper case, remove special characters natively
            REGEXP_REPLACE(UPPER(TRIM(first_name)), '[^A-Z]', '') AS clean_first_name,
            REGEXP_REPLACE(UPPER(TRIM(last_name)), '[^A-Z]', '') AS clean_last_name,
            
            -- DATABRICKS SPECIFIC: try_cast handles malformed dates by returning NULL 
            -- instead of failing the entire cluster job.
            CASE 
                WHEN try_cast(date_of_birth AS DATE) > CURRENT_DATE() THEN NULL 
                WHEN try_cast(date_of_birth AS DATE) < DATE '1900-01-01' THEN NULL
                ELSE try_cast(date_of_birth AS DATE)
            END AS clean_dob,
            
            eligibility_status,
            event_timestamp
        FROM prod_catalog.idv_silver.eligibility_events
        -- Read optimization: Only scan data that arrived since the last watermark
        WHERE ingested_at > (SELECT MAX(last_updated_at) FROM prod_catalog.idv_gold.resolved_identities)
    ),
    
    -- STAGE 2: IN-BATCH DEDUPLICATION
    Deduplicated_Survivors AS (
        -- RESOLUTION: If Partner A and B both update the same token in this batch, 
        -- QUALIFY filters out the older duplicate before it hits the MERGE engine.
        SELECT *
        FROM Cleaned_Incoming_Events
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY tokenized_ssn 
            ORDER BY event_timestamp DESC
        ) = 1
    )
    
    SELECT * FROM Deduplicated_Survivors
) AS source
ON target.tokenized_ssn = source.tokenized_ssn

-- STAGE 3: STATEFUL UPSERT LOGIC
-- Update existing users ONLY if the incoming event is chronologically newer.
WHEN MATCHED AND source.event_timestamp > target.last_updated_at THEN 
    UPDATE SET 
        -- COALESCE ensures a partial update doesn't overwrite known good data with NULLs
        target.best_first_name = COALESCE(source.clean_first_name, target.best_first_name),
        target.best_last_name = COALESCE(source.clean_last_name, target.best_last_name),
        target.confirmed_dob = COALESCE(source.clean_dob, target.confirmed_dob),
        
        target.current_eligibility = source.eligibility_status,
        target.last_updating_partner_id = source.partner_id,
        target.last_updated_at = source.event_timestamp

WHEN NOT MATCHED THEN 
    INSERT (tokenized_ssn, best_first_name, best_last_name, confirmed_dob, current_eligibility, last_updating_partner_id, last_updated_at)
    VALUES (source.tokenized_ssn, source.clean_first_name, source.clean_last_name, source.clean_dob, source.eligibility_status, source.partner_id, source.event_timestamp);
```
<br>
</br>

*** Data processing pipeline sample code***

```python
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ==========================================
# 1. Stateless Cleansing Module
# ==========================================
def clean_eligibility_formats(df: DataFrame) -> DataFrame:
    """Standardizes strings and nullifies invalid date formats."""
    return df.withColumn(
        "clean_first_name", F.regexp_replace(F.upper(F.trim(F.col("first_name"))), "[^A-Z]", "")
    ).withColumn(
        "clean_last_name", F.regexp_replace(F.upper(F.trim(F.col("last_name"))), "[^A-Z]", "")
    ).withColumn(
        "parsed_dob", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
    ).withColumn(
        "clean_dob", 
        F.when(F.col("parsed_dob") > F.current_date(), None)
        .when(F.col("parsed_dob") < F.to_date(F.lit("1900-01-01"), "yyyy-MM-dd"), None)
        .otherwise(F.col("parsed_dob"))
    )

# ==========================================
# 2. In-Batch Resolution Module
# ==========================================
def deduplicate_microbatch(df: DataFrame) -> DataFrame:
    """Resolves duplicate updates for the same token within a single batch."""
    window_spec = Window.partitionBy("tokenized_ssn").orderBy(F.col("event_timestamp").desc())
    
    return df.withColumn("rn", F.row_number().over(window_spec)) \
             .filter(F.col("rn") == 1) \
             .drop("rn", "parsed_dob")

# ==========================================
# 3. Stateful Delta Merge Module
# ==========================================
def upsert_to_curated_layer(source_df: DataFrame, spark_session) -> None:
    """Executes the ACID Merge against the Curated Delta Table."""
    target_table = DeltaTable.forName(spark_session, "prod_catalog.idv_curated.resolved_identities")
    
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition="target.tokenized_ssn = source.tokenized_ssn"
    ).whenMatchedUpdate(
        condition="source.event_timestamp > target.last_updated_at",
        set={
            "best_first_name": F.coalesce(F.col("source.clean_first_name"), F.col("target.best_first_name")),
            "best_last_name": F.coalesce(F.col("source.clean_last_name"), F.col("target.best_last_name")),
            "confirmed_dob": F.coalesce(F.col("source.clean_dob"), F.col("target.confirmed_dob")),
            "current_eligibility": F.col("source.eligibility_status"),
            "last_updating_partner_id": F.col("source.partner_id"),
            "last_updated_at": F.col("source.event_timestamp")
        }
    ).whenNotMatchedInsert(
        values={
            "tokenized_ssn": F.col("source.tokenized_ssn"),
            "best_first_name": F.col("source.clean_first_name"),
            "best_last_name": F.col("source.clean_last_name"),
            "confirmed_dob": F.col("source.clean_dob"),
            "current_eligibility": F.col("source.eligibility_status"),
            "last_updating_partner_id": F.col("source.partner_id"),
            "last_updated_at": F.col("source.event_timestamp")
        }
    ).execute()

# ==========================================
# Main Orchestrator (Used by Structured Streaming)
# ==========================================
def process_eligibility_microbatch(df: DataFrame, spark_session) -> None:
    """
    Main entry point for the foreachBatch streaming sink.
    Orchestrates the pipeline stages cleanly.
    """
    # 1. clean
    cleaned_df = clean_eligibility_formats(df)
    
    # 2. dedupe
    deduped_df = deduplicate_microbatch(cleaned_df)
    
    # 3. merge
    upsert_to_curated_layer(deduped_df, spark_session)
```


