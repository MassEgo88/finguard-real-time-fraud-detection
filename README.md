# FinGuard — Real-Time Fraud Detection Platform

**An end-to-end streaming data engineering project using Confluent Cloud Kafka, Databricks, Apache Spark, Medallion Architecture, and automated email alerting.**

> Project status: End-to-end test execution reported successful; data reconciliation, duplicate-processing investigation, and watchlist-match validation remain in progress.

## Overview

FinGuard is a real-time fraud-detection engineering project designed to demonstrate how transaction events can move from streaming ingestion through data transformation and enrichment to alert generation.

The platform uses Confluent Cloud Kafka as the transaction-event source and Databricks for streaming processing. A Medallion Architecture organizes the data into Bronze, Silver, and Gold layers. Customer reference data and fraud-watchlist information support enrichment and rule-based suspicious-activity detection, while Gmail SMTP provides an email-notification mechanism.

The project focuses on more than moving data. It explores data quality, orchestration, secret management, event processing, operational troubleshooting, and the reliability controls needed when a streaming system triggers external actions.

**Important:** A suspicious-activity indicator is not proof of fraud. Detection rules and thresholds must be explicitly defined, validated, and documented.

## Business problem

Financial transaction systems can produce events continuously. To support timely investigation, engineering teams need pipelines that can:

- Ingest transaction events reliably.
- Transform and validate incoming data.
- Enrich transactions with customer and watchlist information.
- Identify events matching configurable risk rules.
- Produce traceable alert records.
- Notify appropriate recipients.
- Detect duplicate processing and operational failures.

FinGuard is a practical project for exploring these requirements through a modern streaming data platform.

## Architecture

<!-- Replace this placeholder with a real architecture diagram. -->
![FinGuard architecture diagram](docs/images/architecture.png)

### High-level data flow

```text
Confluent Cloud Kafka
        |
        v
Transaction Producer / Event Source
        |
        v
Databricks Streaming Ingestion
        |
        v
Bronze Layer — Raw Transactions
        |
        v
Silver Layer — Validated Transactions
        |
        +----------------------------+
        |                            |
        v                            v
Silver Customers              Silver Watchlist
        |                            |
        +-------------+--------------+
                      |
                      v
           Enrichment and Rule Checks
                      |
                      v
            Gold Alert / Analytics Data
                      |
                      v
            Email Alerting Component
                      |
                      v
             Monitoring and Logging
```

The diagram represents the intended logical architecture. Actual source names, dependencies, and deployment boundaries should be aligned with the exported Databricks implementation.

## Technology stack

| Technology | Purpose |
|---|---|
| Confluent Cloud Kafka | Streaming transaction-event ingestion |
| Databricks | Data engineering and pipeline execution |
| Apache Spark | Distributed data processing |
| Lakeflow Spark Declarative Pipelines | Declarative pipeline development and execution |
| Medallion Architecture | Separation of raw, refined, and curated data |
| Python | Ingestion, transformation, validation, and alert logic |
| Gmail SMTP | Email notification integration |
| Databricks secret management | Secure handling of application credentials |
| Git and GitHub | Source control and project documentation |

Only list technologies and components that are present in the final, exported implementation.

## Medallion Architecture

### Bronze — Raw ingestion

The Bronze layer ingests transaction events from Kafka and stores source data for downstream processing. Customer and watchlist reference data follow their respective ingestion paths.

Objectives include preserving traceability, supporting investigation of source events, and separating ingestion from business transformations.

### Silver — Refined data

The Silver layer applies transformations and data-quality checks to prepare transaction, customer, and watchlist data for downstream use.

The current project includes Silver transaction and customer tables. Required-field expectations have been configured for fields including transaction ID, customer ID, and card number.

Key validation concerns include schema consistency, missing or invalid identifiers, duplicate records, and the preservation of source event timestamps.

### Gold — Curated outputs and alerts

The Gold layer is intended to expose business-oriented outputs for alerting and analytics.

Planned or implemented outputs should be documented according to the actual repository, including any tables for:

- High-value transaction indicators
- Watchlist matches
- Suspicious-activity alerts
- Alert delivery status
- Operational metrics

Document each table's schema and status. Do not describe a planned table as implemented until it exists and has been validated.

## Streaming and alerting design

The intended processing sequence is:

1. Produce transaction events.
2. Ingest events through Kafka.
3. Process data in Databricks.
4. Validate and transform records through the Medallion layers.
5. Enrich transactions with customer and watchlist reference data.
6. Apply configurable business rules.
7. Persist alert records and their processing status.
8. Deliver email notifications through a separately testable alert component.
9. Monitor failures, retries, and duplicate-processing behaviour.

### Alert reliability considerations

Sending an email is an external side effect. A streaming task may retry or process an event again, so the design must account for duplicate alerts, partial failures, and delivery tracking.

Recommended controls include:

- A stable alert identifier or idempotency key
- Explicit alert states, such as pending, sent, and failed
- Bounded retries and error logging
- A clear separation between detection and email delivery
- Monitoring for undelivered alerts
- Secure secret retrieval at runtime

The implementation should use an execution model supported by the deployed Databricks environment. Email delivery should not be assumed to be safe simply because the upstream pipeline completed.

## Execution evidence and validation

A reported orchestration test completed with all three tasks marked successful:

- Kafka producer
- Databricks streaming pipeline
- Email-alert task

The reported end-to-end run duration was approximately **5 minutes and 35 seconds**. The producer was configured for a batch intended to contain **1,000 transactions**.

These are reported test-run observations, not independently verified performance benchmarks.

### Outstanding data-quality investigation

Post-run checks showed more new Silver rows than expected for the producer batch, as well as a watchlist-match rate of approximately 33%, compared with a previously observed historical rate of around 6%. Additional events appeared to arrive after the reported run had completed.

These observations require further investigation. They do not, by themselves, establish whether the cause was duplicate run submission, a producer that remained active, streaming checkpoint behaviour, join cardinality, or another issue.

Before treating the pipeline as validated, the following checks are required:

- Reconcile produced events against Kafka offsets and distinct transaction IDs.
- Compare source, Bronze, Silver, and Gold counts over the same time window.
- Identify duplicate transaction IDs and determine their origin.
- Check whether more than one producer or pipeline run was active.
- Compare event time with processing time.
- Validate the cardinality of customer and watchlist joins.
- Investigate the watchlist-match rate against a known baseline.
- Confirm that alert records and emails are not duplicated on retries.
- Preserve the relevant pipeline logs and execution details.

Successful task status is useful execution evidence, but it does not prove exactly-once processing, zero duplicates, or production readiness.

## Security

Credentials must not be committed to this repository.

For local development, use placeholder configuration and environment variables where appropriate. In Databricks, retrieve secrets from an approved secret scope or other supported secret-management service.

Never commit:

- Gmail App Passwords
- Kafka API keys or secrets
- Databricks access tokens
- Private keys
- Customer personal information
- Unredacted transaction records

Review notebooks, configuration files, screenshots, and Git history before publishing. If a credential has been exposed, revoke or rotate it before continuing.

## Repository structure

```text
.
├── README.md
├── docs/
│   ├── architecture.md
│   ├── data-quality-and-validation.md
│   ├── setup-guide.md
│   └── images/
├── src/
│   ├── ingestion/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── alerts/
├── tests/
└── config/
```

This is the proposed organization. Update it to match the files actually committed to GitHub.

## Setup overview

Detailed instructions belong in `docs/setup-guide.md`.

At a high level:

1. Configure access to a Databricks workspace.
2. Configure Confluent Cloud Kafka access.
3. Create or verify the required Databricks secret scope and keys.
4. Configure the source tables and pipeline dependencies.
5. Deploy the relevant notebooks and pipeline definitions.
6. Configure the email-alert component and test it with a controlled recipient.
7. Run a small test batch.
8. Validate record counts, uniqueness, join results, and alert delivery.
9. Review logs before scaling the test.

Use placeholders for configuration examples. Never include live credentials in setup instructions.

## Skills demonstrated

This project demonstrates practical work in:

- Streaming data ingestion
- Apache Kafka and event-driven architecture
- Databricks data engineering
- Spark-based data transformations
- Medallion Architecture
- Data-quality expectations and validation
- Customer and watchlist enrichment
- Pipeline orchestration
- Email-alert integration
- Secret management
- Troubleshooting and operational observability
- Technical documentation

## Future improvements

The next engineering priorities are:

1. Resolve the unexpected transaction-count growth.
2. Validate event uniqueness and checkpoint behaviour.
3. Investigate the elevated watchlist-match rate.
4. Confirm that only one intended producer and pipeline execution is active.
5. Implement and test alert idempotency and delivery-state tracking.
6. Add automated tests for data quality, joins, and alert logic.
7. Improve structured logging, retries, and failure monitoring.
8. Document deployment, recovery, and configuration procedures.
9. Add validated architecture and pipeline screenshots.

## About this project

FinGuard is a hands-on data engineering project focused on building and validating a streaming data pipeline for rule-based fraud indicators and automated notifications.

The emphasis is on end-to-end engineering: ingestion, transformation, enrichment, data quality, orchestration, security, and operational reliability.

**Project repository:** Add the GitHub URL after publishing.

**Portfolio presentation:** Add the Lovable website URL after publishing.

**Professional profile:** Add the relevant LinkedIn profile link.

---

*Documentation should be updated as the implementation evolves. Reported test results, planned capabilities, and verified production behaviour must remain clearly distinguished.*
