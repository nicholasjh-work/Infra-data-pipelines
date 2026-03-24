-- DDL for the RAW and ANALYTICS schemas
--
-- The RAW schema stores data as produced by the synthetic generator.
-- Each table has clustering keys on member_id and date columns to
-- optimize pruning for common query patterns.

CREATE SCHEMA IF NOT EXISTS RAW;

CREATE OR REPLACE TABLE RAW.MEMBERS (
    MEMBER_ID             INTEGER,
    SIGNUP_DATE           DATE,
    PLAN_TYPE             STRING,
    AGE_GROUP             STRING,
    GENDER                STRING,
    ACQUISITION_CHANNEL   STRING,
    REGION                STRING
) CLUSTER BY (MEMBER_ID);

CREATE OR REPLACE TABLE RAW.DAILY_METRICS (
    MEMBER_ID             INTEGER,
    METRIC_DATE           DATE,
    HRV                   FLOAT,
    RESTING_HEART_RATE    FLOAT,
    SLEEP_HOURS           FLOAT,
    SLEEP_QUALITY         FLOAT,
    STRAIN                FLOAT,
    RECOVERY              FLOAT,
    CALORIES              FLOAT
) CLUSTER BY (METRIC_DATE, MEMBER_ID);

CREATE OR REPLACE TABLE RAW.FEATURE_EVENTS (
    MEMBER_ID             INTEGER,
    EVENT_DATE            DATE,
    FEATURE               STRING,
    EVENT_NAME            STRING
) CLUSTER BY (EVENT_DATE, MEMBER_ID);

CREATE OR REPLACE TABLE RAW.SESSIONS (
    MEMBER_ID             INTEGER,
    SESSION_START         TIMESTAMP,
    SESSION_END           TIMESTAMP,
    DEVICE_TYPE           STRING,
    OS_VERSION            STRING,
    LOCATION              STRING
) CLUSTER BY (SESSION_START, MEMBER_ID);

CREATE OR REPLACE TABLE RAW.EXPERIMENTS (
    EXPERIMENT_ID         INTEGER,
    EXPERIMENT_NAME       STRING,
    START_DATE            DATE,
    END_DATE              DATE,
    DESCRIPTION           STRING
);

CREATE OR REPLACE TABLE RAW.EXPERIMENT_ASSIGNMENTS (
    MEMBER_ID             INTEGER,
    EXPERIMENT_ID         INTEGER,
    VARIANT               STRING,
    ASSIGNED_DATE         DATE
) CLUSTER BY (EXPERIMENT_ID, MEMBER_ID);

CREATE OR REPLACE TABLE RAW.SUBSCRIPTIONS (
    MEMBER_ID             INTEGER,
    PLAN_TYPE             STRING,
    START_DATE            DATE,
    END_DATE              DATE,
    AUTO_RENEW            BOOLEAN
) CLUSTER BY (PLAN_TYPE, MEMBER_ID);

-- The ANALYTICS schema is populated by downstream dbt models.
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
