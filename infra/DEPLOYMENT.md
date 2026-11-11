# AWS deployment (prepared, NOT yet deployed)

Status: the CloudFormation template passes `cfn-lint`, the Glue package builds, and
the CloudWatch publishing code is tested against a mocked AWS (moto). **Nothing has
been created in AWS.** Deploying needs an AWS account, credentials and the owner's
approval because Glue is not free-tier.

## Estimated cost (us-east-1, list prices, verify before deploying)

| Item | Usage per run | Approx. cost |
|---|---|---|
| Glue job, G.1X x 2 workers (2 DPU) at $0.44 per DPU-hour | ~5 min, 1-min minimum | ~$0.08 per run |
| S3 storage | ~0.3 GB (raw gz + Parquet) | < $0.01 per month |
| Athena at $5 per TB scanned | KB-MB per query (partition pruning) | < $0.01 |
| CloudWatch custom metrics | 6 metrics (free tier includes 10) | $0 |
| CloudWatch Logs | a few MB | $0 (5 GB free) |
| Glue crawler (optional, 0.44/DPU-h, 10-min min.) | per crawl | ~$0.07 |

A handful of test runs costs well under $1. The template adds an AWS Budget
($5/month, emails at 80% actual and 100% forecast), a 30-minute Glue timeout and a
1 GB-per-query Athena cutoff as guardrails.

## Steps

```bash
# 0. One-time: install AWS CLI v2 and run `aws configure` with an IAM user that is
#    NOT the root account. Never commit credentials.

# 1. Create the stack (S3, IAM role, Glue job/catalog/crawler, Athena workgroup,
#    SNS + alarms, budget). Confirm the SNS subscription email afterwards.
aws cloudformation deploy --stack-name sensor-lake \
  --template-file infra/cloudformation.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides AlertEmail=<your-email>

BUCKET=$(aws cloudformation describe-stacks --stack-name sensor-lake \
  --query "Stacks[0].Outputs[?OutputKey=='LakeBucketName'].OutputValue" --output text)

# 2. Upload code and raw data (raw zone = landing area, never edited)
python infra/package_for_glue.py
aws s3 cp build/sensor_lake.zip s3://$BUCKET/code/sensor_lake.zip
aws s3 cp glue/glue_job.py      s3://$BUCKET/code/glue_job.py
aws s3 sync lake/raw            s3://$BUCKET/sensor-lake/raw

# 3. Run the job and watch it (logs: CloudWatch > /aws-glue/jobs/output)
aws glue start-job-run --job-name sensor-lake-pipeline
aws glue get-job-runs  --job-name sensor-lake-pipeline --max-items 1

# 4. Make the curated tables queryable: either run the crawler ...
aws glue start-crawler --name sensor-lake-curated-crawler
#    ... or run athena/01_create_tables.sql in the Athena console (workgroup
#    sensor-lake-wg) after replacing ${BUCKET}. Then run athena/queries/*.sql.

# 5. Tear down when finished (empty the bucket first; it is versioned)
aws s3 rm s3://$BUCKET --recursive
aws cloudformation delete-stack --stack-name sensor-lake
```

## What runs where

| Concern | Local | AWS |
|---|---|---|
| Storage | `./lake/` folders | S3 bucket, same `raw/clean/curated` prefixes |
| Compute | PySpark `local[*]` | Glue 5.0 (Spark 3.5) job, same `sensor_lake` code |
| Catalog / SQL | DuckDB views (`local_sql.py`) | Glue Data Catalog + Athena |
| Logs | stdout | CloudWatch Logs (automatic for Glue) |
| Metrics / alerting | logged only | CloudWatch metrics, alarm, EventBridge -> SNS email |
| Windows file-system shim | used | not used (Linux) |

## Known gaps before this is production-ready

- The Glue run itself has not been executed; first deployment may surface
  environment differences (e.g. package import paths).
- Raw ingestion is a one-off `aws s3 sync`; a real plant would push readings via
  IoT Core / Kinesis Firehose into the raw prefix.
- The dashboard reads local Parquet via DuckDB; pointing it at Athena needs a small
  query adapter (e.g. `pyathena`).
