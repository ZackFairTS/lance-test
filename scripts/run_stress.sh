#!/bin/bash
set -eu
source /home/hadoop/lance-stress/run.env

RUN_TAG="${1:-main-$(date +%H%M%S)}"
DATASET_PATH="${S3_BASE}/${RUN_TAG}/dataset"
PHASE1_SECONDS=${PHASE1_SECONDS:-180}
PHASE2_SECONDS=${PHASE2_SECONDS:-600}
PHASE3_SECONDS=${PHASE3_SECONDS:-120}
TOTAL_SECONDS=$((PHASE1_SECONDS + PHASE2_SECONDS + PHASE3_SECONDS + 60))
RATE_TOTAL=${RATE_TOTAL:-10000}
PARALLELISM=${PARALLELISM:-4}
BATCH_SIZE=${BATCH_SIZE:-1024}
COMPACTOR_SLEEP=${COMPACTOR_SLEEP:-3}

LOGDIR=/home/hadoop/lance-stress/logs/${RUN_TAG}
mkdir -p $LOGDIR
echo "=== Run ${RUN_TAG}" | tee $LOGDIR/summary.txt
echo "Dataset: $DATASET_PATH" | tee -a $LOGDIR/summary.txt
echo "Parallelism=$PARALLELISM rate=$RATE_TOTAL batch=$BATCH_SIZE" | tee -a $LOGDIR/summary.txt
echo "Phases: baseline=${PHASE1_SECONDS}s concurrent=${PHASE2_SECONDS}s recovery=${PHASE3_SECONDS}s" | tee -a $LOGDIR/summary.txt

echo "=== Refresh AWS creds"
TOKEN=$(curl -s -m 2 -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 3600")
ROLE=$(curl -s -m 2 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/iam/security-credentials/)
CREDS_JSON=$(curl -s -m 2 -H "X-aws-ec2-metadata-token: $TOKEN" "http://169.254.169.254/latest/meta-data/iam/security-credentials/$ROLE")
python3 -c "
import json,sys
c = json.loads('''$CREDS_JSON''')
open('/home/hadoop/lance-stress/aws.env','w').write(f'''AWS_ACCESS_KEY_ID={c[\"AccessKeyId\"]}
AWS_SECRET_ACCESS_KEY={c[\"SecretAccessKey\"]}
AWS_SESSION_TOKEN={c[\"Token\"]}
AWS_REGION=ap-northeast-1
AWS_DEFAULT_REGION=ap-northeast-1
''')
print('Creds expire:', c['Expiration'])
"

echo "=== Pre-creating dataset"
sudo docker run --rm --env-file /home/hadoop/lance-stress/aws.env -v /home/hadoop/lance-stress:/workspace \
  lance-flink-test:v39 python3 /workspace/scripts/create_dataset.py "$DATASET_PATH" 2>&1 | tail -3

echo "=== Starting metrics collector (will re-init once job is known)"

echo "=== Submitting Flink job (runs for ${TOTAL_SECONDS}s)"
JOB_OUT=$(sudo docker exec jobmanager \
  bash -c "export S3_PATH='$DATASET_PATH' RATE_TOTAL=$RATE_TOTAL PARALLELISM=$PARALLELISM BATCH_SIZE=$BATCH_SIZE RUN_SECONDS=$TOTAL_SECONDS; \
  /opt/flink/bin/flink run -d -c com.stress.StressJob /workspace/stress-job/target/lance-stress-job-1.0.jar 2>&1")
echo "$JOB_OUT" > $LOGDIR/submit.log
JID=$(echo "$JOB_OUT" | grep -oE "JobID [a-f0-9]+" | awk '{print $2}')
echo "  JobID: $JID"
echo "JobID=$JID" >> $LOGDIR/summary.txt

echo "=== Starting metrics collector (targeting $JID)"
nohup python3 /home/hadoop/lance-stress/scripts/metrics_collector.py $LOGDIR/metrics.csv 2 $JID > $LOGDIR/collector.log 2>&1 &
COLLECTOR_PID=$!
echo "  collector pid $COLLECTOR_PID"

echo ""
echo "=== PHASE 1: baseline (no compaction) for ${PHASE1_SECONDS}s"
PHASE1_START=$(date +%s)
sleep $PHASE1_SECONDS

echo ""
echo "=== PHASE 2: concurrent compaction + writes for ${PHASE2_SECONDS}s"
PHASE2_START=$(date +%s)
sudo docker run -d --rm --name compactor \
  --env-file /home/hadoop/lance-stress/aws.env \
  -v /home/hadoop/lance-stress:/workspace \
  lance-flink-test:v39 \
  python3 -u /workspace/scripts/compactor.py "$DATASET_PATH" /workspace/logs/${RUN_TAG}/compactor.log ${COMPACTOR_SLEEP} 2>&1
echo "  compactor started as docker container"
sudo docker ps --filter name=compactor --format "{{.Names}}: {{.Status}}"
sleep $PHASE2_SECONDS

echo ""
echo "=== PHASE 3: stop compaction, recover for ${PHASE3_SECONDS}s"
PHASE3_START=$(date +%s)
sudo docker stop compactor 2>&1 | tail -1 || true
sleep $PHASE3_SECONDS

echo ""
echo "=== Stopping collector"
kill $COLLECTOR_PID 2>/dev/null || true

echo ""
echo "=== Cancelling job"
curl -s -X PATCH "http://localhost:18081/jobs/$JID" > /dev/null || true
sleep 5

echo "=== Phase timestamps"
echo "PHASE1_START=$PHASE1_START" >> $LOGDIR/summary.txt
echo "PHASE2_START=$PHASE2_START" >> $LOGDIR/summary.txt
echo "PHASE3_START=$PHASE3_START" >> $LOGDIR/summary.txt

echo ""
echo "=== Final job info"
curl -s "http://localhost:18081/jobs/$JID" | python3 -m json.tool > $LOGDIR/final-job.json
curl -s "http://localhost:18081/jobs/$JID/exceptions" | python3 -m json.tool > $LOGDIR/exceptions.json
curl -s "http://localhost:18081/jobs/$JID/checkpoints" | python3 -m json.tool > $LOGDIR/checkpoints.json

echo ""
echo "=== Collecting TaskManager log"
sudo docker logs taskmanager > $LOGDIR/taskmanager.log 2>&1

echo ""
echo "=== Verifying final data"
EXPECTED=$((RATE_TOTAL * TOTAL_SECONDS))
sudo docker run --rm --env-file /home/hadoop/lance-stress/aws.env -v /home/hadoop/lance-stress:/workspace \
  lance-flink-test:v39 \
  python3 /workspace/scripts/verify_data.py "$DATASET_PATH" $EXPECTED /workspace/logs/${RUN_TAG}/verify.json 2>&1 | tee $LOGDIR/verify.txt

echo ""
echo "=== S3 size check"
sudo docker run --rm --env-file /home/hadoop/lance-stress/aws.env \
  lance-flink-test:v39 \
  bash -c "pip install awscli -q 2>/dev/null; aws s3 ls --summarize --recursive $DATASET_PATH" 2>&1 | tail -10 > $LOGDIR/s3-size.txt || echo "aws cli missing" > $LOGDIR/s3-size.txt

echo ""
echo "=== DONE. Logs in $LOGDIR"
ls -la $LOGDIR
