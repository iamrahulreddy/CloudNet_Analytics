#!/bin/bash
BUCKET="log-analytics-bucket"
VMS=("web-server" "app-server")
ZONE="us-central1-a"
PROJECT="<Your_GCP_PROJECT>"

for vm in "${VMS[@]}"; do
    gcloud compute scp "iamrahul251@$vm:<PATH>/logs/*.log" /tmp/ --zone="$ZONE" --project="$PROJECT" || { echo "SCP failed for $vm"; exit 1; }
done

python3 /home/iamrahul251/process_logs.py || { echo "Processing failed"; exit 1; }
