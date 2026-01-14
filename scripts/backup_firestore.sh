#!/usr/bin/env bash
# Simple Firestore export script (requires gcloud & firebase-tools authenticated)
# Usage: ./scripts/backup_firestore.sh <PROJECT_ID> <BUCKET_NAME>

PROJECT_ID=${1:-foco-after-dark}
BUCKET=${2:-foco-after-dark-backups}
STAMP=$(date +%Y%m%d-%H%M%S)
DEST=gcs://${BUCKET}/firestore-backup-${STAMP}

echo "Exporting Firestore for project ${PROJECT_ID} to ${DEST}"
gcloud firestore export ${DEST} --project=${PROJECT_ID}

if [ $? -eq 0 ]; then
  echo "Export complete: ${DEST}"
else
  echo "Export failed"
fi
