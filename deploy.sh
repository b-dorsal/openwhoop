#!/bin/bash
set -e

# Configuration
REGION="us-central1"
JOB_NAME="hr-processing"

REPO_NAME=openwhoop

# Get current project ID
PROJECT_ID=openwhoops


# We'll use Google Container Registry (GCR) format or Artifact Registry.
# For simplicity, using GCR format: gcr.io/PROJECT_ID/IMAGE_NAME
IMAGE_NAME="us-central1-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${JOB_NAME}:latest"

echo "Building Docker image locally ($IMAGE_NAME)..."
docker build --platform linux/amd64 -t "$IMAGE_NAME" .

echo "Pushing Docker image to Google Container Registry..."
docker push "$IMAGE_NAME"

echo "Updating Cloud Run Job ($JOB_NAME) with the new image..."
gcloud run jobs update "$JOB_NAME" \
  --image "$IMAGE_NAME" \
  --region "$REGION"

echo "Triggering a new execution of the Cloud Run Job..."
gcloud run jobs execute "$JOB_NAME" \
  --region "$REGION" \
  --format="value(name)"

echo "Deployment and execution trigger complete!"
