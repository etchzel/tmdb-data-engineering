#!/bin/bash

set -e

BUCKET=$1
FILE_PATH=$2
OBJ_NAME=$3
CONTENT_TYPE=$4

DATE=$(date -R)
_REQUEST="PUT\n\n${CONTENT_TYPE}\n${DATE}\n/${BUCKET}/${OBJ_NAME}"
SIGNATURE=$(echo -en "${_REQUEST}" | openssl sha1 -hmac ${AWS_SECRET_ACCESS_KEY} -binary | base64)

# upload the file to MinIO
if curl -X PUT \
        -T ${FILE_PATH} \
        -fsS \
        -H "Date: ${DATE}" \
        -H "Content-Type: ${CONTENT_TYPE}" \
        -H "Authorization: AWS ${AWS_ACCESS_KEY_ID}:${SIGNATURE}" \
        "http://minio:9000/${BUCKET}/${OBJ_NAME}"; then
    echo "File uploaded successfully to ${BUCKET}/${OBJ_NAME}"
    exit 0
else
    echo "Failed to upload file to ${BUCKET}/${OBJ_NAME}"
    exit 1
fi
