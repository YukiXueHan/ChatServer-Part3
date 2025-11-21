#!/bin/bash

# Creates DDB tables for message storage and analytics

set -e

REGION="${AWS_REGION:-us-west-2}"
ACCOUNT_ID="730335612437"

echo "=========================================="
echo "Creating DynamoDB Tables"
echo "=========================================="
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"
echo ""

# ----------------------------
# Table 1: chatflow-messages
# ----------------------------
echo "Creating table: chatflow-messages"
aws dynamodb create-table \
    --table-name chatflow-messages \
    --attribute-definitions \
        AttributeName=roomId,AttributeType=S \
        AttributeName=timestamp,AttributeType=S \
        AttributeName=userId,AttributeType=S \
        AttributeName=messageId,AttributeType=S \
    --key-schema \
        AttributeName=roomId,KeyType=HASH \
        AttributeName=timestamp,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --global-secondary-indexes '[
        {
            "IndexName": "UserMessagesIndex",
            "KeySchema": [
                {"AttributeName": "userId", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        },
        {
            "IndexName": "MessageIdIndex",
            "KeySchema": [
                {"AttributeName": "messageId", "KeyType": "HASH"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }
    ]' \
    --region $REGION || echo "Table chatflow-messages already exists"

echo "✓ Table chatflow-messages created or already exists"
echo ""

# ----------------------------
# Table 2: chatflow-room-participation
# ----------------------------
echo "Creating table: chatflow-room-participation"
aws dynamodb create-table \
    --table-name chatflow-room-participation \
    --attribute-definitions \
        AttributeName=userId,AttributeType=S \
        AttributeName=roomId,AttributeType=S \
    --key-schema \
        AttributeName=userId,KeyType=HASH \
        AttributeName=roomId,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION || echo "Table chatflow-room-participation already exists"

echo "✓ Table chatflow-room-participation created or already exists"
echo ""

# ----------------------------
# Table 3: chatflow-analytics
# ----------------------------
echo "Creating table: chatflow-analytics"
aws dynamodb create-table \
    --table-name chatflow-analytics \
    --attribute-definitions \
        AttributeName=metricType,AttributeType=S \
        AttributeName=entityId,AttributeType=S \
    --key-schema \
        AttributeName=metricType,KeyType=HASH \
        AttributeName=entityId,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION || echo "Table chatflow-analytics already exists"

echo "✓ Table chatflow-analytics created or already exists"
echo ""

# ----------------------------
# 完成提示
# ----------------------------
echo ""
echo "=========================================="
echo "All tables processed!"
echo "=========================================="
echo ""
echo "Tables:"
echo "  1. chatflow-messages (with GSIs: UserMessagesIndex, MessageIdIndex)"
echo "  2. chatflow-room-participation"
echo "  3. chatflow-analytics"
echo ""
echo "Verify tables:"
echo "  aws dynamodb list-tables --region $REGION"
echo ""
echo "View table details:"
echo "  aws dynamodb describe-table --table-name chatflow-messages --region $REGION"
