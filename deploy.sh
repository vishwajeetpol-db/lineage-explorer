#!/usr/bin/env bash
# Fast deploy script for Lineage Explorer
# Uploads ONLY runtime files (~2MB, ~10 seconds) instead of the full repo.
#
# Usage:
#   ./deploy.sh <profile> <warehouse-id> [app-name] [workspace-path]
#
# Examples:
#   ./deploy.sh fe-vm-vish-aws 9711dcb3942dac99
#   ./deploy.sh fe-vm-vish-aws abc123 lineage-explorer-direct
#   ./deploy.sh my-prod-profile abc123 lineage-explorer /Workspace/Users/me/lineage-explorer

set -euo pipefail

PROFILE="${1:?Usage: ./deploy.sh <profile> <warehouse-id> [app-name] [workspace-path]}"
WAREHOUSE_ID="${2:?Usage: ./deploy.sh <profile> <warehouse-id> [app-name] [workspace-path]}"
APP_NAME="${3:-lineage-explorer-direct}"
WS_PATH="${4:-/Workspace/Users/vishwajeet.pol@databricks.com/$APP_NAME}"

STAGING=$(mktemp -d)
trap "rm -rf $STAGING" EXIT

echo "=== Building staging directory ==="
mkdir -p "$STAGING/backend" "$STAGING/frontend/dist"

cp app.yaml requirements.txt "$STAGING/"
cp backend/__init__.py backend/main.py backend/lineage_service.py backend/models.py "$STAGING/backend/"
cp -r frontend/dist/* "$STAGING/frontend/dist/"

# Inject warehouse ID into app.yaml
sed -i '' "s|value: .*# warehouse_id|value: \"$WAREHOUSE_ID\" # warehouse_id|" "$STAGING/app.yaml" 2>/dev/null || true
# Also handle the direct value format
sed -i '' "s|value: \".*\"|value: \"$WAREHOUSE_ID\"|" "$STAGING/app.yaml"
echo "  Warehouse ID: $WAREHOUSE_ID"

FILE_COUNT=$(find "$STAGING" -type f | wc -l | tr -d ' ')
SIZE=$(du -sh "$STAGING" | cut -f1)
echo "  $FILE_COUNT files, $SIZE total"

echo ""
echo "=== Uploading to $WS_PATH ==="
databricks workspace import-dir "$STAGING" "$WS_PATH" --profile "$PROFILE" --overwrite

echo ""
echo "=== Deploying app: $APP_NAME ==="
databricks apps deploy "$APP_NAME" --source-code-path "$WS_PATH" --profile "$PROFILE"

echo ""
echo "=== Done ==="
