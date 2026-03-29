#!/usr/bin/env bash
# Fast deploy script for Lineage Explorer
# Uploads ONLY runtime files (~2MB, ~10 seconds) instead of the full repo.
#
# Usage:
#   ./deploy.sh <profile> <warehouse-id> [app-name]
#
# Examples:
#   ./deploy.sh my-profile abc123def456
#   ./deploy.sh my-profile abc123def456 my-lineage-app
#
# The workspace path is auto-resolved from the deploying user's identity.
# The app must already exist (create it first with `databricks bundle deploy`).

set -euo pipefail

PROFILE="${1:?Usage: ./deploy.sh <profile> <warehouse-id> [app-name]}"
WAREHOUSE_ID="${2:?Usage: ./deploy.sh <profile> <warehouse-id> [app-name]}"
APP_NAME="${3:-lineage-explorer}"

# Auto-resolve the deploying user's workspace path
CURRENT_USER=$(databricks current-user me --profile "$PROFILE" -o json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])" 2>/dev/null) || true

if [ -z "$CURRENT_USER" ]; then
  echo "ERROR: Could not resolve current user. Check your CLI profile."
  exit 1
fi

WS_PATH="/Workspace/Users/${CURRENT_USER}/${APP_NAME}"

STAGING=$(mktemp -d)
trap "rm -rf $STAGING" EXIT

echo "=== Building staging directory ==="
mkdir -p "$STAGING/backend" "$STAGING/frontend/dist"

cp app.yaml requirements.txt "$STAGING/"
cp backend/__init__.py backend/main.py backend/lineage_service.py backend/models.py "$STAGING/backend/"
cp -r frontend/dist/* "$STAGING/frontend/dist/"

# Inject warehouse ID into the staging copy of app.yaml (repo file stays parameterized)
sed -i '' "s|value: \"<your-warehouse-id>\"|value: \"$WAREHOUSE_ID\"|" "$STAGING/app.yaml" 2>/dev/null || \
  sed -i "s|value: \"<your-warehouse-id>\"|value: \"$WAREHOUSE_ID\"|" "$STAGING/app.yaml"
echo "  Warehouse ID: $WAREHOUSE_ID"
echo "  User: $CURRENT_USER"

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
