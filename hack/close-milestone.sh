#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:?Usage: close-milestone.sh <VERSION> (e.g. 0.1.3)}"
REPO="${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}"
TAG="v${VERSION}"

ms_json=$(gh api "repos/${REPO}/milestones?state=open&per_page=100" 2>/dev/null || echo "[]")
milestone_number=$(echo "$ms_json" | jq -r ".[] | select(.title == \"${TAG}\") | .number // empty")

if [[ -z "$milestone_number" ]]; then
  echo "No open milestone '${TAG}' found — skipping."
  exit 0
fi

echo "Closing issues in milestone ${TAG}..."
while IFS= read -r num; do
  gh issue close "$num" --repo "$REPO" \
    --comment "Closed by release [${TAG}](https://github.com/${REPO}/releases/tag/${TAG})."
  echo "  Closed #${num}"
done < <(gh issue list --repo "$REPO" --milestone "$TAG" --state open --limit 200 \
  --json number --jq '.[].number')

echo "Closing milestone ${TAG}..."
gh api --method PATCH "repos/${REPO}/milestones/${milestone_number}" \
  -f state=closed --silent
echo "Done."
