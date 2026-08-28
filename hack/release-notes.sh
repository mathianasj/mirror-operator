#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:?Usage: release-notes.sh <VERSION> (e.g. 0.1.3)}"
REPO="${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}"
TAG="v${VERSION}"
PREV_TAG=$(git describe --tags --abbrev=0 "${TAG}^" 2>/dev/null || echo "")

milestone_issues=()

ms_json=$(gh api "repos/${REPO}/milestones?state=all&per_page=100" 2>/dev/null || echo "[]")
milestone_number=$(echo "$ms_json" | jq -r ".[] | select(.title == \"${TAG}\") | .number // empty")

if [[ -n "$milestone_number" ]]; then
  while IFS=$'\t' read -r num title labels; do
    milestone_issues+=("${num}|${title}|${labels}")
  done < <(gh issue list --repo "$REPO" --milestone "$TAG" --state all --limit 200 \
    --json number,title,labels \
    --jq '.[] | [.number, .title, ([.labels[].name] | join(","))] | @tsv')
fi

commit_issues=()
if [[ -n "$PREV_TAG" ]]; then
  while IFS= read -r num; do
    commit_issues+=("$num")
  done < <(git log "${PREV_TAG}..${TAG}" --oneline 2>/dev/null \
    | grep -oE '#[0-9]+' | tr -d '#' | sort -un)
fi

seen_nums=""
all_issues=()
for entry in "${milestone_issues[@]+"${milestone_issues[@]}"}"; do
  num="${entry%%|*}"
  seen_nums="${seen_nums} ${num} "
  all_issues+=("$entry")
done

for num in "${commit_issues[@]+"${commit_issues[@]}"}"; do
  case "$seen_nums" in
    *" ${num} "*) ;;
    *)
      issue_json=$(gh issue view "$num" --repo "$REPO" --json title,labels 2>/dev/null || echo "")
      if [[ -n "$issue_json" ]]; then
        title=$(echo "$issue_json" | jq -r '.title')
        labels=$(echo "$issue_json" | jq -r '[.labels[].name] | join(",")')
        all_issues+=("${num}|${title}|${labels}")
        seen_nums="${seen_nums} ${num} "
      fi
      ;;
  esac
done

bugs=()
enhancements=()
other=()

for entry in "${all_issues[@]+"${all_issues[@]}"}"; do
  num="${entry%%|*}"
  rest="${entry#*|}"
  title="${rest%%|*}"
  labels="${rest##*|}"

  line="- ${title} (#${num})"

  if echo "$labels" | grep -qi 'bug'; then
    bugs+=("$line")
  elif echo "$labels" | grep -qiE 'enhancement|feature'; then
    enhancements+=("$line")
  else
    other+=("$line")
  fi
done

notes_file=$(mktemp /tmp/release-notes-XXXXXX.md)
{
  if [[ ${#bugs[@]} -gt 0 ]]; then
    echo "## Bug Fixes"
    printf '%s\n' "${bugs[@]}"
    echo
  fi
  if [[ ${#enhancements[@]} -gt 0 ]]; then
    echo "## Enhancements"
    printf '%s\n' "${enhancements[@]}"
    echo
  fi
  if [[ ${#other[@]} -gt 0 ]]; then
    echo "## Other Changes"
    printf '%s\n' "${other[@]}"
    echo
  fi
  if [[ ${#all_issues[@]} -eq 0 ]]; then
    echo "No tracked issues for this release."
    echo
  fi
  if [[ -n "$PREV_TAG" ]]; then
    echo "**Full Changelog**: https://github.com/${REPO}/compare/${PREV_TAG}...${TAG}"
  fi
} > "$notes_file"

echo "$notes_file"
