#!/usr/bin/env bash
# Creates a new github release
set -e

latest=$(git tag --sort=-version:refname | head -n1)
version=${latest}
IFS=. read -r major minor patch <<< "$version"
next="$major.$minor.$((patch + 1))"

git tag "$next"
git push origin "$next"
gh release create "$next" --generate-notes