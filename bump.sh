#!/usr/bin/env bash
git tag $(git tag --sort=-creatordate | head -n 1 | awk -F. '{OFS="."; $NF+=1; print $0}') && git push origin --tags