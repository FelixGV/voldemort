#!/bin/bash

if [[ $# != 3 ]]; then
        echo "Usage: $0 remote branch commit-comment"
        exit 1
fi

REMOTE=$1
BRANCH=$2
COMMENT=$3

# Exit on error
set -e

./gradlew jar
git commit -a -m "$3"
git push $REMOTE $BRANCH
