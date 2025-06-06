#!/bin/bash
set -e

# Run the original entrypoint script
/usr/local/bin/docker-entrypoint.sh "$@"
