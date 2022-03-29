#!/bin/sh

if ! command -v docker-compose >/dev/null; then
  echo "\`docker-compose\` not found. Compose is required to build the sample data."
  exit 1
fi

if test -z "$WL_SAMPLE_DATA_OUTPUT_DIR"; then
  WL_SAMPLE_DATA_OUTPUT_DIR=$(realpath $(dirname $0)/../../examples/data/)
fi

echo "Using output dir $WL_SAMPLE_DATA_OUTPUT_DIR"
export WL_SAMPLE_DATA_OUTPUT_DIR
cd $(dirname $0)
docker-compose build && docker-compose run --rm -T generator
docker-compose down
