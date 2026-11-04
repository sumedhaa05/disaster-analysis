#!/bin/bash
# Downloads NOAA Storm Events "details" files for the chosen years into data/raw
set -e
BASE=https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
mkdir -p data/raw && cd data/raw
for y in 2019 2020 2021 2022 2023; do
  f=$(curl -s $BASE | grep -o "StormEvents_details-ftp_v1.0_d${y}_c[0-9]*\.csv\.gz" | sort -u | tail -1)
  echo "Downloading $f"
  curl -O "$BASE$f"
done
