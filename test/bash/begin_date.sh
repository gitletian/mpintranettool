#!/bin/bash
BATCH_DATE=2017-07-19



while [[ ! "${BATCH_DATE}" > "2017-07-20" ]];
do
  # echo "check done file"

  # day by day
BATCH_DATE=$(date -d "${BATCH_DATE} +1 days" "+%Y-%m-%d")
echo $BATCH_DATE

done
exit 0
