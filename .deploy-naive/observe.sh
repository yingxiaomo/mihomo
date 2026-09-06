#!/bin/sh
# 15-min observation for naive-capable kernel 680841bf
for i in $(seq 1 15); do
  sleep 60
  TS=$(date '+%H:%M:%S')
  E=$(grep -c "level=error" /var/log/nikki/core.log 2>/dev/null)
  C=$(tail -n 300 /var/log/nikki/core.log | grep -c "closed")
  F=$(tail -n 300 /var/log/nikki/core.log | grep -c "failed")
  N=$(tail -n 300 /var/log/nikki/core.log | grep -c "naive")
  echo "$TS err=$E closed=$C failed=$F naive=$N" >> /tmp/observe_result.txt
done
echo DONE >> /tmp/observe_result.txt