#!/bin/bash
#x=$((RANDOM%5))
x=2
sleep $x
exit 0
x=$((RANDOM%100))
if [ "$x" -gt 80 ]
then
  exit 1
else
  exit 0
fi
