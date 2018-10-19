#!/bin/bash
x=$((RANDOM%5))
sleep $x
x=$((RANDOM%100))
if [ "$x" -gt 101 ]
then
  exit 1
else
  exit 0
fi
