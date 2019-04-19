#!/bin/bash

start=$1
end=$2


source virtenv
for i in $(seq -s' ' $start $end)
do
   echo $i
   python DetectorController.py $i &
done
wait



