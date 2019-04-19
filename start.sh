#!/bin/bash
#start dummys
if [ -n "$2" ] && [ $2 -gt 1 ]; then
  first=$2
else
  first=1
fi

for i in `seq $first $1`
do
 python3 DetectorController.py $i &
 arr[$i]=$!
done

terminate() {
	for k in `seq 1 $i`
	do
		kill ${arr[$k]}
	done
}

trap 'terminate' INT
wait
