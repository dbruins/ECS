#!/bin/bash
#start dummys
#for i in `seq 1 $1`
for i in `seq 50 $(($1+50))`
do
 python3 DetectorController.py $i 1> /dev/null&
 arr[$i]=$!
done

#start pcas
if [ "$#" -gt "1" ]
 then
   for j in `seq 1 $2`
   do
    python3 PCA.py "pca$j" 1> /dev/null &
    i=$((i+1))
    arr[$i]=$!
   done
fi

read -p "press key to terminate\n"

#for k in `seq 1 $i`
for k in `seq 50 $(($1+50))`
do
    kill ${arr[$k]}
done
