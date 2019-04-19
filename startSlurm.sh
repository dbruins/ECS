#!/bin/bash

#SBATCH --nodelist=pn[30-35]
#!/bin/sh
#SBATCH --job-name=ECS_Clients
#SBATCH -o outfile  # send stdout to outfile
#SBATCH -e errfile  # send stderr to errfile
#SBATCH -t 0:20:00  # time requested in hour:minute:second

if [ -z "$1" ]
then
   echo "please enter number of detectors"
   exit 1
fi

#nodeList=(pn02 pn04 pn06 pn07 pn08)
nodeList=(pn30 pn31 pn32 pn33 pn34)   

#pcas + globalsystems
srun -w "pn35" pca_and_globalSystem_start.sh &

node_count=${#nodeList[@]}
detector_count=$1

detectors_per_node=$((detector_count/$node_count))
rest=$((detector_count%$node_count))
start=50
end=$((start+detectors_per_node-1))

#detectors
#for i in $(seq 4 $((node_count+4-1)))
for i in ${nodeList[@]}
do
 if [ $rest -gt 0 ]
 then
   end=$((end+1))
   rest=$((rest-1))
 fi
  srun -w $i ./detectorStart.sh $start $end &
  start=$((end+1))
  end=$((end+detectors_per_node))
done
wait


