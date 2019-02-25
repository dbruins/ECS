#!/bin/bash
detectors=("STS" "MVD" "TOF" "TRD" "RICH")

processes=()
for i in "${detectors[@]}"
do
 python3 DetectorController.py $i &> /dev/null&
 processes+=($!)
done

terminate() {
	for k in "${processes[@]}"
		do
	    kill $k
	done
}
trap 'terminate' INT
wait
