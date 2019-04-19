#!/bin/bash

source virtenv
#python PCA.py pca1 &
#python PCA.py pca2 &
python PCA.py test &

python GlobalSystemClient.py all > /dev/null  &

wait

