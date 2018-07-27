
#start dummys
for i in `seq 1 $1`
do
 python3 dummy.py $i 1> /dev/null&
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
#python3 ./Django/ECS_GUI/manage.py runserver > /dev/null &
#i=$((i+1))
#arr[$i]=$!


STR="press key to terminate\n"
read -p "press key to terminate\n"

for i in `seq 1 $1`
do
    kill ${arr[$i]}
done
