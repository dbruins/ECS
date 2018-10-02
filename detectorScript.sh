x=$(shuf -i 1-8 -n 1)
sleep $x
x=$(shuf -i 0-10 -n 1)
if [ "$x" -gt 9 ]
then
  exit 1
else
  exit 0
fi
