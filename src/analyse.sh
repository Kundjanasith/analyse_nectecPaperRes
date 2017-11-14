for i in `seq 1 10`
do
    echo $i
    python analyse.py $i
done
