for i in `seq 1 10`
do
    echo $i
    cat $i/res_u.csv >> res_u.csv
done
