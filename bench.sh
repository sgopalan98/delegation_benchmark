set -x

BIN=./target/release/hashtable_client
OUT=./results

mkdir -p "$OUT"
cargo build --release --bin hashtable_client

function bench {
    date

    file="$OUT/$1.csv"
    rm $file
    header="name,no,total_ops,threads,spent,throughput,latency"
    echo $header > $file

    
    for (( threads=1; threads<=$3; threads++ ));
    do
        $BIN -t $1 -i $2 -c $threads -o $4
    done

    echo '$1 done' 
}

capacity=$1
threads=$2
ops_st=$3


bench ReadHeavy $capacity $threads $ops_st
bench Exchange $capacity $threads $ops_st
bench RapidGrow $capacity $threads $ops_st
