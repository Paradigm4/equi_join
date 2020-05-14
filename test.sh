#!/bin/bash

MYDIR=`dirname $0`
pushd $MYDIR > /dev/null
MYDIR=`pwd`

# --outfile <filename> : write iquery output to the given file.
# This is important for running in the SciDB test harness.
if [ "$1" = "--outfile" ]; then
    OUTFILE="$2"
    shift 2
else
    OUTFILE=$MYDIR/test.out
fi
EXPFILE=$MYDIR/test.expected

# Use csv:l format instead of the default dcsv, to suppress printing dimensions
# because the instance_id dimension will vary based on how many database instances are running.
FMT='-ocsv:l'

# Use this to log output of queries that produce output in a deterministic order.
log_query () {
    iquery "$FMT" -aq "$1" >> $OUTFILE 2>&1
}

# Use this to log output of queries that don't sort their output.
# The output is sorted by the shell before writing to the log file.
log_unsorted_query () {
    ( iquery "$FMT" -aq "$1" | print_header_then sort ) >> $OUTFILE 2>&1
}

# Helper function for log_unsorted_query.
# Given an input stream, prints the first line of the stream,
# then applies the given command (e.g. sort, grep) to the remainder of the stream.
print_header_then () {
    IFS= read -r header
    printf '%s\n' "$header"
    "$@"
}

iquery -anq "remove(left)"  > /dev/null 2>&1
iquery -anq "remove(right)" > /dev/null 2>&1
iquery -anq "store(apply(build(<a:string>[i=0:5,2,0], '[(null),(def),(ghi),(jkl),(mno)]', true), b, double(i)*1.1), left)" > /dev/null 2>&1
iquery -anq "store(apply(build(<c:string>[j=1:5,3,0], '[(def),(mno),(null),(def)]', true), d, j), right)" > /dev/null 2>&1

rm $OUTFILE > /dev/null 2>&1

echo >> $OUTFILE 2>&1
echo "Chapter 1" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0                                                                                       ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_right'                                                     ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_left'                                                      ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first'                                                         ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first'                                                        ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     hash_join_threshold:0                              ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    hash_join_threshold:0                              ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_right', keep_dimensions:1                                  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_left',  keep_dimensions:1                                  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1                                  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1                                  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1,    hash_join_threshold:0        ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1,    hash_join_threshold:0        ), a,b,d)"

echo >> $OUTFILE 2>&1
echo "Chapter 2" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0                                                                                       ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_right'                                                     ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_left'                                                      ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first'                                                         ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first'                                                        ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     hash_join_threshold:0                              ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    hash_join_threshold:0                              ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_right', keep_dimensions:1                                  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_left',  keep_dimensions:1                                  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1                                  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1                                  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1,    hash_join_threshold:0        ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1,    hash_join_threshold:0        ), c,d,b)"

echo >> $OUTFILE 2>&1
echo "Chapter 3" >> $OUTFILE 2>&1
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1)                                                                                   )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_right'                                                 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_left'                                                  )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first'                                                     )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first'                                                    )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     hash_join_threshold:0                          )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    hash_join_threshold:0                          )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_right', keep_dimensions:TRUE                           )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_left',  keep_dimensions:TRUE                           )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     keep_dimensions:TRUE                           )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    keep_dimensions:TRUE                           )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     keep_dimensions:TRUE,   hash_join_threshold:0  )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    keep_dimensions:TRUE,   hash_join_threshold:0  )"

echo >> $OUTFILE 2>&1
echo "Chapter 4" >> $OUTFILE 2>&1
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0)                                                                                   )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_right', keep_dimensions:0                              )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_left',  keep_dimensions:0                              )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:0                              )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:0                              )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:0,    hash_join_threshold:0    )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:0,    hash_join_threshold:0    )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_right', keep_dimensions:true                           )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_left',  keep_dimensions:true                           )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:true                           )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:true                           )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:true, hash_join_threshold:0    )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:true, hash_join_threshold:0    )"

echo >> $OUTFILE 2>&1
echo "Chapter 5" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1                                                                                    ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_left', keep_dimensions:FALSE                            ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_right',keep_dimensions:FALSE                            ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:FALSE                            ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:FALSE                            ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:FALSE,    hash_join_threshold:0  ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:FALSE,    hash_join_threshold:0  ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_left', keep_dimensions:TRUE                             ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_right',keep_dimensions:TRUE                             ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:TRUE, hash_join_threshold:0      ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:TRUE, hash_join_threshold:0      ), i,a,b,c)"

echo >> $OUTFILE 2>&1
echo "Chapter 6" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1                                                                                    ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_left', keep_dimensions:FALSE                            ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_right',keep_dimensions:false                            ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:false                            ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:false                            ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:false, hash_join_threshold:0     ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:false, hash_join_threshold:0     ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_left', keep_dimensions:true                             ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_right',keep_dimensions:TRUE                             ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:TRUE                             ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:TRUE                             ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:TRUE,  hash_join_threshold:0     ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:TRUE,  hash_join_threshold:0     ), d,c,a,b)"

echo >> $OUTFILE 2>&1
echo "Chapter 7" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c'                                                                                     ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'hash_replicate_left'                                                    ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'hash_replicate_right'                                                   ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_left_first'                                                       ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_right_first'                                                      ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_left_first',     hash_join_threshold:0                            ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_right_first',    hash_join_threshold:0                            ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'hash_replicate_left', keep_dimensions:1                        ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'hash_replicate_right',keep_dimensions:1                        ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_left_first',    keep_dimensions:1                        ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_right_first',   keep_dimensions:1                        ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_left_first',    hash_join_threshold:0, keep_dimensions:1 ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_right_first',   hash_join_threshold:0, keep_dimensions:1 ))"

echo >> $OUTFILE 2>&1
echo "Chapter 8" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_names:(j,c), right_names:(i,a), algorithm:'merge_left_first', keep_dimensions:0))"
log_query "sort(equi_join(right, left, left_names:(j,c), right_ids:(-1,0), algorithm:'merge_left_first', keep_dimensions:0 ))"

echo >> $OUTFILE 2>&1
echo "Chapter 9" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0                                                                                 , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_right'                                               , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_left'                                                , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first'                                                   , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first'                                                  , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     hash_join_threshold:0                        , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    hash_join_threshold:0                        , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_right', keep_dimensions:1                            , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_left',  keep_dimensions:1                            , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1                            , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1                            , chunk_size:1  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1,    hash_join_threshold:0, chunk_size:1    ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1,    hash_join_threshold:0, chunk_size:1    ), a,b,d)"


echo >> $OUTFILE 2>&1
echo "Chapter 10" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0                                                                               , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_right'                                             , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_left'                                              , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first'                                                 , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first'                                                , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     hash_join_threshold:0                      , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    hash_join_threshold:0                      , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_right', keep_dimensions:1                          , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_left',  keep_dimensions:1                          , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1                          , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1                          , chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1,    hash_join_threshold:0, chunk_size:1  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1,    hash_join_threshold:0, chunk_size:1  ), c,d,b)"

echo >> $OUTFILE 2>&1
echo "Chapter 11" >> $OUTFILE 2>&1
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1)                                                                                 , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_right'                                               , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_left'                                                , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first'                                                   , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first'                                                  , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     hash_join_threshold:0                        , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    hash_join_threshold:0                        , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_right', keep_dimensions:TRUE                         , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_left',  keep_dimensions:TRUE                         , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     keep_dimensions:TRUE                         , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    keep_dimensions:TRUE                         , chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     keep_dimensions:TRUE,   hash_join_threshold:0, chunk_size:1 )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    keep_dimensions:TRUE,   hash_join_threshold:0, chunk_size:1 )"

echo >> $OUTFILE 2>&1
echo "Chapter 12" >> $OUTFILE 2>&1
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0)                                                                                 , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_right', keep_dimensions:0                            , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_left',  keep_dimensions:0                            , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:0                            , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:0                            , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:0,    hash_join_threshold:0  , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:0,    hash_join_threshold:0  , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_right', keep_dimensions:true                         , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_left',  keep_dimensions:true                         , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:true                         , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:true                         , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:true, hash_join_threshold:0  , chunk_size:1 )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:true, hash_join_threshold:0  , chunk_size:1 )"

echo >> $OUTFILE 2>&1
echo "Chapter 13" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1                                                                               , chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_left', keep_dimensions:FALSE                       , chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_right',keep_dimensions:FALSE                       , chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:FALSE                       , chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:FALSE                       , chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:FALSE, hash_join_threshold:0, chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:FALSE, hash_join_threshold:0, chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_left', keep_dimensions:TRUE                        , chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_right',keep_dimensions:TRUE                        , chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:TRUE,  hash_join_threshold:0, chunk_size:1 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:TRUE,  hash_join_threshold:0, chunk_size:1 ), i,a,b,c)"

echo >> $OUTFILE 2>&1
echo "Chapter 14" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1                                                                               , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_left', keep_dimensions:FALSE                       , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_right',keep_dimensions:false                       , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:false                       , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:false                       , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:false, hash_join_threshold:0, chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:false, hash_join_threshold:0, chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_left', keep_dimensions:true                        , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_right',keep_dimensions:TRUE                        , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:TRUE                        , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:TRUE                        , chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:TRUE,  hash_join_threshold:0, chunk_size:1 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:TRUE,  hash_join_threshold:0, chunk_size:1 ), d,c,a,b)"

echo >> $OUTFILE 2>&1
echo "Chapter 15" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c'                                                                   , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'hash_replicate_left'                                  , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'hash_replicate_right'                                 , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_left_first'                                     , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_right_first'                                    , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_left_first',     hash_join_threshold:0          , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_right_first',    hash_join_threshold:0          , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'hash_replicate_left', keep_dimensions:1      , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'hash_replicate_right',keep_dimensions:1      , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_left_first',    keep_dimensions:1      , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_right_first',   keep_dimensions:1      , chunk_size:1                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_left_first',    hash_join_threshold:0, keep_dimensions:1, chunk_size:1 ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_right_first',   hash_join_threshold:0, keep_dimensions:1, chunk_size:1 ))"

echo >> $OUTFILE 2>&1
echo "Chapter 16" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_names:(j, c), right_names:(i,a), algorithm:'merge_left_first', keep_dimensions:0, chunk_size:1))"
log_query "sort(equi_join(right, left, left_names:(j,c) , right_ids:(-1,0), algorithm:'merge_left_first', keep_dimensions:0,  chunk_size:1 ))"

echo >> $OUTFILE 2>&1
echo "Chapter 17" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0                                                                               , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_right'                                             , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_left'                                              , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first'                                                 , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first'                                                , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     hash_join_threshold:0                      , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    hash_join_threshold:0                      , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_right', keep_dimensions:1                          , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'hash_replicate_left',  keep_dimensions:1                          , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1                          , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1                          , chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1,    hash_join_threshold:0, chunk_size:2  ), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1,    hash_join_threshold:0, chunk_size:2  ), a,b,d)"


echo >> $OUTFILE 2>&1
echo "Chapter 18" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0                                                                               , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_right'                                             , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_left'                                              , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first'                                                 , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first'                                                , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     hash_join_threshold:0                      , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    hash_join_threshold:0                      , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_right', keep_dimensions:1                          , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'hash_replicate_left',  keep_dimensions:1                          , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1                          , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1                          , chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_left_first',     keep_dimensions:1,    hash_join_threshold:0, chunk_size:2  ), c,d,b)"
log_query "sort(equi_join(right, left, left_ids:0, right_ids:0, algorithm:'merge_right_first',    keep_dimensions:1,    hash_join_threshold:0, chunk_size:2  ), c,d,b)"

echo >> $OUTFILE 2>&1
echo "Chapter 19" >> $OUTFILE 2>&1
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1)                                                                                 , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_right'                                               , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_left'                                                , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first'                                                   , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first'                                                  , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     hash_join_threshold:0                        , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    hash_join_threshold:0                        , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_right', keep_dimensions:TRUE                         , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'hash_replicate_left',  keep_dimensions:TRUE                         , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     keep_dimensions:TRUE                         , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    keep_dimensions:TRUE                         , chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_left_first',     keep_dimensions:TRUE,   hash_join_threshold:0, chunk_size:2                )"
log_unsorted_query "equi_join(left, right, left_ids:(0,-1), right_ids:(0,-1), algorithm:'merge_right_first',    keep_dimensions:TRUE,   hash_join_threshold:0, chunk_size:2                )"

echo >> $OUTFILE 2>&1
echo "Chapter 20" >> $OUTFILE 2>&1
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0)                                                                               , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_right', keep_dimensions:0                          , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_left',  keep_dimensions:0                          , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:0                          , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:0                          , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:0,    hash_join_threshold:0, chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:0,    hash_join_threshold:0, chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_right', keep_dimensions:true                       , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'hash_replicate_left',  keep_dimensions:true                       , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:true                       , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:true                       , chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_left_first',     keep_dimensions:true, hash_join_threshold:0, chunk_size:2               )"
log_unsorted_query "equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), algorithm:'merge_right_first',    keep_dimensions:true, hash_join_threshold:0, chunk_size:2               )"

echo >> $OUTFILE 2>&1
echo "Chapter 21" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1                                                                               , chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_left', keep_dimensions:FALSE                       , chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_right',keep_dimensions:FALSE                       , chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:FALSE                       , chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:FALSE                       , chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:FALSE, hash_join_threshold:0, chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:FALSE, hash_join_threshold:0, chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_left', keep_dimensions:TRUE                        , chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'hash_replicate_right',keep_dimensions:TRUE                        , chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_left_first',    keep_dimensions:TRUE, hash_join_threshold:0 , chunk_size:2 ), i,a,b,c)"
log_query "sort(equi_join(left, right, left_ids:-1, right_ids:1, algorithm:'merge_right_first',   keep_dimensions:TRUE, hash_join_threshold:0 , chunk_size:2 ), i,a,b,c)"

echo >> $OUTFILE 2>&1
echo "Chapter 22" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1                                                                               , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_left', keep_dimensions:FALSE                       , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_right',keep_dimensions:false                       , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:false                       , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:false                       , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:false, hash_join_threshold:0, chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:false, hash_join_threshold:0, chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_left', keep_dimensions:true                        , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'hash_replicate_right',keep_dimensions:TRUE                        , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:TRUE                        , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:TRUE                        , chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_left_first',    keep_dimensions:TRUE,  hash_join_threshold:0, chunk_size:2 ), d,c,a,b)"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, algorithm:'merge_right_first',   keep_dimensions:TRUE,  hash_join_threshold:0, chunk_size:2 ), d,c,a,b)"

echo >> $OUTFILE 2>&1
echo "Chapter 23" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c'                                                                   , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'hash_replicate_left'                                  , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'hash_replicate_right'                                 , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_left_first'                                     , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_right_first'                                    , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_left_first',     hash_join_threshold:0          , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a=c', algorithm:'merge_right_first',    hash_join_threshold:0          , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'hash_replicate_left', keep_dimensions:1      , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'hash_replicate_right',keep_dimensions:1      , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_left_first',    keep_dimensions:1      , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_right_first',   keep_dimensions:1      , chunk_size:2                  ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_left_first',    hash_join_threshold:0, keep_dimensions:1, chunk_size:2 ))"
log_query "sort(equi_join(right, left, left_ids:1, right_ids:-1, filter:'a<>c and j>3', algorithm:'merge_right_first',   hash_join_threshold:0, keep_dimensions:1, chunk_size:2 ))"

echo >> $OUTFILE 2>&1
echo "Chapter 24" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_names:(j,c), right_names:(i,a), algorithm:'merge_left_first', keep_dimensions:0, chunk_size:2))"
log_query "sort(equi_join(right, left, left_names:(j,c), right_ids:(-1,0), algorithm:'merge_left_first', keep_dimensions:0,  chunk_size:2 ))"

echo >> $OUTFILE 2>&1
echo "Chapter 25" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, left_outer:true), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, left_outer:true, algorithm:'hash_replicate_right'), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, left_outer:true, algorithm:'merge_left_first'), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, left_outer:true, algorithm:'merge_right_first'), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, left_outer:true, algorithm:'merge_left_first', hash_join_threshold:0), a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, left_outer:true, algorithm:'merge_right_first', hash_join_threshold:0), a,b,d)"

echo >> $OUTFILE 2>&1
echo "Chapter 26" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, right_outer:true),a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, right_outer:true, algorithm:'hash_replicate_left'),a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, right_outer:true, algorithm:'merge_left_first'),a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, right_outer:true, algorithm:'merge_right_first'),a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, right_outer:true, algorithm:'merge_left_first', hash_join_threshold:0),a,b,d)"
log_query "sort(equi_join(left, right, left_ids:0, right_ids:0, right_outer:true, algorithm:'merge_right_first', hash_join_threshold:0),a,b,d)"

echo >> $OUTFILE 2>&1
echo "Chapter 27" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), left_outer:true, keep_dimensions:1),j,c,d)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), left_outer:true, keep_dimensions:1, algorithm:'hash_replicate_right'),j,c,d)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), left_outer:true, keep_dimensions:1, algorithm:'merge_left_first'),j,c,d)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), left_outer:true, keep_dimensions:1, algorithm:'merge_right_first'),j,c,d)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), left_outer:true, keep_dimensions:1, algorithm:'merge_left_first', hash_join_threshold:0),j,c,d)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), left_outer:true, keep_dimensions:1, algorithm:'merge_right_first', hash_join_threshold:0),j,c,d)"

echo >> $OUTFILE 2>&1
echo "Chapter 28" >> $OUTFILE 2>&1
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), right_outer:true, keep_dimensions:1),j,c)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), right_outer:true, keep_dimensions:1, algorithm:'hash_replicate_left'),j,c)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), right_outer:true, keep_dimensions:1, algorithm:'merge_left_first'),j,c)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), right_outer:true, keep_dimensions:1, algorithm:'merge_right_first'),j,c)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), right_outer:true, keep_dimensions:1, algorithm:'merge_left_first', hash_join_threshold:0),j,c)"
log_query "sort(equi_join(right, left, left_ids:(-1,0), right_ids:(-1,0), right_outer:true, keep_dimensions:1, algorithm:'merge_right_first', hash_join_threshold:0),j,c)"

echo >> $OUTFILE 2>&1
echo "Chapter 29" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_ids:(0,-1), right_ids:(0,1), left_outer:1, right_outer:1, keep_dimensions:1, algorithm:'merge_right_first'), a, i,b)"
log_query "sort(equi_join(left, right, left_ids:(0,-1), right_ids:(0,1), left_outer:1, right_outer:1, keep_dimensions:1, algorithm:'merge_left_first'), a, i,b)"

echo >> $OUTFILE 2>&1
echo "Chapter 30" >> $OUTFILE 2>&1
log_query "sort(equi_join(left, right, left_names:i, right_names:j, left_outer:1, right_outer:1, out_names:(uv,w,x,y_,z)), uv)"
log_query "sort(equi_join(left, right, left_names:i, right_names:j, left_outer:1, right_outer:1, algorithm:'merge_left_first'),  i)"
log_query "sort(equi_join(left, right, left_names:i, right_names:j, left_outer:1, right_outer:1, algorithm:'merge_right_first'), i)"

diff $OUTFILE test.expected && echo "$(basename $0) succeeded"
