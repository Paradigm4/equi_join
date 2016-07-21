#!/bin/bash

MYDIR=`dirname $0`
pushd $MYDIR > /dev/null
MYDIR=`pwd`
OUTFILE=$MYDIR/test.out
EXPFILE=$MYDIR/test.expected

iquery -anq "remove(left)"  > /dev/null 2>&1
iquery -anq "remove(right)" > /dev/null 2>&1
iquery -anq "store(apply(build(<a:string>[i=0:5,2,0], '[(null),(def),(ghi),(jkl),(mno)]', true), b, double(i)*1.1), left)" > /dev/null 2>&1
iquery -anq "store(apply(build(<c:string>[j=1:5,3,0], '[(def),(mno),(pqr),(def)]', true), d, j), right)" > /dev/null 2>&1

rm $OUTFILE > /dev/null 2>&1

echo " " >> $OUTFILE 2>&1
echo "Chapter 1" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0'                                                                                           ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=hash_replicate_right'                                                         ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=hash_replicate_left'                                                          ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=merge_left_first'                                                             ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=merge_right_first'                                                            ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=merge_left_first',     'hash_join_threshold=0'                                ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=merge_right_first',    'hash_join_threshold=0'                                ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=hash_replicate_right', 'keep_dimensions=1'                                    ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=hash_replicate_left',  'keep_dimensions=1'                                    ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=merge_left_first',     'keep_dimensions=1'                                    ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=merge_right_first',    'keep_dimensions=1'                                    ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=merge_left_first',     'keep_dimensions=1',    'hash_join_threshold=0'        ), a,b,d)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=0', 'right_ids=0', 'algorithm=merge_right_first',    'keep_dimensions=1',    'hash_join_threshold=0'        ), a,b,d)" >> $OUTFILE 2>&1


echo " " >> $OUTFILE 2>&1
echo "Chapter 2" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0'                                                                                           ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=hash_replicate_right'                                                         ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=hash_replicate_left'                                                          ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=merge_left_first'                                                             ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=merge_right_first'                                                            ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=merge_left_first',     'hash_join_threshold=0'                                ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=merge_right_first',    'hash_join_threshold=0'                                ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=hash_replicate_right', 'keep_dimensions=1'                                    ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=hash_replicate_left',  'keep_dimensions=1'                                    ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=merge_left_first',     'keep_dimensions=1'                                    ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=merge_right_first',    'keep_dimensions=1'                                    ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=merge_left_first',     'keep_dimensions=1',    'hash_join_threshold=0'        ), c,d,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=0', 'right_ids=0', 'algorithm=merge_right_first',    'keep_dimensions=1',    'hash_join_threshold=0'        ), c,d,b)" >> $OUTFILE 2>&1

echo " " >> $OUTFILE 2>&1
echo "Chapter 3" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0'                                                                                                  )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=hash_replicate_right'                                                                )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=hash_replicate_left'                                                                 )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=merge_left_first'                                                                    )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=merge_right_first'                                                                   )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=merge_left_first',     'hash_join_threshold=0'                                       )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=merge_right_first',    'hash_join_threshold=0'                                       )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=hash_replicate_right', 'keep_dimensions=T'                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=hash_replicate_left',  'keep_dimensions=T'                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=merge_left_first',     'keep_dimensions=T'                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=merge_right_first',    'keep_dimensions=T'                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=merge_left_first',     'keep_dimensions=T',   'hash_join_threshold=0'                )" >> $OUTFILE 2>&1
iquery -aq "equi_join(left, right, 'left_ids=0,~0', 'right_ids=0,~0', 'algorithm=merge_right_first',    'keep_dimensions=T',   'hash_join_threshold=0'                )" >> $OUTFILE 2>&1

echo " " >> $OUTFILE 2>&1
echo "Chapter 4" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0'                                                                                                  )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=hash_replicate_right', 'keep_dimensions=0'                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=hash_replicate_left',  'keep_dimensions=0'                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=merge_left_first',     'keep_dimensions=0'                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=merge_right_first',    'keep_dimensions=0'                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=merge_left_first',     'keep_dimensions=0',    'hash_join_threshold=0'               )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=merge_right_first',    'keep_dimensions=0',    'hash_join_threshold=0'               )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=hash_replicate_right', 'keep_dimensions=true'                                        )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=hash_replicate_left',  'keep_dimensions=true'                                        )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=merge_left_first',     'keep_dimensions=true'                                        )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=merge_right_first',    'keep_dimensions=true'                                        )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=merge_left_first',     'keep_dimensions=true', 'hash_join_threshold=0'               )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=~0,0', 'right_ids=~0,0', 'algorithm=merge_right_first',    'keep_dimensions=true', 'hash_join_threshold=0'               )" >> $OUTFILE 2>&1

echo " " >> $OUTFILE 2>&1
echo "Chapter 5" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1'                                                                                        ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=hash_replicate_left', 'keep_dimensions=F'                                  ), i,a,b,c)" >> $OUTFILE 2>&1 
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=hash_replicate_right','keep_dimensions=F'                                  ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=merge_left_first',    'keep_dimensions=F'                                  ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=merge_right_first',   'keep_dimensions=F'                                  ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=merge_left_first',    'keep_dimensions=F',    'hash_join_threshold=0'      ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=merge_right_first',   'keep_dimensions=F',    'hash_join_threshold=0'      ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=hash_replicate_left', 'keep_dimensions=TRUE'                               ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=hash_replicate_right','keep_dimensions=TRUE'                               ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=merge_left_first',    'keep_dimensions=TRUE', 'hash_join_threshold=0'      ), i,a,b,c)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(left, right, 'left_ids=~0', 'right_ids=1', 'algorithm=merge_right_first',   'keep_dimensions=TRUE', 'hash_join_threshold=0'      ), i,a,b,c)" >> $OUTFILE 2>&1

echo " " >> $OUTFILE 2>&1
echo "Chapter 6" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0'                                                                                        ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=hash_replicate_left', 'keep_dimensions=FALSE'                              ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=hash_replicate_right','keep_dimensions=false'                              ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=merge_left_first',    'keep_dimensions=false'                              ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=merge_right_first',   'keep_dimensions=false'                              ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=merge_left_first',    'keep_dimensions=false', 'hash_join_threshold=0'     ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=merge_right_first',   'keep_dimensions=false', 'hash_join_threshold=0'     ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=hash_replicate_left', 'keep_dimensions=true'                               ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=hash_replicate_right','keep_dimensions=TRUE'                               ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=merge_left_first',    'keep_dimensions=TRUE'                               ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=merge_right_first',   'keep_dimensions=TRUE'                               ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=merge_left_first',    'keep_dimensions=TRUE',  'hash_join_threshold=0'     ), d,c,a,b)" >> $OUTFILE 2>&1
iquery -aq "sort(equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'algorithm=merge_right_first',   'keep_dimensions=TRUE',  'hash_join_threshold=0'     ), d,c,a,b)" >> $OUTFILE 2>&1

echo " " >> $OUTFILE 2>&1
echo "Chapter 7" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a=c'                                                                                         )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a=c', 'algorithm=hash_replicate_left'                                                        )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a=c', 'algorithm=hash_replicate_right'                                                       )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a=c', 'algorithm=merge_left_first'                                                           )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a=c', 'algorithm=merge_right_first'                                                          )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a=c', 'algorithm=merge_left_first',     'hash_join_threshold=0'                              )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a=c', 'algorithm=merge_right_first',    'hash_join_threshold=0'                              )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a<>c and j>3', 'algorithm=hash_replicate_left', 'keep_dimensions=1'                          )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a<>c and j>3', 'algorithm=hash_replicate_right','keep_dimensions=1'                          )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a<>c and j>3', 'algorithm=merge_left_first',    'keep_dimensions=1'                          )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a<>c and j>3', 'algorithm=merge_right_first',   'keep_dimensions=1'                          )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a<>c and j>3', 'algorithm=merge_left_first',    'hash_join_threshold=0', 'keep_dimensions=1' )" >> $OUTFILE 2>&1
iquery -aq "equi_join(right, left, 'left_ids=1', 'right_ids=~0', 'filter:a<>c and j>3', 'algorithm=merge_right_first',   'hash_join_threshold=0', 'keep_dimensions=1' )" >> $OUTFILE 2>&1

diff test.out test.expected

