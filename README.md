# rjoin
Relational-style Join of SciDB Arrays by Attributes. Work in progress.

## Examples
```
#Make up some arrays:
$ iquery -aq "store(apply(build(<a:string>[i=0:5,2,0], '[(abc),(def),(ghi),(jkl),(mno)]', true), b, double(i)*1.1), left)"
{i} a,b
{0} 'abc',0
{1} 'def',1.1
{2} 'ghi',2.2
{3} 'jkl',3.3
{4} 'mno',4.4

$ iquery -aq "store(apply(build(<c:string>[j=1:5,3,0], '[(def),(mno),(pqr),(def)]', true), d, j), right)"
{j} c,d
{1} 'def',1
{2} 'mno',2
{3} 'pqr',3
{4} 'def',4

#Join the arrays on attribute 0 (string,i.e. left.a=right.c). This is difficult right now otherwise:
$ iquery -aq "rjoin(left, right, 'left_ids=0', 'right_ids=0')"
{instance_id,value_no} a,b,d
{0,0} 'def',1.1,1
{0,1} 'def',1.1,4
{2,0} 'mno',4.4,2

#Join on two keys: left.i = right.d (note: dimension to attribute) and left.a=right.c
$ iquery -aq "rjoin(left, right, 'left_ids=~0,0', 'right_ids=1,0')"
{instance_id,value_no} i,a,b
{0,0} 1,'def',1.1
```

## Usage
```
rjoin(left_array, right_array, [, 'setting=value` [,...]])
```
Where:
 * left and right array could be any SciDB arrays or operator outputs
 * `left_ids=a,~b,c`: 0-based dimension or attribute numbers to use as join keys from the left array; dimensions prefaced with `~`
 * `right_ids=d,e,f`: 0-based dimension or attribute numbers to use as join keys from the right array; dimensions prefaced with `~`
 * `chunk_size=S`: for the output
 * `keep_dimensions=0/1`: 1 if the output should contain all the input dimensions, converted to attributes. 0 is default, meaning dimensions are only retained if they are join keys.
 * `algorithm=name`: a hard override on how to perform the join, currently supported values are `left_to_right` and `right_to_left`
 
There should be as many left_ids as right_ids and they must match data types (dimensions are int64). The result is returned as:
`<join_key_0:type [NULL], join_key_1:.., left_att_0:type, left_att_1,... right_att_0...> [instance_id, value_no]`
Note the join keys are placed first, their names are inherited from the left array, they are nullable if nullable in any of the inputs (however NULL keys do not participate in the join). This is followed by remaining left attributes, then left dimensions if requested, then right attributes, then right dimensions.

The order of the returned result is indeterminate: will vary with algorithm and even number of instances.

## Memory Limitations
At the moment, one of the arrays must fit in memory on one node. The operator will try to pre-scan the data and figure out which it is. Then that array is sent around the cluster. More generic merge-based code path is coming.

## Still needs a lot of work!
 * implement the merge-join code path (using hash distribution, similar what https://github.com/Paradigm4/grouped_aggregate does)
 * fix the parameters: accept proper dimension references and names
 * pick join-on keys automatically by checking for matching names, if not supplied
 * handle cases where join expands the data, potentially add boolean expression to run on the data as join is computed

... something like that
