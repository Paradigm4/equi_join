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
* `hash_join_threshold=MB`: a threshold on the array size used to choose the algorithm; see next section for details; defaults to the `merge-sort-buffer` config
* `algorithm=name`: a hard override on how to perform the join, currently supported values are below; see next section for details
  * `hash_replicate_left`: hash join replicating the left array
  * `hash_replicate_right`: hash join replicating the right array
  * `merge_left_first`: merge join redistributing the left array first
  * `merge_right_first`: merge join redistributing the right array first
 
There should be as many left_ids as right_ids and they must match data types (dimensions are int64). The result is returned as:
`<join_key_0:type [NULL], join_key_1:.., left_att_0:type, left_att_1,... right_att_0...> [instance_id, value_no]`
Note the join keys are placed first, their names are inherited from the left array, they are nullable if nullable in any of the inputs (however NULL keys do not participate in the join). This is followed by remaining left attributes, then left dimensions if requested, then right attributes, then right dimensions.

The order of the returned result is indeterminate: will vary with algorithm and even number of instances.

## Algorithms
The operator first estimates the lower bound sizes of the two input arrays and then, absent a user override, picks an algorithm based on those sizes.

### Size Estimation
It is easy to determine if an input array is materialized (leaf of a query or output of a materializing operator). If this is the case, the exact size of the array can be determined very quickly (O of number of chunks with no disk scans). Otherwise, the operator initiates a pre-scan of just the Empty Tag attribute to find the number of non-empty cells in the array. The pre-scan continues until either completion, or the estimated size reaching `hash_join_threshold`. The per-instance lower bounds of `hash_join_threshold` or less are then added together with one round of message exchange.

### Hash Replicate
If it is determined (or user-dictated) that one of the arrays is small enough to fit in memory on every instance, then that array is copied entirely to every instance and loaded into an in-memory hash table. The other array is then read as-is and joined via hash table lookup. TBD: filter the chunks of the other array.

### Merge
If both arrays are sufficiently large, the smaller array's join keys are hashed and the hash is used to redistribute it such that each instance gets roughly an equal portion. The other array is then redistributed along the same hash, ensuring co-location. Finally the redistributed arrays are sorted and joined using a merge sort pass. TBD: also filter chunks of the other array and potentially add a bloom filter.

## Still needs a lot of work!
 * implement chunk filtering when joining by dimensions, possibly bloom filter for merge path
 * fix the parameters: accept decent looking dimension references and names
 * pick join-on keys automatically by checking for matching names, if not supplied
 * handle cases where join expands the data, potentially add boolean expression to run on the data as join is computed

... something like that
