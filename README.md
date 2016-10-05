# equi_join
Relational-style Equi-Join of SciDB Arrays by Attributes or Dimensions. The objective of the operator is to make joining of various diverse datasets easier and more performant. In the future, SciDB will have a more sophisticated query optimizer to determine join strategies. For now, the choice of strategy is done locally in the operator, using some array pre-scanning if needed.

## Examples
We'll start with a couple toy arrays:
```
$ iquery -aq "store(apply(build(<a:string>[i=0:5,2,0], '[(null),(def),(ghi),(jkl),(mno)]', true), b, double(i)*1.1), left)"
{i} a,b
{0} null,0
{1} 'def',1.1
{2} 'ghi',2.2
{3} 'jkl',3.3
{4} 'mno',4.4

$ iquery -aq "store(apply(build(<c:string>[j=1:5,3,0], '[(def),(mno),(null),(def)]', true), d, j), right)"
{j} c,d
{1} 'def',1
{2} 'mno',2
{3} null,3
{4} 'def',4
```

Join the arrays on the string attribute, i.e. left.a=right.c:
```
$ iquery -aq "equi_join(left, right, 'left_names=a', 'right_names=c')"
{instance_id,value_no} a,b,d
{0,0} 'def',1.1,1
{0,1} 'mno',4.4,2
{1,0} 'def',1.1,4
```
Note "left.a" and "right.c" are combined into a single attribute "a" (name inherited from left). The other two attributes are "left.b" and "right.d".

Perform left, right or full outer joins. Note the order of returned results may vary:
```
$ iquery -aq "equi_join(left, right, 'left_names=a', 'right_names=c', 'left_outer=true')"
{instance_id,value_no} a,b,d
{0,0} null,0,null        
{0,1} 'def',1.1,1
{0,2} 'def',1.1,4
{1,0} 'ghi',2.2,null     
{1,1} 'jkl',3.3,null     
{2,0} 'mno',4.4,2

$ iquery -aq "equi_join(left, right, 'left_names=a', 'right_names=c', 'right_outer=true')"
{instance_id,value_no} a,b,d
{0,0} 'def',1.1,1
{0,1} 'mno',4.4,2
{0,2} null,null,3
{1,0} 'def',1.1,4

$ iquery -aq "equi_join(left, right, 'left_names=a', 'right_names=c', 'left_outer=true', 'right_outer=true')"
{instance_id,value_no} a,b,d
{1,0} 'mno',4.4,2
{2,0} null,0,null
{2,1} null,null,3
{2,2} 'def',1.1,1
{2,3} 'def',1.1,4
{3,0} 'ghi',2.2,null
{3,1} 'jkl',3.3,null
```

Join on two keys: left.i = right.d (dimension to attribute) and left.a=right.c. Note we can use zero-based dimension and attribute identifiers instead of names:
```
$ iquery -aq "equi_join(left, right, 'left_ids=~0,0', 'right_ids=1,0')"
{instance_id,value_no} i,a,b
{0,0} 1,'def',1.1
```

### Compared to the Existing cross_join

We make a large 2D dense array:
```
$ iquery -anq "store(build(<a:double> [x=1:10000,1000,0, y=1:10000,1000,0], random()), twod)"
Query was executed successfully
```

Here's how to use the current cross_join to extract a strip at x=128:
```
$ time iquery -naq "consume(cross_join(twod as A, redimension(build(<x:int64>[i=0:0,1,0], 128), <i:int64>[x=1:10000,1000,0]) as B, A.x, B.x))"
Query was executed successfully

real	0m0.381s
user	0m0.008s
sys	0m0.004s
```

With equi-join, note the redimension is no longer necessary and the performance is similar:
```
$ time iquery -naq "consume(equi_join(twod, build(<x:int64>[i=0:0,1,0], 128), 'left_names=x', 'right_names=x'))"
Query was executed successfully

real	0m0.315s
user	0m0.008s
sys	0m0.000s
```

Here, `equi_join` detects that the join is on dimensions and uses a chunk filter structure to prevent irrelevant chunks from being scanned. The above is a lucky case for `cross_join` - as the number of attributes increases, the advantage of `equi_join` gets bigger. If the join is on attributes, `cross_join` definitely cannot keep up. Moreover `cross_join` always replicates the right array, no matter how large, to every instance; this is often disastrous. `equi_join` will adapt well regardless of the order of arguments, in most cases.

One disadvantage at the moment is that `equi_join` is fully materializing. In a scenario such as:
```
equi_join(equi_join(A, B,..), C,..)
```
The inside operation is evaluated first, the output is materialized and then the outer operation is evaluated. This isn't always optimal. And in some cases nested `cross_join` can be faster. Hoping to make this more optimal soon.

At the moment, `cross_join` remains useful for the Full Cartesian Product use case, i.e.:
```
$ iquery -aq "cross_join(left, right)"
{i,j} a,b,c,d
{0,1} null,0,'def',1
{0,2} null,0,'mno',2
{0,3} null,0,'pqr',3
{1,1} 'def',1.1,'def',1
```
In cases like this, the inputs are usually small (because the output squares them). Here, preserving dimensionality is often useful and the approach of replicating one of the arrays is about the best one can do. 

## Usage
```
equi_join(left_array, right_array, [, 'setting=value` [,...]])
```
Where left and right array could be any SciDB arrays or operator outputs.

### Specifying join-on fields (keys)
* `left_names=a,b,c`: comma-separated dimension or attribute names from the left array
* `left_ids=a,~b,c`: 0-based dimension or attribute numbers from the left array; dimensions prefaced with `~`
* `right_names=d,e,f`: comma-seaparated dimension or attribute names from the right array
* `right_ids=d,e,f`: 0-based dimension or attribute numbers from the right array; dimensions prefaced with `~`

You can use either `names` or `ids` for either array. There must be an equal number of left and right keys and they must match data types; dimensions are int64. TBD: auto-detect fields with the same name if not specified.

### Outer joins
* `left_outer=0/1`: if set to `1` or `true` perform a left outer join: include all cells from the left array and populate with nulls where there are no corresponding cells in the right array
* `right_outer=0/1`: if set to `1` or `true` perform a right outer join: include all cells from the right array and populate with nulls where there are no corresponding cells in the left array

By default, both of these are set to false. Setting both `left_outer=true` and `right_outer=true` will result in a full outer join.

### Output names
If desired, user can set a list of output names to disambiguate:
* `out_names=a,b,c,...`
The number of provided tokens must match the number of attributes in the output (num left attrs + num right attrs - num join keys). By default, the names are copied from the input arrays, left array taking precedence for join keys.

### Additional filter on the output:
* `filter:expression` can be used to apply an additional filter to the result. 

Use any valid boolean expression over the output attributes. For example:
```
$ iquery -aq "equi_join(left, right, 'left_names=a', 'right_names=c', 'filter:b<d')"  
{instance_id,value_no} a,b,d
{0,0} 'def',1.1,4
```
Note, `equi_join(..., 'filter:expression')` is equivalent to `filter(equi_join(...), expression)` except the operator is materializing and the former will apply filtering prior to materialization. This is an efficiency improvement in cases where the join on keys increases the size of the data before filtering. If `out_names=` is set, then the expression will refer to the names provided in `out_names`. 

### Other settings:
* `chunk_size=S`: for the output
* `keep_dimensions=0/1`: 1 if the output should contain all the input dimensions, converted to attributes. 0 is default, meaning dimensions are only retained if they are join keys.
* `hash_join_threshold=MB`: a threshold on the array size used to choose the algorithm; see next section for details; defaults to the `merge-sort-buffer` config
* `bloom_filter_size=bits`: the size of the bloom filters to use, in units of bits; TBD: clean this up
* `algorithm=name`: a hard override on how to perform the join, currently supported values are below; see next section for details
  * `hash_replicate_left`: copy the entire left array to every instance and perform a hash join
  * `hash_replicate_right`: copy the entire right array to every instance and perform a hash join
  * `merge_left_first`: redistribute the left array by hash first, then perform either merge or hash join
  * `merge_right_first`: redistribute the right array by hash first, then perform either merge or hash join

### Result
Each array cell on the left is associated with 0 or more array cells on the right IFF all specified keys are equal respectively: `left_cell.key1 = right_cell.key1 AND left_cell.key2=right_cell.key2 AND ...` For inner joins, the output will contain one cell for each such association using all attributes from both arrays, plus dimensions if requested. Outer joins will include all cells from the input(s) as specified and use NULLs when a matching cell cannot be found in the opposite array. A cell where any of the join-on keys are NULL will not be associated with any tuples from the opposite array; so these cells will not be present unless the join is outer. The order of the returned result is indeterminate and will vary with algorithm and number of instances.

### Result Schema
The result is returned as:
`<join_key_0:type [NULL], join_key_1:.., left_att_0:type, left_att_1,... right_att_0...> [instance_id, value_no]`
Note the join keys are placed first, their names are assigned from the left array, they are nullable if nullable in either of the inputs. This is followed by remaining left attributes, then left dimensions if requested, then right attributes, then right dimensions.

Note that the result is "flattened" along `[instance_id, value_no]` in a matter similar to operators like `grouped_aggregate`, `sort`, `stream` and so on. Depending on the join keys, input chunking may or may not be easy to preserve and it may take extra work to preserve it. The needs to perform any redimensioning manually, post join.

## Installation

The easiest route is to use https://github.com/paradigm4/dev_tools Follow the instructions there to set up dev_tools first. Then do:
```
iquery -aq "install_github('paradigm4/equi_join')"
iquery -aq "load_library('equi_join')"
```

## Algorithms
The operator first estimates the lower bound sizes of the two input arrays and then, absent a user override, picks an algorithm based on those sizes.

### Size Estimation
It is easy to determine if an input array is materialized (leaf of a query or output of a materializing operator). If this is the case, the exact size of the array can be determined very quickly (O of number of chunks with no disk scans). Otherwise, the operator initiates a pre-scan of just the Empty Tag attribute to find the number of non-empty cells (count) in the array. The count, multiplied by the attribute sizes is used to estimate total size. The pre-scan continues until either end of array (at the local instance), or the estimated size reaching `hash_join_threshold`. Thus we ensure the pre-scan does not take too long. The per-instance pre-scan results then gathered together with one round of message exchange between instances.

### Replicate and Hash
If it is determined (or user-dictated) that one of the arrays is small enough to fit in memory on every instance, then that array is copied entirely to every instance and loaded into an in-memory hash table. The table is used to assemble a filter over the chunk positions in the other array. The other array is then read, using the filter to prevent disk scans for irrelevant chunks. Chunks that make it through the filter are joined using the hash table lookup.

### Merge
If both arrays are sufficiently large, the smaller array's join keys are hashed and the hash is used to redistribute it such that each instance gets roughly an equal portion. Concurrently, a filter over chunk positions and a bloom filter over the join keys are built. The chunk and bloom filters are copied to every instance. The second array is then read - using the filters to eliminate unnecessary chunks and values - and redistributed along the same hash, ensuring co-location. Now that both arrays are colocated and their exact sizes are known, the algorithm may decide to read one of them into a hash table (if small enough) or sort both and join via a pass over two sorted sets.

## Future work
 * make the operation not materializing when possible
 * pick join-on keys automatically by checking for matching names, if not supplied
 * better tuning for the Bloom Filter: choosing size and number of hash functions based on available memory
 * add the cross-product code path?
