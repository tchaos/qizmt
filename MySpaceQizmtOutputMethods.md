<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Output Methods #


## Sorted ##

<table><tr valign='top'><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_OutputMethodSorted.png' alt='OutputMethodSorted' /> </td><td> The Sorted output method sorts the data across the entire cluster in consistent timing regardless of how skewed the keys are but incurs consistent 2X performance cost. This method is typically used when producing a very large sorted index at the end of a pipeline of mapreducers.</td></tr></table>


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_FoilSorted.png' alt='FoilSorted' />


### Ascending/Descending sorted ###
By default, keys are sorted in byte-ascending order. To sort keys in byte-descending order, add the output direction tag to the mapreducer:
` <OutputDirection>descending</OutputDirection> `


### Text Index Creation ###
Instead of outputting back into MR.DFS in reduce, you can output data directly to the local file system or to a third party application local on each machine. When mapreducer complete, peek at the first line of each output file of each machine to construct a master index showing what range is covered by each file across the cluster.


### Ranged Foil Sorted ###
` <OutputMethod>rsorted</OutputMethod> `
First range goes to reducer of first core of first machine of cluster; second range goes to second core of first machine in cluster, etc. Each core gets a range, and each machine also has a super-range.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_RangedFoilSorted.png' alt='RangedFoilSorted' />


### Round Robin Foil Sorted ###
` <OutputMethod>fsorted</OutputMethod> `
First range goes to reducer of first core of first machine of cluster; second range goes to first core of second machine in cluster, etc. Each core gets a range, but each machine has multiple ranges which are not sequential.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_RoundRobinFoilSorted.png' alt='RoundRobinFoilSorted' />


## Grouped ##

<table><tr valign='top'><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_OutputMethodGrouped.png' alt='OutputMethodGrouped' /> </td><td> The default template uses the grouped sort algorithm. This output method grantees that all identical keys make their way to the same reducer, but does not produce a fully sorted index across the cluster. This output method is immune to skewing. </td></tr></table>


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Grouped.png' alt='Grouped' />


## Hash Sorted ##

<table><tr valign='top'><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_OutputMethodHashSorted.png' alt='OutputMethodHashSorted' /> </td><td> Hashsorted is the fastest output method for sorting data across a cluster but is not immune to skewing in the keys and only 2 bytes of the key may be considered for the exchange phase. <code>&lt;KeyMajor&gt;</code> must be set to 2 when using hashsorted. When using hashsorted, place the least redundant 2 bytes of the key in the first two bytes. Left padded string keys, for example, cannot be hashsorted. Hashsorted performs exceptionally well when keys are completely random. </td></tr></table>


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_HashSorted.png' alt='HashSorted' />
