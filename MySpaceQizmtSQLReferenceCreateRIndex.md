<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Reference](MySpaceQizmtSQLReference.md)


# `CREATE RINDEX` #

```
CREATE RINDEX <indexName> 
       FROM <tableName> 
       [pinMemory | pinMemoryHash | diskonly] 
       [keepValueOrder] 
       [outlier <delete | random | fifo> <max> ] 
       ON <KeyColumnName>
```


Create an RINDEX for the specified table. This command requires the table to already be sorted across the cluster on KeyColumnName. This is usefully when you need to making low latency queries and updates on a very large amount of data without incurring mapreducer overhead.


RINDEX with PINMEMORYHASH are often used for large scale graph processing as they allow very low latency read/write to a large distributed memory source.


| **Option** | **When to use** |
|:-----------|:----------------|
| PINMEMORYHASH | When portions of a distributed table are accessed they remain in memory until the RINDEX is dropped. This is enabled by default. |
| DISKONLY   | The table is not cached in memory; heavy disk access every time a query is performed. |
| KEEPVALUEORDER | Rows returned are in the order they were inserted into the table. By default this order is not preserved. |
| OUTLIER DELETE | During BULK UPDATE, any tuples of the same key with a quantity exceeding the limit will be deleted. |
| OUTLIER RANDOM | During BULK UPDATE, any tuples of the same key with a quantity exceeding the limit will be randomly deleted until quantity matches the maximum. |
| OUTLIER FIFO | During BULK UPDATE, any tuples of the same key with a quantity exceeding the limit will be deleted (older tuples deleted, new tuples kept) until quantity matches the maximum. |




### Example ###

```
CREATE RINDEX idxGraph FROM tblGraph ON leftID;
```




