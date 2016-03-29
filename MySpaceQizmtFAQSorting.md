<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Sorting #

## How to sort data with qizmt? ##

---

All mapreducer jobs inherently sort data across the cluster. However, there are a few OutputMethods which sort differently:

`<OutputMethod>grouped</OutputMethod>` - each core in the cluster gets a hash of the keys during the exchange. The data is not fully sorted across the cluster, but each cores hash of the keys are sorted so that all identical-keys make their way to the same reducer.

`<OutputMethod>sorted</OutputMethod>` - The mapper is executed twice over the input, first to determine distribution of the keys, padding, etc. then again to break the data up into key ranges. During the exchange phase, each machine gets an even distribution of the keys and they keys are evenly sorted across the cluster.  The data is fully sorted across the cluster when the sorted output method is used.

`<OutputMethod>hashsorted</OutputMethod>` - KeyMajor must be set to 2 to use hashsorted, a 2 byte hashtable of all possible values is used during the exchange phase. The data is fully sorted across the cluster when the hashsorted output method is used.



## How to do DESC or descending sort of mapreducer jobs? ##

---

This can be achieved by setting the OutputDirection to descending in IOSettings:
```
  <OutputMethod>sorted</OutputMethod>
  <OutputDirection>descending</OutputDirection>
</IOSettings> 
```