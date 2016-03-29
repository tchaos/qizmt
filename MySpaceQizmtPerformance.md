<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Performance Tuning #

## After adding more machines to my cluster mapreduce exuction is not speeding up for me. ##

---


This is usually caused by adding more servers without adding more non-blocking IO. We recommend at least 1Gb/s of non blocking IO per machine in the cluster of 8-core machines and 2Gb/s non blocking IO per machine in a cluster of 16-core machines. Every machine in the cluster should be able to max out its network card’s bandwidth concurrently between all machines in the cluster.

## For some jobs the map phase is slower then the reduce phase. ##

---


The most common scenario that this occurs is when the data is very small and there is processing intensive logic in the mapper. MR.DFS files are broken up into 64MB parts by default when they are put into MR.DFS. If the input data contains only one or a few 64MB parts then only a few mapper will execute on the data. The easiest remedy for this is to re-spread the data by running it through a mapreducer using the values only and assigning a random Int64 to the key. Then once the data is re-spread, run the resulting MR.DFS file into the mapreducer with processing intensive mapper logic.

## For some jobs the reduce phase is slower then the map phase. ##

---


This is usually the case when the mapper is sending the same key for each tuple of input data. All like-keys will go to the same reducer. The more data is divided up across unique keys, the more the computation can be parallelized in a single mapreducer. This may also occur if:
  * there is processing intensive logic in the reducer which executes only for a particular key/value.
  * the reducer expands the key value pairs into combinations or otherwise makes the data larger.
  * the business logic in the reducer is more computationally expensive then the map logic.