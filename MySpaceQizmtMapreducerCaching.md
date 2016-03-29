<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Mapreducer Caching #

<table>
<tr><td> <b>Obtain Code</b> </td><td> Qizmt examples </td></tr>
<tr><td> <b>View/Edit Code</b> </td><td> Qizmt edit Qizmt-ExplicitCacheWordCount.xml </td></tr>
<tr><td> <b>Execute Code</b> </td><td> Qizmt exec Qizmt-ExplicitCacheWordCount.xml </td></tr>
</table>

Most mapreducer jobs can be executed faster in subsequent executions by caching intermediate data of a mapreducer job and only exchanging/sorting the delta input data. In this way only MR.DFS files that have not been picked up yet flow throughout the cluster during a mapreduce job rather than the entire input set. Typically, for production pipelines can be implemented with smaller clusters of servers than R&D clusters when Qizmt Mapreducer Caching is used.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MapReduceCaching.png' alt='MapReduceCaching' />


## Enabling Caching ##

Caching is enabled by adding a `<Delta>` node under the `<Job>` node of a mapreducer job, e.g.
```
      <Delta>
        <Name>*ExplicitCacheWordCount_Cache*</Name>
        <DFSInput>*dfs://ExplicitCacheWordCount_Input*.txt*</DFSInput>
      </Delta>
```
In this example, after the mapreducer job is run the first time, a file will be created in MR.DFS per the **/Delta/Name** node containing the distributed cached intermediate data. e.g.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MRDFSCacheFile.png' alt='MRDFSCacheFile' />


## Explicit Caching ##

By default, all key/value pairs are cached when delta caching is enabled. In some scenarios you may want to override the contents of the cached intermediate data across the cluster. This can be done by setting the **`[ExplicitCache]`** function attribute on the reducer event and then explicitly adding key/value pairs to the cache. When this is done, the automatic cache is replaced with an explicit cache e.g.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_ExplicitCacheAttribute.png' alt='ExplicitCacheAttribute' />

All key/value pairs sent into the output.Cache() will make their way back into this reducer after being merged with delta key/value pairs in subsequent runs of the mapreducer. Files are pulled into the cache by the name of the file in MR.DFS. Qizmt keeps track of which MR.DFS files have been pulled into the mapreducer per the wildcard specified in the **/Delta/DFSInput** node, once they have been cached, the names of the MR.DFS files are stored in the cache file to avoid re-caching them in subsequent runs.


## Viewing what MR.DFS Files have been Rolled into a Cache File ##
The **Qizmt info** command, when applied to a cache file (aqua), shows which MR.DFS files have already been picked up by a mapreducer using the cache file.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MRDFSCacheFileViewInputs.png' alt='MRDFSCacheFileViewInputs' />


## Invalidating MR.DFS Files Already in a Cache ##

MR.DFS files already in a cache file can be removed so that they will be re-cached on the next run of the mapreducer using the cache e.g.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MRDFSCacheFileInvalidate.png' alt='MRDFSCacheFileInvalidate' />

Once a MR.DFS file has been invalidated from a cache file, the mapreducer using the cache file will pick it up along with any other files which match the wild card specified in the **/Delta/DFSInput**  node. Invalidating an MR.DFS file in a cache file does not clean up any data which that file may have input as it is already merged with inputs from all other cached MR.DFS files, however it does make the mapreducer using the cache re-pull that file as an input the next time it is executed.


## Deleting a Cache File ##

Deleting a cache file will both invalidate all cached inputs as well as delete all intermediate data cached by that cache file. When the mapreducer job which owns a deleted cache file is executed, all inputs picked up by the **/Delta/DFSInput** wildcard are re-exchanged across the cluster and the intermediate data is re-cached.


## Limitations of Qizmt Caching ##

  * Cache files cannot be shared across different mapreducers, a mapreducer owns at most 1 cache file and a cache file is owned by at most one mapreducer.
  * If any administrative action is taken on a cluster such as adding or removing machines, all cache files must be deleted.
  * If replication is > 1, caching is ok but cached intermediate data itself is not replicated.
