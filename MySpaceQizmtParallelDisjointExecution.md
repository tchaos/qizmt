<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Parallel Disjoint Execution #

Sometimes it is beneficial to stream the same MR.DFS file through every machine or a process on every core in the cluster. Parallel disjoint execution is typically useful when data is small enough to stream all data through one or more machines and perform hash based logic when this approach is more cost-effective them mapreduce.


## Remote Job Multiple Hosts ##
A remote job may be run on multiple explicitly specified hosts and even process a different MR.DFS file on each host. The `<Host>` tag is optional, if not specified, the remote jobs will live round robin across the cluster.

<table><tr valign='top'><td>
<pre><code>    &lt;IOSettings&gt;<br>
        &lt;JobType&gt;remote&lt;/JobType&gt;        <br>
        &lt;DFS_IO&gt;<br>
          &lt;DFSReader&gt;dfs://Input1.txt&lt;/DFSReader&gt;<br>
          &lt;DFSWriter&gt;dfs://Output1.txt&lt;/DFSWriter&gt;<br>
          &lt;Host&gt;Machine0&lt;/Host&gt;<br>
        &lt;/DFS_IO&gt;       <br>
        &lt;DFS_IO&gt;<br>
          &lt;DFSReader&gt;dfs://Input2.txt&lt;/DFSReader&gt;<br>
          &lt;DFSWriter&gt;dfs://Output2.txt&lt;/DFSWriter&gt;<br>
           &lt;Host&gt;Machine1&lt;/Host&gt;<br>
        &lt;/DFS_IO&gt;<br>
      &lt;/IOSettings&gt;<br>
</code></pre>
</td><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_RemoteMultipleHosts.png' alt='RemoteMultipleHosts' /> </td></tr></table>


## Remote Job All Machines ##
A remote job may be run on one core per machine; each remote sub process will get an identical copy of the same document streamed in from MR.DFS. The # wildcard will be replaced with a numeric DPID unique across the cluster.

<table><tr valign='top'><td>
<pre><code>      &lt;IOSettings&gt;<br>
        &lt;JobType&gt;remote&lt;/JobType&gt;        <br>
        &lt;DFS_IO_Multi&gt;<br>
          &lt;DFSReader&gt;dfs://Input.txt&lt;/DFSReader&gt;<br>
          &lt;DFSWriter&gt;dfs://Output####.txt&lt;/DFSWriter&gt;<br>
          &lt;Mode&gt;ALL MACHINES&lt;/Mode&gt;<br>
        &lt;/DFS_IO_Multi&gt;<br>
      &lt;/IOSettings&gt;<br>
</code></pre>
</td><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_RemoteAllMachines.png' alt='RemoteAllMachines' /> </td></tr></table>


## Remote Job All Cores ##
A remote job may be run on every core in the cluster without explicitly specifying host names, each remote will get an identical copy of the same document streamed from MR.DFS. The # wildcard will be replaced with a numeric DPID unique across the cluster.

<table><tr valign='top'><td>
<pre><code>     &lt;IOSettings&gt;<br>
        &lt;JobType&gt;remote&lt;/JobType&gt;        <br>
        &lt;DFS_IO_Multi&gt;<br>
          &lt;DFSReader&gt;dfs://Input.txt&lt;/DFSReader&gt;<br>
          &lt;DFSWriter&gt;dfs://Output####.txt&lt;/DFSWriter&gt;<br>
          &lt;Mode&gt;ALL CORES&lt;/Mode&gt;<br>
        &lt;/DFS_IO_Multi&gt;<br>
      &lt;/IOSettings&gt;<br>
</code></pre>
</td><td> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_RemoteAllCores.png' alt='RemoteAllCores' /> </td></tr></table>
