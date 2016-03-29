<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)


# MySpace Qizmt Administrator Guide #


## Suggested Minimum Requirements per node ##

  * 8-way 2GHz cores
  * 32GB RAM
  * 1 Gb/s non-blocking network bandwidth. (Every machine should be able to max out its 1Gb/s to 100% at the same time)
  * 1TB – 4TB disk space per node in RAID 0 for installation drive
  * RAID 10 for OS drive
  * Windows 2003 SP2, Windows 2008, Windows Vista or Windows 7
  * .NET Framework 3.5 SP1

## Windows configuration settings to set per machine of cluster: ##

  * Note: The machines of your cluster should be on a private vlan and not allow untrusted connectivity.
  * Disable windows search indexing
  * dword decimal:
  * HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Services\LanManServer\Parameters\DisableDos 1
  * HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Control\Filesystem\NtfsDisableLastAccessUpdate 1
  * HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters\SynAttackProtect 0
  * HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters\MaxUserPort 15000
  * HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters\TcpTimedWaitDelay 5
  * HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters\TcpFinWait2Delay 5
  * HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters\MaxHashTableSize 16384

## Cluster Installation ##

  1. Install MySpace.DataMining.Qizmt.msi and any extensions on all machines.
> > - See **Quick Start** section for details on driver installation. Also, be sure that the account has read/write permission to `\\<host>\<drive>$\<installdir>\` from every machine to every other machine. This can be tested by logging in to every machine with the account or automated with PowerShell
  1. Pick one machine and issue the command: <br /> C:\>**`Qizmt  format Machines=<HOST0>,<HOST2>,<HOST2>,...`** <br />You may now RDP into any of the specified hosts and issue Qizmt commands to code/debug jobs, execute jobs, etc. <br /> For command help, issue: <br /> C:\>**Qizmt**


## Adding a New Machine to an Existing Cluster ##

  1. Install the Qizmt driver on the new machine along with any extensions, see section Single Machine Quick Start
  1. From any machine in the cluster, issue the command: **`Qizmt addmachine <hostname>`** <br /> Where `<hostname>` is the hostname of the new machine to add to the cluster
    * Data in cluster will automatically be redistribute
    * The new node will pull a subset of the data from the other machines
    * All machines in cluster will have a smaller portion of the data after a node has been added


## Removing Machine from a Cluster ##

  1. From any machine in cluster (including the machine to remove), issue command: **`Qizmt removemachine <hostname>`** <br /> Where `<hostname>` is the hostname of the machine to remove from the cluster
    * Data in cluster will automatically redistribute between the remaining machines
    * If the machine being removed has a hard drive failure, redundancy set in the @format command must be 2 or greater in order to complete the removal without loss of data.


## Uninstalling Qizmt Driver from Machine ##
  1. Remove the machine from any cluster that it may be part of see: _Removing Machine from a Cluster_
  1. Start->Control Panel
  1. Select _Add/Remove Programs_ or _Programs and Features_
  1. Double click on the Qizmt installation and select Uninstall


## Qizmt Command Line Usage ##

[Qizmt Command Line](MySpaceQizmtCommandLine.md)


## Borrow Machines from another Cluster ##

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_BorrowNodes.png' alt='BorrowNodes' />

As long as there is sufficient hard drive, machines may be borrowed from another cluster without affecting the contents each MR.DFS
  1. From Any Machine on Cluster A <br />  **Qizmt removemachine Machine2 && Qizmt removemachine Machine3 && Qizmt removemachine Machine4** <br />
  1. From Any Machine on Cluster B <br />  **Qizmt addmachine Machine2 && Qizmt addmachine Machine3 && Qizmt addmachine Machine4**


## Splitting a Cluster into Two Smaller Clusters ##

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SplitCluster.png' alt='SplitCluster' />

A cluster may be split into two clusters such that one cluster retains the entire MR.DFS and the new cluster gets a clean new MR.DFS
  1. From any machine in Cluster B <br /> **Qizmt removemachine Machine8 && Qizmt removemachine Machine9 && Qizmt removemachine Machine2 && Qizmt removemachine Machine3 && Qizmt removemachine Machine4** <br />
  1. From any removed machine <br /> **Qizmt format Machines=Machine8,Machine9,Machine2,Machine3,Machine4**


## Cluster Health ##

View the current health of the cluster. When this command is run any corrupted files, disconnected servers and other fault scenarios are detected.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Health.png' alt='Health' />


## Command History ##
View history of commands executed on the current cluster.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_History.png' alt='History' />


## Developers on Cluster ##
View a list of all users currently logged into all windows machines of the cluster.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Who.png' alt='Who' />


## Other Qizmt Administration Commands ##

<table>
<tr><td> <b>Command</b> </td><td> <b>Usage</b> </td></tr>

<tr valign='top'><td> <code>ps</code> </td><td> distributed processes, schedule and queue info </td></tr>
<tr valign='top'><td> <code>who</code> </td><td>show who is logged on  </td></tr>
<tr valign='top'><td> <code>history</code> </td><td>show command history  </td></tr>
<tr valign='top'><td> <code>killall</code> </td><td> kill all jobs, clean any orphaned intermediate data </td></tr>
<tr valign='top'><td> <code>info [&lt;dfspath&gt;[:&lt;host&gt;]]</code>        </td><td> information for MR.DFS or a MR.DFS file </td></tr>
<tr valign='top'><td> <code>getjobs \\&lt;netpath&gt;</code> </td><td> Archive all jobs from MR.DFS to a file on a network path. </td></tr>
<tr valign='top'><td> <code>putjobs \\&lt;net path&gt;</code> </td><td> Put all jobs from an archive into MR.DFS. Jobs by the same name will be skipped. </td></tr>
<tr valign='top'><td> <code>examples</code> </td><td> generate example jobs source code </td></tr>
<tr valign='top'><td> <code>importdir</code> </td><td> import jobs from into MR.DFS </td></tr>
<tr valign='top'><td> <code>listinstalldir</code> </td><td> List all installed directories </td></tr>
<tr valign='top'><td> <code>exechistory</code> </td><td> List the most recent executed commands </td></tr>
<tr valign='top'><td> <code>harddrivespeedtest &lt;/td&gt;&lt;td&gt; [&lt;filesize&gt;]</code></td><td> Test write/read hard drive speed </td></tr>
<tr valign='top'><td> <code>networkspeedtest &lt;/td&gt;&lt;td&gt; [&lt;filesize&gt;]</code></td><td> Test upload/download network speed test </td></tr>
<tr valign='top'><td> <code>cputemp</code> </td><td> List cpu temperatures </td></tr>
<tr valign='top'><td> <code>ghost</code> </td><td> List intermediate data leaked from when a job was aborted early across the cluster and is marked for deletion on next killall </td></tr>
<tr valign='top'><td> <code>perfmon</code> </td><td> <div><code>&lt;network|cputime|diskio|availablememory&gt;</code></div>
<blockquote><div>  <code>  [a=&lt;Number of readings to get.  Return average.&gt;]</code></div>
<div>  <code>  [t=&lt;Number of threads&gt;]</code></div>
<div>  <code>  [s=&lt;Milliseconds of sleep to take between readings&gt;]</code></div>
<div>get Perfmon counter readings</div></blockquote>

<div><code>generic</code></div>
<blockquote><div>  <code>  o=&lt;Object/category name&gt;</code></div>
<div>  <code>  c=&lt;Counter name&gt;</code></div>
<div>  <code>  i=&lt;Instance Name&gt;</code></div>
<div>  <code>  [f Display readings in friendly byte size units]</code></div>
<div>  <code>  [a=&lt;Number of readings to get.  Return average.&gt;]</code></div>
<div>  <code>  [t=&lt;Number of threads&gt;]</code></div>
<div>  <code>  [s=&lt;Milliseconds of sleep to take between readings&gt;]</code></div>
<div>specify a Perfmon counter to read</div>
</td></tr>
<tr valign='top'><td> <code>packetsniff</code> </td><td>
<div><code>[t=&lt;Number of threads&gt;]</code></div>
<div><code>[s=&lt;Milliseconds to sniff&gt;]</code></div>
<div><code>[v verbose]</code></div>
<div><code>[a include non-cluster machines]</code></div>
<div>Sniff packets</div>
</td></tr>
<tr valign='top'><td> <code>md5 &lt;dfsfile&gt;</code> </td><td> compute MD5 of DFS data file </td></tr>
<tr valign='top'><td> <code>checksum &lt;dfsfile&gt;</code> </td><td> compute sum of DFS data file </td></tr>
<tr valign='top'><td> <code>sorted &lt;dfsfile&gt;</code> </td><td> check if a DFS data file has sorted lines in ascending big endian byte sorted </td></tr>
<tr valign='top'><td> <code>nearprime &lt;positiveNum&gt;</code> </td><td> find the nearest prime number </td></tr>
<tr valign='top'><td> <code>genhostnames &lt;pattern&gt; &lt;startNum&gt; &lt;endNum&gt; [&lt;delimiter&gt;]</code>
generate host names </td><td> generate a list of hostnames using a pattern such as FOOBAR#### </td></tr>
<tr valign='top'><td> <code>viewlog</code> </td><td>
<div><code>[machine=&lt;machineName&gt;]</code></div>
<div><code>[count=&lt;number of entries to return&gt;]</code></div>
<div>view log entries</div>
<div>If you suspect that there is an error or a job is taking too long. This command can be used to view any errors that may have occurred even if the current running job or jobs have not yet completed execution.</div>
</td></tr>
<tr valign='top'><td> <code>stresstests</code> </td><td> <div>Generate a series of mapreducer jobs which may be executed to stresstest the capabilities of the current Qizmt cluster.</div>
</blockquote><blockquote><div>    <code>Qizmt exec grouped_10GB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec hashsorted_10GB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec sorted_10GB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec sorted_POS_10GB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec grouped_1TB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec hashsorted_1TB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec sorted_1TB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec sorted_POS_1TB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec grouped_5TB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec hashsorted_5TB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec sorted_5TB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec sorted_POS_5TB_Of_100_Byte_Rows.xml</code></div>
<div>    <code>Qizmt exec sortTestsDriver.xml [s = run tests]</code></div>
<div>    <code>Qizmt exec sortTestsDriver.xml [sv = run tests and verify results]</code></div>
<div>    <code>Qizmt exec sortTestsDriver.xml [v = verify results]</code></div>
</td></tr></blockquote>

<tr valign='top'><td> <code>enqueue</code> </td><td>
<div><code>command=&lt;value&gt;</code></div>
<div><code>[ExecTimeout=&lt;secs&gt; Maximum seconds Qizmt exec can run</code></div>
<div><code>OnTimeout=&lt;tcmd&gt;] Run on timeout; e.g.  Qizmt kill #JID#</code></div>
<div>Adds a command to the end of the queue</div>
</td></tr>
<tr valign='top'><td> <code>queuekill</code> </td><td>
<div><code>&lt;QueueID&gt;</code></div>
<div>Removes the specified Queue Identifier</div>
</td></tr>
<tr valign='top'><td> <code>clearqueue</code> </td><td>
<div>Removes all entries from the queue</div>
</td></tr>
<tr valign='top'><td> <code>schedule</code> </td><td>
<div><code>command=&lt;value&gt;</code></div>
<div><code>start=&lt;now|&lt;datetime&gt;&gt;</code></div>
<div><code>[frequency=&lt;seconds&gt;]</code></div>
<div><code>[texceptions=&lt;&lt;datetime&gt;[-&lt;datetime&gt;]&gt;[,...]] ranges when not to run</code></div>
<div><code>[wexceptions=&lt;weekday&gt;[,...]] whole weekdays not to run</code></div>
<div><code>[wtexceptions=&lt;wdtime&gt;[,...]] time on day-of-week not to run</code></div>
<div>adds a command entry to the scheduler</div>
<div>(datetime format is <code>[M[/D[/Y]].][h:m[:s][AM|PM]]</code>)</div>
<div>(wdtime format is <code>&lt;weekday&gt;@&lt;h:m[:s][AM|PM]&gt;[-&lt;h:m[:s][AM|PM]&gt;]</code>)</div>
</td></tr>
<tr valign='top'><td> <code>pauseschedule</code> </td><td>
<div><code>&lt;ScheduleID&gt;</code></div>
<div>Pauses the specified Schedule Identifier</div>
</td></tr>
<tr valign='top'><td> <code>unpauseschedule</code> </td><td>
<div><code>&lt;ScheduleID&gt;</code></div>
<div>Un-pauses the specified Schedule Identifier</div>
</td></tr>
<tr valign='top'><td> <code>unschedule</code> </td><td>
<div><code>&lt;ScheduleID&gt;</code></div>
<div>Removes the specified Schedule Identifier</div>
</td></tr>
<tr valign='top'><td> <code>clearschedule</code> </td><td>
<div>Removes all entries from the scheduler</div>
</td></tr>

</table>


# Data Replication #
Clusters can have replication level up to the number of (machines / 2) -1. The higher the replication level, the more machines may have hard drive or network failures without loss of data in the cluster. The number of machines which may concurrently fail without loss of data is (replication level – 1). Higher replication levels cause the MR.DFS free disk space to drop. The cluster disk space in a cluster is dropped to (physical disk space / replication level).

<table>
<tr><td> <b>Replication Level</b> </td><td> <b>Allowed Concurrent Failures</b> </td><td> <b>Cluster Disk Space</b> </td></tr>
<tr><td> 1 </td><td> 0 </td><td> 100% </td></tr>
<tr><td> 2 </td><td> 1 </td><td> 50% </td></tr>
<tr><td> 3 </td><td> 2 </td><td> 33% </td></tr>
</table>

Here are a few commands for viewing and managing the replication of a cluster:
<table>
<tr><td> <b>Command</b> </td><td> <b>Description</b> </td></tr>
<tr><td> <code>Qizmt replicationview</code> </td><td> view the replication level of the current cluster </td></tr>
<tr><td> <code>Qizmt replicationupdate &lt;level&gt;</code> </td><td> increase or decrease the replication level of the current cluster </td></tr>
</table>

Data replication is useful for:
  1. lusters that need reliability
  1. lusters used for data warehousing
  1. xtremely large clusters, even if used for transient data as the probability of failures increases with cluster size and age of hard drives. (the older the hard drives, the higher probability of a disk failure)


## MR.DFS Chunks and MR.DFS Parts ##
Each MR.DFS file is broken up into one or more parts. However, with replication enabled, each part consists of multiple redundant copies called chunks. By default files in a cluster are broken up into 64MB parts, so if a file has 320MB of data, it will be broken up into 5 parts across the MR.DFS. The number of chunks for an MR.DFS file is (parts **replication level)**

<table>
<tr><td> <b>MR.DFS File Size</b> </td><td> <b>Parts</b> </td><td> <b>Replication Level</b> </td><td> <b>Chunks</b></td></tr>
<tr><td> 320MB </td><td> 5 </td><td> 1 </td><td> 5 </td></tr>
<tr><td> 320MB </td><td> 5 </td><td> 2 </td><td> 10 </td></tr>
<tr><td> 320MB </td><td> 5 </td><td> 3 </td><td> 15 </td></tr>
</table>


## Formatting Cluster with Data Replication ##

If a cluster is formatted with replication, then machines may fail without loss of data and the cluster is recovered by removing the machine with disk failure or network failure. Both network failure and disk failure are the most likely failures in a cluster. Permissions issues and many other possible causes of failure are not used to chop a machine out of a cluster to avoid over-copping from occurring and should be addressed by solving the problem causing loss of connectivity. When redundancy is enabled or greater then 1, **Qizmt put** and **Qizmt exec** exclude failed machines at the start of execution and display a warning message listing any machines excluded due to failures. The failed machines, however, are not removed from the cluster until **Qizmt removemachine** is executed. It is when **Qizmt removemachine** is executed on a machine with disk or network failure, the remaining machines re-distribute file chunks evenly across the cluster.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MRDFSReplication.png' alt='MRDFSReplication' />


## Increasing and Decreasing Replication ##

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MRDFSReplicationChunks.png' alt='MRDFSReplicationChunks' />

A cluster that already has in its MR.DFS can still have its replication decreased or increased. For example, if a cluster has replication level of two and a machine is added, a small amount of data chunks on the other machines will move to the newly added machine until all machines have an even spread of the data in the MR.DFS.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MRDFSReplicationChunksAdd1.png' alt='MRDFSReplicationChunksAdd1' />

If the replication level is then decreased, data chunks are removed from machines across the cluster such that the new replication level is maintained and the data remains evenly spread across the cluster.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MRDFSReplicationChunksRemove1.png' alt='MRDFSReplicationChunksRemove1' />


## Cluster without Replication ##

When a cluster is formatted, if no replication is specified e.g. set to the value of 1, then the cluster will split files up across the cluster without redundancy. If a machine is lost, the data chunks on the lost-machine are permanently lost. If a machine is still available but is removed, then the data chunks on the machine-to-remove will be redistributed across the rest of the machines. Having no replication has performance gains, as the replication phase of **Qizmt put** and **Qizmt exec** incur extra bandwidth cost to make redundant copies of MR.DFS file chunks. It is ideal to use clusters that have no replication for:
  1. small clusters which are not warehousing data but do transient processing and the time to reload data after a failure is insignificant
  1. single machine desktop installations used for developing mapreducers offline
  1. small clusters which have RAID 5 or RAID 10 hard drives (this only works well for small clusters)
  1. large clusters used to do performance benchmarking/competitive sorting competitions

For example, if we issue `Qizmt put \\SERVERX\fruit_*.txt` each machine gets a disjoint subset of all the `fruit_*.txt` files picked up by the **`Qizmt put`** command.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MRDFSChunksNoReplication.png' alt='MRDFSChunksNoReplication' />