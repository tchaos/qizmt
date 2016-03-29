<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md)


# System Tables #

_Sys.Tables_ contains a list of all of the user tables along with the underlying _rectangular binary file_ in _MR.DFS_. When data developers are communicating with mapreduce developers, this can be used to locate the underlying data of SQL tables for mapreduce processing.


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_SysTables.png' alt='Sys.Tables' />


The rest of the System Tables may be used to perform diagnostics on the cluster.

| MRDFS.Help | Information and usage on all Qizmt commands. |
|:-----------|:---------------------------------------------|
| MRDFS.Users | Users currently logged into the machines of the cluster. |
| MRDFS.CPU  | Perfmon statistics about the CPU across the cluster. |
| MRDFS.DiskIO | Perfmon statistics about the disk IO across the cluster. |
| MRDFS.Network | Perfmon statistics about the network bandwidth use across the cluster. |
| MRDFS.Memory | Perfmon statistics about the available memory across the cluster. |
| MRDFS.DistributedFiles | List of files in the underlying MR.DFS       |
| MRDFS.RunningJobs | List of jobs currently running on the cluster. |
| MRDFS.History | History of commands executed on the cluster. |
| MRDFS.Info | Hard disk usage / free space per machine in the cluster. |
| MRDFS.Health | Percent health of the cluster and listing of all machines in cluster with disk failures. |
| MRDFS.InstallDir | Installation directory of Qizmt for every machine in the cluster. |
| MRDFS.Temprature | Physical temperature of every machine in the cluster. |
| MRDFS.Ghost | List intermediate cache data in the cluster which is leaked from a job being aborted early. |
| MRDFS.Log  | Concatenation of error logs from every machine in the cluster. |