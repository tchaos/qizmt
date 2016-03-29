<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Remote Jobs #

## How to run multiple remote jobs on different machines processing different files in MR.DFS? ##

---

If just 1 remote with 1 DFS\_IO then it all goes to 1 machine, but keep in mind that bottlenecks bandwidth to 128 megabyte per second. If
multiple DFS\_IO or mapreducer then your total bandwidth is 128 megabyte per second `*` number of hosts.

Here is example of streaming 4 MR.DFS files to 4 processes. Two of the remotes running on MACHINEA and two of the remotes running on
MACHINEB.
```
<Job Name="multiremotes_InputData" Custodian="" Email="" Description="">
  <IOSettings>
    <JobType>remote</JobType>
    <DFS_IO>
      <DFSReader>dfs://File0.txt</DFSReader>
      <DFSWriter>dfs://File0_output.txt</DFSWriter>
      <Host>MACHINEA</Host>
    </DFS_IO>
    <DFS_IO>
      <DFSReader>dfs://File1.txt</DFSReader>
      <DFSWriter>dfs://File1_output.txt</DFSWriter>
      <Host>MACHINEA</Host>
    </DFS_IO>
    <DFS_IO>
      <DFSReader>dfs://File2.txt</DFSReader>
      <DFSWriter>dfs://File2_output.txt</DFSWriter>
      <Host>MACHINEB</Host>
    </DFS_IO>
    <DFS_IO>
      <DFSReader>dfs://File3.txt</DFSReader>
      <DFSWriter>dfs://File3_output.txt</DFSWriter>
      <Host>MACHINEB</Host>
    </DFS_IO>
  </IOSettings>
  <Remote>
  <![CDATA[
  public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
  {    
    System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
    //
  }
  ]]>
```

## What machine does a remote job execute on? ##

---

By default Qizmt determines what machine a remote job runs on, however this can be overridden explicitly by specifying `<Host>`  in the DFS\_IO section of the remote job.


If redundancy is enabled, then any job can execute on any machine if not explicitly specified.  Ideally can use network paths always so that it doesn’t matter what machine a job executes on. E.g. in the case of redundancy factor is > 1, all job types will failover to a good machine if a machine is inaccessible.

## How to execute a remote job on all machines or all cores without explicitly listing a series of `<DFS_IO>`s? ##

---

```
<DFS_IO_Multi>
  <DFSReader>dfs://Qizmt-RemoteMultiIO_Input.txt</DFSReader>
  <DFSWriter>dfs://Qizmt-RemoteMultiIO_Output1####.txt</DFSWriter>
  <Mode>ALL MACHINES</Mode>
</DFS_IO_Multi>
<DFS_IO_Multi>
  <DFSReader>dfs://Qizmt-RemoteMultiIO_Input.txt</DFSReader>
  <DFSWriter>dfs://Qizmt-RemoteMultiIO_Output2####.txt</DFSWriter>
  <Mode>ALL CORES</Mode>
</DFS_IO_Multi>
```

This way you don’t have to explicitly specify every machine or every core if you want a remote job to run everywhere.