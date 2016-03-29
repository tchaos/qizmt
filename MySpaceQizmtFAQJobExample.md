<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Jobs / Examples #

## How do I create a new job in MR.DFS? ##

---

To create a job in MR.DFS, RDP into any machine of the cluster and issue the edit with the name of the jobs file you wish to create.<br />
e.g. `qizmt edit dfs://myjob.xml`

If a jobs file by the same name already exists, then the editor will open it for editing, however, if it does not exist, the editor will create an example job which includes 3 major job types: local, remote and mapreduce.

## Where is the word count example? ##

---

Generate the built-in Qizmt examples by issuing the command: **qizmit examples**
<br />e.g.
`qizmit examples`
> Qizmt exec Qizmt-GroupBy.xml<br />
> Qizmt exec Qizmt-WordCount.xml<br />
> Qizmt exec Qizmt-CellularKMeans.xml <br />
> Qizmt exec Qizmt-SortInt.xml<br />
> Qizmt exec Qizmt-Pointers.xml<br />
> Qizmt exec Qizmt-Linq.xml <br />
> Qizmt exec Qizmt-Geometry.xml<br />

All of the examples are self-contained; they all generate their own inputs and verify their own outputs and can be executed without any preparation.
<br />

&lt;Br/&gt;


The examples can be viewed with the **edit** command.
<br />
e.g.<br />
`qizmt edit Qizmt-Linq.xml`

## What is the easiest way to quickly prototype mapreducer logic? ##

---

  1. Use the edit command to create a new jobs file
> > e.g. `qizmt edit dfs://myjob.xml`
  1. Modify the lines of the first remote job to some sample data of what your input will look like
  1. Modify the map and reduce logic
  1. Save, Close and Execute the jobs file, or execute it in the debugger with F5