<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Job Types #


## `Local` ##

Method executes on 1 machine. The Shell() method is often used in Locals to script a set of tasks in a pipeline, input deltas and publish results. **Example:**

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Local.png' alt='Local' />

> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_LocalDiagram.png' alt='LocalDiagram' />


## `Remote` ##

Method executes on 1 or more remote machines to perform a task on a MR.DFS input file or wild card set of files. The resulting stream writes data back out to the DFS. Use this type of job for algorithms which are more cost-effectively executed on a single machine using top-down approach, such as Top-Down Collaborative Filtering. **Example:**

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Remote.png' alt='Remote' />

> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_RemoteDiagram.png' alt='RemoteDiagram' />


## `MapReduce` ##

Most data processing tasks may be performed optimally and with ultra-simplistic code when written as a mapreducer. **Example:**

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MapReduce.png' alt='MapReduce' />

> <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MapReduceDiagram.png' alt='MapReduceDiagram' />