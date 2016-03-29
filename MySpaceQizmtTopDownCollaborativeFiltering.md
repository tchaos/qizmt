<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Top-Down Collaborative Filtering #

<table>
<tr><td> <b>Obtain Code</b> </td><td> Qizmt examples </td></tr>
<tr><td> <b>View/Edit Code</b> </td><td> Qizmt edit Qizmt-CollaborativeFiltering.xml </td></tr>
<tr><td> <b>Execute Code</b> </td><td> Qizmt exec Qizmt-CollaborativeFiltering.xml </td></tr>
</table>

In this example, remote jobs are used to copy a MR.DFS document to every node in the MR.DFS and then perform Top-Down Collaborative Filtering on such that each core in the cluster does a subset of the work. The resulting scores are then redistributed back out into the MR.DFS.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_CollaborativeFilteringExample.png' alt='CollaborativeFilteringExample' />
