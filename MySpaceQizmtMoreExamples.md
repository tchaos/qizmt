<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# More Examples #


## K-Means Locator ##
<table>
<tr><td> <b>Obtain Code</b> </td> <td> Qizmt examples </td></tr>
<tr><td>  <b>View/Edit Code</b> </td> <td> Qizmt edit Qizmt-CellularKMeans.xml </td></tr>
<tr><td>  <b>Execute Code</b> </td> <td> Qizmt exec Qizmt-CelularKMeans.xml </td></tr>
</table>

This example calculates the K-Means of randomly generated vertices. The centroids are first randomly placed, during the map phase each centroids and vertices are rounded up to some range (Abstraction Sector) as the key. During the exchange phase all vertices and centroids are transferred between machines in the cluster such that all vertices within the same range are together. Within each range, the traditional k-means algorithm is applied to all sectors in parallel. As new data is added, this mapreducer only exchanges the delta vertices and the centroids.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_KMeansKey.png' alt='KMeansKey' />


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_KMeansInfo.png' alt='KMeansInfo' />


## K-Means Mapreduce Form ##

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_KMeansMRForm.png' alt='KMeansMRForm' />


## KMeans Example Map Logic ##

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_KMeansExampleMapLogic.png' alt='KMeansExampleMapLogic' />


## K-Means Example Reduce Logic ##

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_KMeansExampleReduceLogic.png' alt='KMeansExampleReduceLogic' />
