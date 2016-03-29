<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Advanced Optimizations #

<table cellspacing='4'>
<tr><td> <b>Description</b> </td><td> <b>XPath in Job file (Qizmt edit <code>&lt;myjob&gt;</code>)</b> </td><td> <b>Value Range</b> </td></tr>

<tr>
<td width='45%' valign='top'>
<h3><code>OutputMethod</code></h3>
<b>Default:</b> sorted<br>
<br>
By default the OutputMethod is <b>sorted</b> however it may be explicitly changed to <b>grouped</b> when fully sorted results are not required because grouped is a cheaper operation then fully sorted. Typically a series of mapreducer pipelines may be significantly optimized by using grouped for the intermediate outputs and sorted for the final mapreducer output:<br>
<blockquote>:<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MapReduceGroupedIntermediate.png' alt='MapReduceGroupedIntermediate' />
</td>
<td width='40%' valign='top'>
<br />
</blockquote><ul><li>SourceCode/Jobs/Job/IOSettings/OutputMethod</li></ul>

<b>Examples:</b>

<blockquote><code>&lt;OutputMethod&gt;sorted&lt;/OutputMethod&gt;</code></blockquote>

<blockquote><code>&lt;OutputMethod&gt;grouped&lt;/OutputMethod&gt;</code>
</td>
<td width='15%' valign='top'>
<br />
grouped | sorted<br>
</td>
</tr></blockquote>

<tr>
<td width='45%' valign='top'>
<h3><code>KeyMajor</code></h3>
<b>Default:</b> Entire Key Length<br>
<br>
Change KeyMajor to set how many bytes of the key to consider during machine exchange. By default the entire key is considered to ensure even distribution of data, however, significant performance gains can be achieved by using only a subset of the key during exchange. Generally random and less uniform keys may benefit from a reduction in KeyMajor without skewing the distribution.<br>
</td>
<td width='40%' valign='top'>
<br />
<ul><li>SourceCode/Jobs/Job/IOSettings/KeyMajor</li></ul>

<b>Examples:</b>

<blockquote><code>&lt;KeyMajor&gt;16&lt;/KeyMajor&gt;</code></blockquote>

<blockquote><code>&lt;KeyMajor&gt;100&lt;/KeyMajor&gt;</code>
</td>
<td width='15%' valign='top'>
<br />
1 - <a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Infinity.png' alt='Infinity' />
</td>
</tr></blockquote>

<tr>
<td width='45%' valign='top'>
<h3><code>Delta</code></h3>
<b>Default:</b> disabled<br>
<br>
When a delta node is added to a mapreducer job, the intermediate files of the mapreduce operation are cached automatically. Subsequent executions of the same mapreduce job will only exchange files which have not been cached by the mapreducer.<br>
<br>
<h4>Explicit Caching</h4>
Sometimes automatic caching may have the effect of overly expanding the data. For example, if the delta contains random samples which can be redundant, these random samples will cause the cache to expand to fast if automatic caching is used. When delta is enabled, the reducer code may override the automatic caching by explicitly identifying which key/value pairs to cache after the exchange phase, e.g.<br>
<pre><code>   ... void Reduce(...<br>
      <br>
            output.Cache(key, value);<br>
</code></pre>
</td>
<td width='40%' valign='top'>
<br />
<ul><li>SourceCode/Jobs/Job/Delta/Name<br>
</li><li>SourceCode/Jobs/Job/Delta/DFSInput</li></ul>

<b>Examples:</b>
<pre><code>      &lt;Delta&gt;<br>
        &lt;Name&gt;mydata_cacheA&lt;/Name&gt;<br>
        &lt;DFSInput&gt;dfs://mydataA*.txt&lt;/DFSInput&gt;<br>
      &lt;/Delta&gt;<br>
</code></pre>
<pre><code>      &lt;Delta&gt;<br>
        &lt;Name&gt;mydata_cacheB&lt;/Name&gt;<br>
        &lt;DFSInput&gt;dfs://mydataB*.txt&lt;/DFSInput&gt;<br>
      &lt;/Delta&gt;<br>
</code></pre>
</td>
<td width='15%' valign='top'>
<br />
Any valid windows file name.<br>
</td>
</tr>

</table>
