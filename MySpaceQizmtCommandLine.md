<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Qizmt Command Line #

<table>
<tr><td> <b>Usage</b> </td><td> <b>Description</b> </td></tr>

<tr valign='top'>
<td><div><code>retrylogmd5 &lt;logfile&gt;</code></div>
</td>
<td>writes all output to the logfile</td>
</tr>

<tr valign='top'>
<td><div><code>edit &lt;jobs.xml&gt;</code></div>
</td>
<td>edit the specified jobs source code XML file</td>
</tr>

<tr valign='top'>
<td><div><code>exec ["&lt;/xpath&gt;=&lt;value&gt;"] &lt;jobs.xml&gt;</code></div>
</td>
<td>run the jobs source code XML file</td>
</tr>

<tr valign='top'>
<td><div><code>addmachine &lt;host&gt;</code></div>
</td>
<td>add a machine to the cluster</td>
</tr>

<tr valign='top'>
<td><div><code>removemachine &lt;host&gt;</code></div>
</td>
<td>remove a machine from the cluster</td>
</tr>

<tr valign='top'>
<td><div><code>ps</code></div>
</td>
<td>distributed processes, schedule and queue info</td>
</tr>

<tr valign='top'>
<td><div><code>who</code></div>
</td>
<td>show who is logged on</td>
</tr>

<tr valign='top'>
<td><div><code>history [&lt;count&gt;]</code></div>
</td>
<td>show command history</td>
</tr>

<tr valign='top'>
<td><div><code>killall</code></div>
</td>
<td>kill all jobs, clean any orphaned intermediate data</td>
</tr>

<tr valign='top'>
<td><div><code>gen &lt;output-dfsfile&gt;</code></div>
<div><code>&lt;outputsize&gt;</code></div>
<div><code>[type=&lt;bin|ascii|word&gt;]</code></div>
<div><code>[row=&lt;size&gt;]</code></div>
<div><code>[writers=&lt;count&gt;]</code></div>
<div><code>[rand=custom]</code></div>
</td>
<td>generate random binary, ascii or word data</td>
</tr>

<tr valign='top'>
<td><div><code>combine &lt;inputfiles...&gt; [+ &lt;outputfile&gt;]</code></div>
</td>
<td>combine files into one</td>
</tr>

<tr valign='top'>
<td><div><code>format machines=localhost</code></div>
</td>
<td>format machines=localhost</td>
</tr>

<tr valign='top'>
<td><div><code>info [&lt;dfspath&gt;[:&lt;host&gt;]]</code></div>
<div><code>[-s short host name]</code></div>
<div><code>[-mt multi-threaded]</code></div>
</td>
<td>information for DFS or a DFS file</td>
</tr>

<tr valign='top'>
<td><div><code>head &lt;dfspath&gt;[:&lt;host&gt;:&lt;part&gt;] [&lt;count&gt;]</code></div>
</td>
<td>show first few lines of file</td>
</tr>

<tr valign='top'>
<td><div><code>put &lt;netpath&gt; [&lt;dfspath&gt;[@&lt;recordlen&gt;]]</code></div>
</td>
<td>put a file into DFS</td>
</tr>

<tr valign='top'>
<td><div><code>fput files|dirs=&lt;item[,item,item]&gt;|@&lt;filepath to list&gt;</code></div>
<div><code>[pattern=&lt;pattern&gt;]</code></div>
<div><code>[mode=continuous]</code></div>
<div><code>[dfsfilename=&lt;targetfilename&gt;]</code></div>
</td>
<td>put files into DFS</td>
</tr>

<tr valign='top'>
<td><div><code>putbinary &lt;wildcard&gt; &lt;dfspath&gt;</code></div>
</td>
<td>put binary into DFS</td>
</tr>

<tr valign='top'>
<td><div><code>get [parts=&lt;first&gt;[-[&lt;last&gt;]]] &lt;dfspath&gt; &lt;netpath&gt;</code></div>
</td>
<td>get a file from DFS</td>
</tr>

<tr valign='top'>
<td><div><code>fget &lt;dfspath&gt;</code></div>
<div><code>&lt;targetFolder&gt;[ &lt;targetFolder&gt; &lt;targetFolder&gt;] |</code></div>
<div><code>@&lt;filepath to target folder list&gt; [-gz] [-md5]</code></div>
</td>
<td>get a file from DFS</td>
</tr>

<tr valign='top'>
<td><div><code>getbinary &lt;dfspath&gt; &lt;netpath&gt;</code></div>
</td>
<td>get binary from DFS</td>
</tr>

<tr valign='top'>
<td><div><code>del &lt;dfspath|wildcard&gt;</code></div>
</td>
<td>delete a DFS file using multiple threads</td>
</tr>

<tr valign='top'>
<td><div><code>rename &lt;dfspath-old&gt; &lt;dfspath-new&gt;</code></div>
</td>
<td>rename a DFS file</td>
</tr>

<tr valign='top'>
<td><div><code>getjobs &lt;netpath.dj&gt;</code></div>
</td>
<td>archive all jobs</td>
</tr>

<tr valign='top'>
<td><div><code>putjobs &lt;netpath.dj&gt;</code></div>
</td>
<td>import jobs archive</td>
</tr>

<tr valign='top'>
<td><div><code>ls</code></div>
</td>
<td>DFS file listing</td>
</tr>

<tr valign='top'>
<td><div><code>countparts &lt;dfspath&gt;</code></div>
</td>
<td>get parts count of a file</td>
</tr>

<tr valign='top'>
<td><div><code>invalidate &lt;cacheName&gt; &lt;fileNodeName&gt;</code></div>
</td>
<td>invalidate a file node of the cache</td>
</tr>

<tr valign='top'>
<td><div><code>health [-a check DFS health]</code></div>
<div><code>[-v verify driver]</code></div>
<div><code>[-mt multi-threaded]</code></div>
</td>
<td>Show the health of the machines in the cluster</td>
</tr>

<tr valign='top'>
<td><div><code>examples</code></div>
</td>
<td>generate example jobs source code</td>
</tr>

<tr valign='top'>
<td><div><code>importdir &lt;netpath&gt;</code></div>
</td>
<td>import jobs into DFS</td>
</tr>

<tr valign='top'>
<td><div><code>listinstalldir</code></div>
</td>
<td>List all installed directories</td>
</tr>

<tr valign='top'>
<td><div><code>harddrivespeedtest [&lt;filesize&gt;]</code></div>
</td>
<td>Test write/read hard drive speed</td>
</tr>

<tr valign='top'>
<td><div><code>networkspeedtest [&lt;filesize&gt;]</code></div>
</td>
<td>Test upload/download network speed test</td>
</tr>

<tr valign='top'>
<td><div><code>exechistory</code></div>
</td>
<td>List the most recent executed commands</td>
</tr>

<tr valign='top'>
<td><div><code>cputemp</code></div>
</td>
<td>List cpu temperature</td>
</tr>

<tr valign='top'>
<td><div><code>ghost</code></div>
</td>
<td>List ghost data files</td>
</tr>

<tr valign='top'>
<td><div><code>perfmon &lt;network|cputime|diskio|availablememory&gt;</code></div>
<div><code>[a=&lt;Number of readings to get.  Return average.&gt;]</code></div>
<div><code>[t=&lt;Number of threads&gt;]</code></div>
<div><code>[s=&lt;Milliseconds of sleep to take between readings&gt;]</code></div>
</td>
<td>get Perfmon counter readings</td>
</tr>

<tr valign='top'>
<td><div><code>perfmon generic</code></div>
<div><code>o=&lt;Object/category name&gt;</code></div>
<div><code>c=&lt;Counter name&gt;</code></div>
<div><code>i=&lt;Instance Name&gt;</code></div>
<div><code>[f Display readings in friendly byte size units]</code></div>
<div><code>[a=&lt;Number of readings to get.  Return average.&gt;]</code></div>
<div><code>[t=&lt;Number of threads&gt;]</code></div>
<div><code>[s=&lt;Milliseconds of sleep to take between readings&gt;]</code></div>
</td>
<td>specify a Perfmon counter to read</td>
</tr>

<tr valign='top'>
<td><div><code>packetsniff [t=&lt;Number of threads&gt;]</code></div>
<div><code>[s=&lt;Milliseconds to sniff&gt;]</code></div>
<div><code>[v verbose]</code></div>
<div><code>[a include non-cluster machines]</code></div>
</td>
<td>Sniff packets</td>
</tr>

<tr valign='top'>
<td><div><code>md5 &lt;dfsfile&gt;</code></div>
</td>
<td>compute MD5 of DFS data file</td>
</tr>

<tr valign='top'>
<td><div><code>checksum &lt;dfsfile&gt;</code></div>
</td>
<td>compute sum of DFS data file</td>
</tr>

<tr valign='top'>
<td><div><code>sorted &lt;dfsfile&gt;</code></div>
</td>
<td>check if a DFS data file has sorted lines</td>
</tr>

<tr valign='top'>
<td><div><code>nearprime &lt;positiveNum&gt;</code></div>
</td>
<td>find the nearest prime number</td>
</tr>

<tr valign='top'>
<td><div><code>genhostnames &lt;pattern&gt; &lt;startNum&gt;</code></div>
<div><code>&lt;endNum&gt; [&lt;delimiter&gt;]</code></div>
</td>
<td>generate host names</td>
</tr>

<tr valign='top'>
<td><div><code>viewlogs [machine=&lt;machineName&gt;]</code></div>
<div><code>[count=&lt;number of entries to return&gt;]</code></div>
</td>
<td>view log entries</td>
</tr>

<tr valign='top'>
<td><div><code>clearlogs</code></div>
</td>
<td>clear logs from all machines in the cluster</td>
</tr>

<tr valign='top'>
<td><div><code>maxuserlogsview</code></div>
</td>
<td>view maxUserLogs configuration</td>
</tr>

<tr valign='top'>
<td><div><code>maxuserlogsupdate &lt;integer&gt;</code></div>
</td>
<td>update maxUserLogs configuration</td>
</tr>

<tr valign='top'>
<td><div><code>maxdglobalsview</code></div>
</td>
<td>view maxDGlobals configuration</td>
</tr>

<tr valign='top'>
<td><div><code>maxdglobalsupdate &lt;integer&gt;</code></div>
</td>
<td>update maxDGlobals configuration</td>
</tr>

<tr valign='top'>
<td><div><code>recordsize &lt;user-size&gt;</code></div>
</td>
<td>returns bytes of user-friendly size</td>
</tr>

<tr valign='top'>
<td><div><code>swap &lt;file1&gt; &lt;file2&gt;</code></div>
</td>
<td>file names to swap</td>
</tr>

<tr valign='top'>
<td><div><code>regressiontest basic</code></div>
</td>
<td>basic regression test Qizmt</td>
</tr>

<tr valign='top'>
<td><div><code>kill &lt;JobID&gt;</code></div>
</td>
<td>kill the specified Job Identifier</td>
</tr>

<tr valign='top'>
<td><div><code>enqueue command=&lt;value&gt;</code></div>
<div><code>[ExecTimeout=&lt;secs&gt; Maximum seconds Qizmt exec can run</code></div>
<div><code>OnTimeout=&lt;tcmd&gt;] Run on timeout; e.g.  Qizmt kill #JID#</code></div>
</td>
<td>Adds a command to the end of the queue</td>
</tr>

<tr valign='top'>
<td><div><code>queuekill &lt;QueueID&gt;</code></div>
</td>
<td>Removes the specified Queue Identifier</td>
</tr>

<tr valign='top'>
<td><div><code>clearqueue</code></div>
</td>
<td>Removes all entries from the queue</td>
</tr>

<tr valign='top'>
<td><div><code>schedule command=&lt;value&gt; </code></div>
<div><code>start=&lt;now|&lt;datetime&gt;&gt; </code></div>
<div><code>[frequency=&lt;seconds&gt;]</code></div>
<div><code>[texceptions=&lt;&lt;datetime&gt;[-&lt;datetime&gt;]&gt;[,...]] ranges when not to run</code></div>
<div><code>[wexceptions=&lt;weekday&gt;[,...]] whole weekdays not to run</code></div>
<div><code>[wtexceptions=&lt;wdtime&gt;[,...]] time on day-of-week not to run</code></div>
</td>
<td><div>adds a command entry to the scheduler</div>
<div>(datetime format is <code>[M[/D[/Y]].][h:m[:s][AM|PM]]</code>)</div>
<div>(wdtime format is <code>&lt;weekday&gt;@&lt;h:m[:s][AM|PM]&gt;[-&lt;h:m[:s][AM|PM]&gt;]</code>)</div>
</td>
</tr>

<tr valign='top'>
<td><div><code>pauseschedule &lt;ScheduleID&gt;</code></div>
</td>
<td>Pauses the specified Schedule Identifier</td>
</tr>

<tr valign='top'>
<td><div><code>unpauseschedule &lt;ScheduleID&gt;</code></div>
</td>
<td>Un-pauses the specified Schedule Identifier</td>
</tr>

<tr valign='top'>
<td><div><code>unschedule &lt;ScheduleID&gt;</code></div>
</td>
<td>Removes the specified Schedule Identifier</td>
</tr>

<tr valign='top'>
<td><div><code>clearschedule</code></div>
</td>
<td>Removes all entries from the scheduler</td>
</tr>



<tr valign='top'>
<td><div><code>shuffle &lt;source&gt; &lt;target&gt;</code></div>
</td>
<td>Shuffle underlying parts of a rectangular binary file, maintaining chunk order</td>
</tr>

<tr valign='top'>
<td><div><code>spread &lt;dfsfile&gt; &lt;out-dfsfile&gt;</code></div>
</td>
<td>Spread a DFS file across the cluster</td>
</tr>

</table>
