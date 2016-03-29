<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Building Infrastructures on top of Qizmt #


## Building a RDBMS on top of Qizmt ##
One way to support SQL against `<file>.tbl` files would be to write a series of mapreducers and a local job to parse SQL and fire off appropriate mapreducer jobs, by nature of mapreduce, you do not have to worry about the complexities of writing a query optimizer, e.g.
<pre>
Local job: SQL<br>
Mapreduce job: insert_into_select.xml<br>
Mapreduce job: select_standard_out.xml<br>
Mapreduce job: update_table.xml<br>
Local job: delete_from_table.xml<br>
Local job: drop_table.xml<br>
Local job: truncate_table.xml</pre>
Note: `update_table.xml` actually creates a new table, deletes the old, and renames the new table back to the name of the old table, but could possibly safely support some mutating functionality in Qizmt to make this more efficient.

An example run could look like:

<pre>
C:\> Qizmt gensql<br>
Generating SQL Jobs:<br>
insert_into_select.xml – selecting into a new empty table from 1 or more other tables, could potentially support WITH INDEX and use Qizmt caching<br>
select_standard_out.xml – display a resulting temporary table to the standard output and then delete it<br>
update_table.xml – change lines in a table based on some criteria<br>
delete_from_table.xml – delete lines in a table based on some criteria<br>
drop_table.xml – Qizmt command to delete the MR.DFS file<br>
truncate_table.xml – Qizmt command to delete the MR.DFS file and recreate it empty<br>
.<br>
.<br>
.<br>
SQL jobs generated, for help issue command: Qizmt sql "help"<br>
C:\> Qizmt exec sql "INSERT INTO Customer SELECT * FROM Customer_delta"<br>
</pre>


## Building a Search Indexer on top of Qizmt ##
Search indexing is easily done with Qizmt as mapreduce is the optimal way to sort large amounts of data evenly across many servers. Qizmt caching makes periodic deltas exchange and redistribute across a sorted cluster evenly without having to resort the entire index. This is typically useful for:
  * Large scale existence checking
  * Large scale end-user search services

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_SearchIndexerAtop.png' alt='SearchIndexerAtop' />
