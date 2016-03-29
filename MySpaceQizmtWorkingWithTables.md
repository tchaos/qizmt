<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Working with Tables #
Sometimes it is advantageous to store recordsets directly in MR.DFS table files rather than manually tokenizing the data into a text format. By convention, it is suggested to store recordsets in MR.DFS files using the `<file>.tbl` naming convention. If reading a table into a mapreducer job from MR.DFS, the DFSInput must contain `<filename>@<row size>` showing the number of bytes in each row. If writing a table to a mapreducer job from MR.DFS, the DFSOutput must contain `<filename>@<row size>` showing the number of bytes in each row.

## Specifying Schema in Rectangular Binary Files ##
An alternative to specifying the number of bytes in each row `<filename>@<row size>`, is the use of data type key words.  For example, `<filename>@Int,Int` instead of `<filename>@8`, `<filename>@Int,Long` instead of `<filename>@12`.  The current data type key words supported are: `Int`, `Long`, `DateTime`, and `Char(n)`.

## Writing data from Mapreducer out to a Table ##
A recordset may be directly written out to a MR.DFS file from a reducer as follows.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_ReduceTableOutput.png' alt='ReduceTableOutput' />


## Reading a Table data into a Mapreducer ##
A line of data may be read directly into a recordset only if it was written out as a recordset in the reducer of another mapreducer job.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_MapTableInput.png' alt='MapTableInput' />


## Walkthrough: Loading and Processing Rectangular Binary Data ##
  1. Generate rectangular binary data with **gensort** or other means consisting of 100 bytes per row
  1. Put the binary data into MR.DFS with: **Qizmt put \\MACHINEA\D$\binarydata.tbl@100**
  1. The rectangular binary data consisting of 100 bytes per row may now be input to a mapreducer
  1. Edit a mapreducer and add **`<DFSInput>dfs://binarydata.tbl@100</DFSInput>`** for the input
  1. To read each 100 byte row from the binary table into a recordset use: <br /> **recordset rline = recordset.PrepareRow(line);**


## Walkthrough: Storing Mapreducer Results into a Table ##
  1. Modify the DFSOutput to include the size of recordset that will be output. If the recordsets change in size or are heterogeneous, then set the size to the largest possible size supported. E.g. **`<DFSOutput>dfs://SomeOutput.tbl@1024</DFSOutput>`**
  1. In reducer output a recordset directly:
> <pre>
recordset rout = recordset.Prepare();<br>
rout.PutInt(uid);<br>
rout.PutDouble(total);<br>
output.Add(rout);<br>
</pre>
> > Note: the table row size may be larger than the recordset, but if smaller an exception will be thrown.