<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# MR.DFS Filename Ranged Input #
Mapreducers jobs can take inputs both by Date ranges and numeric ranges in the MR.DFS file names. Ranges may be specified in the `<DFSInput>` tag rather than solid filenames.


## Wildcard Input ##
The ‘`*`’ character may be used as wildcard for 0 or more characters in the MR.DFS file name.
` <DFSInput>dfs://SomeFile*.txt</DFSInput> `


## Aggregate Input ##
The ‘;’ character may be used to delimit multiple MR.DFS file inputs.
` <DFSInput>dfs://SomeFile1.txt;dfs://SomeFile2.txt</DFSInput> `


## Numeric/Date Ranged Input ##
The ‘|’ and ‘-’ characters may be used to input a range of files containing digits in their MR.DFS filenames.
` DFSInput>dfs://Qizmt-WildCardNumericRanges_Input|20090601-20100101|*.txt</DFSInput> `


## Aggregate Ranged Input ##
Multiple ranged inputs may be aggregated.
` <DFSInput>dfs://Qizmt-WildCardNumericRanges_Input|11-12|*.txt;dfs://Qizmt-WildCardNumericRanges_Input15f.txt;dfs://Qizmt-WildCardNumericRanges_Input|20090601-20100101|*.txt</DFSInput> `
