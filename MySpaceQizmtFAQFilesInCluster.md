<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Files in Cluster #

## How to put a file into cluster from a remote machine? ##

---

There are a few ways:
  1. Use the rput command, this will copy the file locally with the current credentials then put it into qizmt with the qizmt credentials.
  1. Copy locally then put into qizmt with put command
  1. Write a remote job that opens an ADO.Net connection and streams data in from an RDBMS


## Ok, so when I move a file to the cluster, how do I reference it in the xml? ##

---

IO tags of job, set the input of the mapreducer to the file in MR.DFS<br />
e.g.<br />
```
<Job …
  <IOSettings>
    .
    .
    .
   <DFSInput>dfs://OpenCV_Input.blob</DFSInput>
```

## How to copy files into and out of MR.DFS from shares which I have access to but the account Qizmt is running under does not? ##

---

You can use rget and rput to use the credentials of the account issuing the command to move files between MR.DFS and a share somewhere.


**qizmt rput** – copies the file from the share to the local machine using the executing users credentials, qizmt rput then copies the file into MR.DFS using the qizmt credentials.


**qizmt rget** – copies the file from MR.DFS using the qizmt credentials to a temp directory on the current machine, qizmt rget then copies the file from the temp directory to the share using the executing users credentials.


Alternatively, you can use regular **qizmt get** to get the file out of MR.DFS and then copy it to the network share with regular windows
**copy /Y**


Alternatively, can use a remote job to stream the file somewhere with some kind of API such as ADO.Net


If you want to use get to get the file but it is too big to fit on single machine you can get it out in parts:


`Qizmt get [parts=<first>[-[<last>]]] <dfspath> <netpath>`


The total number of parts can be seen in the **qizmt ls** command.


`Qizmt info <dfsfilename>` will show how many parts on each machine
`Qizmt info <dfsfilename>:<host>` will show how many parts on each machine and the size of each part.


The size of each parts caps around 64 megabytes


Can use **qizmt info** to see how many parts a MR.DFS file has:


C:\>Qizmt info datafile.txt

[file information](DFS.md)

> DFS File: datafile.txt<br />
> > Size: 4 TB (4398046511104)<br />
> > Sample Size: 467.27 KB (116.82 KB avg)<br />
> > Parts: 9351<br />
> > > X parts on MACHINEA (500 GB data; 467.27 KB samples)<br />
> > > X parts on MACHINEB (500 GB data; 467.27 KB samples)<br />
> > > X parts on MACHINEC (500 GB data; 467.27 KB samples)<br />
> > > X parts on MACHINED (500 GB data; 467.27 KB samples)<br />
> > > X parts on MACHINEE (500 GB data; 467.27 KB samples)<br />
> > > X parts on MACHINEF (500 GB data; 467.27 KB samples)<br />
> > > X parts on MACHINEG (500 GB data; 467.27 KB samples)<br />
> > > X parts on MACHINEG (500 GB data; 467.27 KB samples)<br />


Can use the parts switch on qizmt get to get ranges of parts if they do not all fit on a single machine:


qizmt get parts=0-999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET0\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=1000-1999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET1\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=2000-2999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET2\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=3000-3999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET3\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=4000-4999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET4\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=5000-5999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET5\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=6000-6999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET6\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=7000-7999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET7\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=8000-8999 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET8\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

qizmt get parts=9000-9351 datafile.txt \\MACHINEA\Drop\temp.txt

copy /Y \\MACHINEA\Drop\temp.txt \\TARGET9\Drop\temp.txt

del /Q /P \\MACHINEA\Drop\temp.txt

Thought if sending a large amount of data to a bunch of machines, it may be advantageous to install a qizmt cluster on those machines and have a mapreducer job just dump the data out locally on that cluster so that it is not bottlenecked.

## How to stream multiple files in other VLANs into a Qizmt cluster? ##

---

Data can be streamed into a cluster in without being limited to the disk space of a single machine is to create a remote job and stream the data directly into a MR.DFS file, this will also avoid extra disk copying, however this will require the Qizmt account to have read permissions set on the source file or files.
```
<SourceCode>
  <Jobs>
    <Job Name="StreamBigFileIn_CreateSampleData" Custodian="" Email="" Description="Create sample data">
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://BigFile.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                string sLine; //if binary data with fixed length tuples, use byte[] wrapped in byteslice then put into recordset before outputing
                using(System.IO.StreamReader sr = new System.IO.StreamReader("\\NETWORKPATH..."))
                {
                    //stream line at a time or chunk at a time, how ever the data is formatted
                    .
                    .
                    .
                    dfsoutput.WriteLine(sline); 
                }
           }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
```


## Is there a way to read more than one dfs file at the same time? Let’s say I need compare 2 dfs files line by line. I may specify Remote job that process dfsinput, but I need open another dfs stream. Could I do that? ##

---

Could do a mapreducer to prepend a category “A,” to the first MR.DFS file, and another mapreducer to prepend “B,” to the second file. Then can run them both into a single mapreducer which keys both on the category and the line. Then in reducer compare the lines. This would remain fully distributed way to compare two files.


Also, there are some high level commands for getting an MD5 or a checksum of a file which could be compared if the entire files are identical but these are not distributed and a bit slow, we just use them for regression testing:



> md5 `<dfsfile>`   compute MD5 of DFS data file<br />
> checksum `<dfsfile>`   compute sum of DFS data file<br />
> sorted `<dfsfile>`   check if a DFS data file has sorted lines<br />
