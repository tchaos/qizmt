<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Synchronizations #

## How to synchronize all remote, map or reduce processes on a single machine? ##

---

If you want to use a OS wide critical section you can use:
```
System.Threading.Mutex osCriticalSection = new System.Threading.Mutex(false,
"CreatePublishDir{7104700F-424F-4a34-BAF8-6A127852A216}");
osCriticalSection.WaitOne();
{
  sw.WriteLine(val);
}
osCriticalSection.ReleaseMutex();
osCriticalSection.Close();
```

But it would probably be more efficient to just use a different filename for each reducer process by appending Qizmt\_ProcessID.ToString() to the file name.

Here are qizmt variables in case it helps with coordinating:

`int Qizmt_ProcessID` – id of current process in cluster, 0 to Qizmt\_ProcessCount - 1


`int Qizmt_ProcessCount` – total of number of processes for the job, usually equal to number of                                                  cores or a prime near the number of cores


`string Qizmt_MachineHost` – hostname of current machine that current mapper, reducer, local or remote is running on


`string Qizmt_MachineIP` – current machine that current mapper, reducer, local or remote is running on


`string[] Qizmt_ExecArgs` – commandline arguments sent in with the qizmt exec command


e.g. if there are 4000 cores in the cluster, the index created from reducer with Qizmt\_ProcessID 0 will have the first sorted range, the file created from reducer with Qizmt\_ProcessID 1 will have the second sorted range all the way up to Qizmt\_ProcessID 3999 which will have the last sorted range.