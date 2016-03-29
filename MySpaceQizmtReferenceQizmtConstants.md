<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Qizmt`_``*` Constants #



| **Name** | **Description** |
|:---------|:----------------|
| `const int Qizmt_ProcessID` | ID of current process in cluster, 0 to `Qizmt_ProcessCount` – 1.  |
| `const int Qizmt_ProcessCount` | Total number of processes for the job, usually equal to the number of cores or a prime near the number of cores. |
| `const string Qizmt_MachineHost` | Host name of the machine that the current mapper, reducer, local or remote is running on.  |
| `const string Qizmt_MachineIP` | IP address of the machine that the current mapper, reducer, local or remote is running on.  |
| `const string[] Qizmt_ExecArgs` | 	Command line arguments sent in with the Qizmt exec command  |
| `const int Qizmt_KeyLength` | IOSettings/ KeyLength  |
| `const string Qizmt_LogName` | 	Name of log file |
| `const bool Qizmt_KeyRepeated ` | 	It has the value of true if the current key in reducer occurs in multiple reducers. |