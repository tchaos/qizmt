<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# StaticGlobals #



| **Name** | **Description** |
|:---------|:----------------|
| `public static long MapIteration` | The current iteration of map  |
| `public static long ReduceIteration` | The current iteration of reduce |
| `public static int Qizmt_KeyLength` | The key length of the current mapreduce  |
| `public static int Qizmt_BlocksTotalCount` | Total number of processes for the job, usually equal to the number of cores or a prime near the number of cores.  |
| `public static int Qizmt_BlockID` | 	ID of current process in cluster, 0 to Qizmt\_BlocksTotalCount – 1.  |
| `public static string[] Qizmt_Hosts` | The list of all hosts in the cluster.  |
| `public static string Qizmt_OutputDirection` | 	The sorting direction of a mapreduce job. |
| `public static bool Qizmt_Last` | 	It has the value of true if this is the last iteration of a map or reduce job. |
| `public static string Qizmt_InputFileName` | 	It is the input file name of the current line in map. |