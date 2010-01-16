/**************************************************************************************
 *  MySpace’s Mapreduce Framework is a mapreduce framework for distributed computing  *
 *  and developing distributed computing applications on large clusters of servers.   *
 *                                                                                    *
 *  Copyright (C) 2008  MySpace Inc. <http://qizmt.myspace.com/>                      *
 *                                                                                    *
 *  This program is free software: you can redistribute it and/or modify              *
 *  it under the terms of the GNU General Public License as published by              *
 *  the Free Software Foundation, either version 3 of the License, or                 *
 *  (at your option) any later version.                                               *
 *                                                                                    *
 *  This program is distributed in the hope that it will be useful,                   *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of                    *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                     *
 *  GNU General Public License for more details.                                      *
 *                                                                                    *
 *  You should have received a copy of the GNU General Public License                 *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.             *
***************************************************************************************/

//#define CLIENT_LOG_ALL

#define AELIGHT_TRACE

#if DEBUG
//#define AELIGHT_TRACE_PORT // DEBUG only.
#endif

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {
        static void ShowUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("    {0} <action> [<logging>] [<arguments>]", appname);
            Console.WriteLine("Logging:");
            Console.WriteLine("    retrylogmd5 <logfile>   writes all output to the logfile");
            Console.WriteLine("Actions:");
            Console.WriteLine("    edit <jobs.xml>         edit the specified jobs source code XML file");
            //Console.WriteLine("    exec <jobs.xml>         run the specified jobs source code XML file");
            Console.WriteLine(@"    exec [""</xpath>=<value>""] <jobs.xml>  run the jobs source code XML file");
            Console.WriteLine("    addmachine <host>       add a machine to the cluster");
            Console.WriteLine("    removemachine <host>    remove a machine from the cluster");
            //Console.WriteLine("    ps                      distributed process information");
            Console.WriteLine("    ps                      distributed processes, schedule and queue info");
            Console.WriteLine("    who                     show who is logged on");
            Console.WriteLine("    history [<count>]       show command history");
            Console.WriteLine("    killall                 kill all jobs, clean any orphaned intermediate data");
            //Console.WriteLine("    gen <output-dfsfile> <outputsize> [<rowsize>] [<writerCount>] [<customRandom>]    generate random data");
            //Console.WriteLine("    asciigen <output-dfsfile> <outputsize> [<rowsize>] [<writerCount>] [<customRandom>]  generate random ASCII");
            //Console.WriteLine("    wordgen <output-dfsfile> <outputsize> [<rowsize>] [<writerCount>] [<customRandom>]   random sentence words");
            Console.WriteLine("    gen <output-dfsfile>");
            Console.WriteLine("        <outputsize>");
            Console.WriteLine("        [type=<bin|ascii|word>]");
            Console.WriteLine("        [row=<size>]");
            Console.WriteLine("        [writers=<count>]");
            Console.WriteLine("        [rand=custom]");
            Console.WriteLine("        generate random binary, ascii or word data");
            Console.WriteLine("    combine <inputfiles...> [+ <outputfile>]   combine files into one");
            Console.WriteLine("    format machines=localhost       format a new DFS");
            Console.WriteLine("    info [<dfspath>[:<host>]]");
            Console.WriteLine("         [-s short host name]");
            Console.WriteLine("         [-mt multi-threaded]");
            Console.WriteLine("         information for DFS or a DFS file");
            Console.WriteLine("    head <dfspath>[:<host>:<part>] [<count>]   show first few lines of file");
            Console.WriteLine("    put <netpath> [<dfspath>[@<recordlen>]]    put a file into DFS");
            Console.WriteLine("    fput files|dirs=<item[,item,item]>|@<filepath to list> [pattern=<pattern>]");
            Console.WriteLine("         [mode=continuous] [dfsfilename=<targetfilename>]");
            Console.WriteLine("         put files into DFS");
            Console.WriteLine("    putbinary <wildcard> <dfspath>  put binary into DFS");
            Console.WriteLine("    get [parts=<first>[-[<last>]]] <dfspath> <netpath>  get a file from DFS");
            Console.WriteLine("    fget <dfspath> <targetFolder>[ <targetFolder> <targetFolder>] |");
            Console.WriteLine("         @<filepath to target folder list> [-gz] [-md5]");
            Console.WriteLine("         get a file from DFS");
            Console.WriteLine("    getbinary <dfspath> <netpath>  get binary from DFS");
            Console.WriteLine("    del <dfspath|wildcard>  delete a DFS file using multiple threads");
            //Console.WriteLine("    delmt <dfspath|wildcard>  delete a DFS file using multiple threads");
            //Console.WriteLine("    delst <dfspath|wildcard>  delete a DFS file using a single thread");
            Console.WriteLine("    rename <dfspath-old> <dfspath-new>   rename a DFS file");
            Console.WriteLine("    getjobs <netpath.dj>    archive all jobs");
            Console.WriteLine("    putjobs <netpath.dj>    import jobs archive");
            Console.WriteLine("    ls                      DFS file listing");
            Console.WriteLine("    countparts <dfspath>    get parts count of a file");
            Console.WriteLine("    invalidate <cacheName> <fileNodeName>   invalidate a file node of the cache");
            Console.WriteLine("    health [-a check DFS health]");
            Console.WriteLine("           [-v verify driver]");
            Console.WriteLine("           [-mt multi-threaded]");
            Console.WriteLine("           Show the health of the machines in the cluster");
            Console.WriteLine("    examples                generate example jobs source code");
            Console.WriteLine("    importdir <netpath>     import jobs into DFS");
            Console.WriteLine("    listinstalldir          List all installed directories");
            Console.WriteLine("    harddrivespeedtest [<filesize>] Test write/read hard drive speed");
            Console.WriteLine("    networkspeedtest [<filesize>]   Test upload/download network speed test");
            Console.WriteLine("    exechistory             List the most recent executed commands");
            Console.WriteLine("    cputemp                 List cpu temperature");
            Console.WriteLine("    ghost                   List ghost data files");
            Console.WriteLine("    perfmon <network|cputime|diskio|availablememory>");
            Console.WriteLine("                    [a=<Number of readings to get.  Return average.>] ");
            Console.WriteLine("                    [t=<Number of threads>]");
            Console.WriteLine("                    [s=<Milliseconds of sleep to take between readings>]");
            //Console.WriteLine("                    [<host1[;host2...]>|@<hosts.txt>]");
            Console.WriteLine("                    get Perfmon counter readings");
            Console.WriteLine("    perfmon generic");
            Console.WriteLine("                    o=<Object/category name>");
            Console.WriteLine("                    c=<Counter name>");
            Console.WriteLine("                    i=<Instance Name>");
            Console.WriteLine("                    [f Display readings in friendly byte size units]");
            Console.WriteLine("                    [a=<Number of readings to get.  Return average.>] ");
            Console.WriteLine("                    [t=<Number of threads>]");
            Console.WriteLine("                    [s=<Milliseconds of sleep to take between readings>]");
            //Console.WriteLine("                    [<host1[;host2...]>|@<hosts.txt>]");
            Console.WriteLine("                    specify a Perfmon counter to read");
            Console.WriteLine("    packetsniff     [t=<Number of threads>]");
            Console.WriteLine("                    [s=<Milliseconds to sniff>]");
            Console.WriteLine("                    [v verbose]");
            Console.WriteLine("                    [a include non-cluster machines]");
            //Console.WriteLine("                    [<host1[;host2...]>|@<hosts.txt>]");
            Console.WriteLine("                    Sniff packets");
            Console.WriteLine("    md5 <dfsfile>           compute MD5 of DFS data file");
            Console.WriteLine("    checksum <dfsfile>      compute sum of DFS data file");
            Console.WriteLine("    sorted <dfsfile>        check if a DFS data file has sorted lines");
            Console.WriteLine("    nearprime <positiveNum> find the nearest prime number");
            Console.WriteLine("    genhostnames <pattern> <startNum>");
            Console.WriteLine("                 <endNum> [<delimiter>]");
            Console.WriteLine("                 generate host names");
            Console.WriteLine("    viewlogs      [machine=<machineName>]");
            Console.WriteLine("                 [count=<number of entries to return>]");
            Console.WriteLine("                 view log entries");
            Console.WriteLine("    clearlogs                      clear logs from all machines in the cluster");
            Console.WriteLine("    maxuserlogsview                view maxUserLogs configuration");
            Console.WriteLine("    maxuserlogsupdate <integer>    update maxUserLogs configuration");
            Console.WriteLine("    maxdglobalsview                view maxDGlobals configuration");
            Console.WriteLine("    maxdglobalsupdate <integer>    update maxDGlobals configuration");
            Console.WriteLine("    recordsize <user-size>         returns bytes of user-friendly size");
            Console.WriteLine("    swap <file1> <file2>           file names to swap");
            Console.WriteLine("    regressiontest basic           basic regression test " + appname);
            Console.WriteLine("    kill <JobID>                   kill the specified Job Identifier");
            Console.WriteLine("    enqueue command=<value>");
            Console.WriteLine("         [ExecTimeout=<secs>   Maximum seconds Qizmt exec can run");
            Console.WriteLine("         OnTimeout=<tcmd>]     Run on timeout;  e.g.  Qizmt kill #JID#");
            Console.WriteLine("         Adds a command to the end of the queue");
            Console.WriteLine("    queuekill <QueueID>            Removes the specified Queue Identifier");
            Console.WriteLine("    clearqueue                     Removes all entries from the queue");
            Console.WriteLine("    schedule command=<value> start=<now|<datetime>> [frequency=<seconds>]");
            Console.WriteLine("         [texceptions=<<datetime>[-<datetime>]>[,...]] ranges when not to run");
            Console.WriteLine("         [wexceptions=<weekday>[,...]] whole weekdays not to run");
            Console.WriteLine("         [wtexceptions=<wdtime>[,...]] time on day-of-week not to run");
            Console.WriteLine("         adds a command entry to the scheduler");
            Console.WriteLine("         (datetime format is {0})",
                MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry.TimeSpec.DATE_TIME_FORMAT);
            Console.WriteLine("         (wdtime format is {0})",
                MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry.TimeSpec.WT_FORMAT);
            Console.WriteLine("    pauseschedule <ScheduleID>     Pauses the specified Schedule Identifier");
            Console.WriteLine("    unpauseschedule <ScheduleID>   Un-pauses the specified Schedule Identifier");
            Console.WriteLine("    unschedule <ScheduleID>        Removes the specified Schedule Identifier");
            Console.WriteLine("    clearschedule                  Removes all entries from the scheduler");
            Console.WriteLine("    shuffle <source> <target>      Shuffle underlying parts of a rectangular");
            Console.WriteLine("                                   binary file, maintaining chunk order");
            Console.WriteLine("    spread <dfsfile> <out-dfsfile> Spread a DFS file across the cluster");
        }


        public static int FILE_BUFFER_SIZE
        {
            get
            {
                return dfs.FILE_BUFFER_SIZE;
            }
        }


        public static bool IsPrime(int x)
        {
            if (x <= 1)
            {
                return false;
            }
            for (int y = 2; y < x; y++)
            {
                if (0 == (x % y))
                {
                    return false;
                }
            }
            return true;
        }

        public static int NearestPrimeLE(int x)
        {
            if (x > 2)
            {
                if (IsPrime(x))
                {
                    return x;
                }
                for (int w = x - 1; w >= 2; w--)
                {
                    if (IsPrime(w))
                    {
                        return w;
                    }
                }
            }
            return 2;
        }

        public static int NearestPrimeGE(int x)
        {
            if (x > 2)
            {
                if (IsPrime(x))
                {
                    return x;
                }
                for (int w = x + 1; w < int.MaxValue; w++)
                {
                    if (IsPrime(w))
                    {
                        return w;
                    }
                }
            }
            return 2;
        }


        protected internal static bool InteractiveMode
        {
            get
            {
                return !isdspace;
            }
        }


        public static void EnsureNetworkPath(string path)
        {
            if (!path.StartsWith(@"\\"))
            {
                //if (!QuietMode)
                {
                    System.IO.FileInfo finfo = new System.IO.FileInfo(path);
                    //Console.Error.WriteLine("Local path not supported; use network path:  {0}", @"\\" + System.Net.Dns.GetHostName() + @"\" + finfo.FullName.Replace(':', '$'));
                    Console.Error.WriteLine("Local path not supported; use a network path:  {0}", @"\\" + System.Net.Dns.GetHostName() + @"\C$\" + finfo.Name);
                }
#if DEBUG
                if (!QuietMode)
                {
                    Console.WriteLine("DEBUG:  allowing local path from command-line");
                    return;
                }
#endif
                SetFailure();
                throw new Exception("Network path expected");
            }

#if NOLOCALHOSTPATH
            if (!QuietMode)
            {
                if (path.StartsWith(@"\\localhost\", StringComparison.OrdinalIgnoreCase)
                    || path.StartsWith(@"\\127.0.0.1\"))
                {
                    Console.Error.WriteLine("Warning: localhost should not be used in network path");
                }
            }
#endif

        }


        public static string NetworkPathForHost(string host)
        {
            return Surrogate.NetworkPathForHost(host);
        }

        public static string MapNodeToNetworkPath(dfs.DfsFile.FileNode node)
        {
            return dfs.MapNodeToNetworkPath(node);
        }

        // Appended to netpaths.
        public static void MapNodesToNetworkPaths(IList<dfs.DfsFile.FileNode> nodes, List<string> netpaths)
        {
            dfs.MapNodesToNetworkPaths(nodes, netpaths);
        }


        public static void CheckUserLogs(IList<string> hosts, string logfilename)
        {
            StringBuilder sb = new StringBuilder(100);

            for (int i = 0; i < hosts.Count; i++)
            {
                try
                {
                    string np = NetworkPathForHost(hosts[i]) + @"\" + logfilename;
                    if (System.IO.File.Exists(np))
                    {
                        string trickle = System.IO.File.ReadAllText(np);
                        if (-1 != trickle.IndexOf("Thread exception:") || -1 != trickle.IndexOf("ArrayComboList Sub Process exception: "))
                        {
                            //throw new Exception("Exception detected from " + hosts[i] + ": " + trickle);
                            Console.Error.WriteLine("Exception detected from " + hosts[i] + ": " + trickle);
                            SetFailure();
                            return;
                        }
                        sb.Append(trickle);
                        System.IO.File.Delete(np);
                    }
                }
                catch
                {
                    Console.WriteLine("Unable to check user logs on '{0}'", hosts[i]);
                }
            }

            if (0 != sb.Length)
            {
                Console.WriteLine("{0}", sb.ToString());
            }
        }


        /*
        public static bool WildcardMatch(string wc, string str)
        {
            string srex = System.Text.RegularExpressions.Regex.Escape(wc).Replace(@"\*", @".*");
            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            return rex.IsMatch(str);
        }
         * */



        public static List<string> SplitInputPaths(dfs dc, string pathlist, bool StripRecordInfo)
        {
            return dc.SplitInputPaths(pathlist, StripRecordInfo);
        }

        public static List<string> SplitInputPaths(dfs dc, string pathlist)
        {
            return dc.SplitInputPaths(pathlist);
        }


        public static string DurationString(int secs)
        {
            int mins = secs / 60;
            int hrs = mins / 60;
            string srhrs = (mins / 60).ToString().PadLeft(2, '0');
            string srmins = (mins % 60).ToString().PadLeft(2, '0');
            string srsecs = (secs % 60).ToString().PadLeft(2, '0');
            return srhrs + ":" + srmins + ":" + srsecs;
        }


        public static void OutputDuration(int secs)
        {
            ConsoleColor oldcolor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("{0}{1}{2}", isdspace? "\u00011" : "", DurationString(secs), isdspace ? "\u00010" : "");
            Console.ForegroundColor = oldcolor;
        }


        internal static string ExecArgsCode(IList<string> args)
        {
            StringBuilder result = new StringBuilder();
            foreach (string earg in args)
            {
                if (0 != result.Length)
                {
                    result.Append(',');
                }
                result.Append("@`");
                result.Append(earg);
                result.Append('`');
            }
            return result.ToString();
        }


        public static readonly string CommonDynamicCsCode = MySpace.DataMining.DistributedObjects.CommonCs.CommonDynamicCsCode;

        private static int maxuserlogs = 0;
        public static void Exec(string ExecOpts, SourceCode cfg, string[] ExecArgs, bool verbose, bool verbosereplication)
        {
            {
                dfs dc = LoadDfsConfig();
                MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_Hosts = dc.Slaves.SlaveList.Split(';');
                MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_MaxDGlobals = dc.MaxDGlobals;
                maxuserlogs = dc.MaxUserLogs;
            }            

            Environment.CurrentDirectory = AELight_Dir;
            try
            {
                foreach (SourceCode.Job cfgj in cfg.Jobs)
                {
                    if (null == cfgj.IOSettings)
                    {
                        LogOutput("IOSettings required in jobs file");
                        return;
                    }

                    DateTime start = DateTime.Now;

                    if (0 == string.Compare("mapreduce", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                    {
                        ExecOneMapReduce(ExecOpts, cfgj, ExecArgs, verbose, verbosereplication);
                    }
                    else if (0 == string.Compare("remote", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                    {
                        ExecOneRemote(cfgj, ExecArgs, verbose, verbosereplication);
                    }
                    else if (0 == string.Compare("remotemulti", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                    {
                        //ExecOneRemoteMulti(cfgj, ExecArgs, verbose);
                        throw new Exception("RemoteMulti no longer supported");
                    }
                    else if (0 == string.Compare("local", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                    {
                        if (cfgj.SuppressDefaultOutput != null)
                        {
                            verbose = false;
                        }
                        ExecOneLocal(cfgj, ExecArgs, verbose);
                    }
                    else if (0 == string.Compare("test", cfgj.IOSettings.JobType, StringComparison.OrdinalIgnoreCase))
                    {
                        ExecOneTest(cfgj, ExecArgs, verbose);
                    }
                    else
                    {
                        LogOutput(" <!> Unknown job type ''" + cfgj.IOSettings.JobType + "");
                        return;
                    }

                    if (verbose)
                    {
                        Console.Write("Duration: ");
                        OutputDuration((int)Math.Round(((DateTime.Now - start).TotalSeconds)));
                        Console.WriteLine();
                    }

                    //ReplicationPhase(verbosereplication);

                    System.GC.Collect();
                    System.GC.WaitForPendingFinalizers();
                }
            }
            finally
            {
                Environment.CurrentDirectory = OriginalUserDir;
            }
        }

        public static void Exec(string ExecOpts, SourceCode cfg, string[] ExecArgs, bool verbose)
        {
            Exec(ExecOpts, cfg, ExecArgs, verbose, verbose);
        }


        static bool _CanClientExceptionFailover_top(Exception e)
        {
            if (null != e as System.IO.DirectoryNotFoundException
                || null != e as System.IO.DriveNotFoundException
                || null != e as System.IO.EndOfStreamException
                || null != e as System.IO.FileNotFoundException
                || null != e as System.IO.PathTooLongException
                || null != e as System.UnauthorizedAccessException
                || null != e as System.Security.SecurityException
                || null != e as System.IO.InvalidDataException
                || null != e as System.ObjectDisposedException
                || null != e as System.NotSupportedException
                )
            {
                return false; // !
            }
            if (null != e as System.Net.Sockets.SocketException
                || null != e as System.Security.SecurityException
                || null != e as System.UnauthorizedAccessException
                || null != e as System.IO.IOException
                )
            {
                return true;
            }
            return false;
        }

        public static bool CanClientExceptionFailover(Exception e)
        {
            return _CanClientExceptionFailover_top(e)
                || _CanClientExceptionFailover_top(e.InnerException);
        }


        public class ExceptionFailoverRetryable : Exception
        {

            public ExceptionFailoverRetryable(Exception innerException, bool ShouldRetry)
                : base(innerException.Message, innerException)
            {
                this.ShouldRetry = ShouldRetry;
            }


            public bool ShouldRetry;

        }


        public static string GenerateZdFileDataNodeName(string dfspath)
        {
            return dfs.GenerateZdFileDataNodeName(dfspath, jid);
        }


        public static string GenerateZdFileDataNodeBaseName(string dfspath)
        {
            string basefn = GenerateZdFileDataNodeName(dfspath); // Has JID in it.
            int ipos = basefn.IndexOf('.') + 1;
            return basefn.Substring(0, ipos) + "%n." + basefn.Substring(ipos);
        }


        public static void EnterAdminLock()
        {
            using (System.Threading.Mutex lm = new System.Threading.Mutex(false, "DOexeclog"))
            {
                lm.WaitOne(); // Lock also taken by kill.
                try
                {
                    string exclfn = AELight_Dir + @"\excl.dat";
                    string excltext = "persist=" + douser + Environment.NewLine;
                    System.IO.File.WriteAllText(exclfn, excltext);
                }
                finally
                {
                    lm.ReleaseMutex();
                }
            }
        }

        public static bool LeaveAdminLock()
        {
            using (System.Threading.Mutex lm = new System.Threading.Mutex(false, "DOexeclog"))
            {
                lm.WaitOne(); // Lock also taken by kill.
                try
                {
                    string exclfn = AELight_Dir + @"\excl.dat";
                    if (System.IO.File.Exists(exclfn))
                    {
                        System.IO.File.Delete(exclfn);
                        lm.ReleaseMutex();
                        return true;
                    }
                }
                catch
                {
                }
                lm.ReleaseMutex();
                return false;
            }
        }


        static bool IsAdminCmd = false; // Only set by EnterAdminCmd()

        public static void EnterAdminCmd()
        {
            EnterAdminCmd(false);
        }

        public static void EnterAdminCmd(bool BypassJobs)
        {
#if CLIENT_LOG_ALL
            LogOutputToFile("CLIENT_LOG_ALL: EnterAdminCmd(BypassJobs=" + BypassJobs.ToString() + ")");
#endif
            using (System.Threading.Mutex lm = new System.Threading.Mutex(false, "DOexeclog"))
            {
                lm.WaitOne(); // Lock also taken by kill.

                string lockfn = AELight_Dir + @"\execlock.dat";
                if (System.IO.File.Exists(lockfn))
                {
                    lm.ReleaseMutex();
                    {
                        /*ConsoleColor oldc = ConsoleColor.Gray;
                        if (!isdspace)
                        {
                            oldc = Console.ForegroundColor;
                            Console.ForegroundColor = ConsoleColor.Red;
                        }*/
                        //Console.Error.WriteLine("{0}An administrative task is already in progress; please try again later{1}", isdspace ? "\u00014" : "", isdspace ? "\u00010" : ""); // relayed stderr doesn't process color codes.
                        Console.Error.WriteLine("An administrative task is already in progress; please try again later");
                        /*if (!isdspace)
                        {
                            Console.ForegroundColor = oldc;
                        }*/
                    }
                    SetFailure();
                    return;
                }

                int thispid = System.Diagnostics.Process.GetCurrentProcess().Id;
                string sthispid = thispid.ToString();

                int jobsrunning = 0;
                bool foundselfjob = false;

                string qfn = AELight_Dir + @"\execq.dat";
                string qfnbackup = qfn + "$";
                if (System.IO.File.Exists(qfn))
                {
                    string selflinestart = sthispid + " " + sjid + " ";
                    System.IO.File.Copy(qfn, qfnbackup, true);
                    using (System.IO.StreamReader fsin = new System.IO.StreamReader(qfnbackup))
                    {
                        using (System.IO.StreamWriter fsout = new System.IO.StreamWriter(qfn))
                        {
                            string line;
                            while (null != (line = fsin.ReadLine()))
                            {
                                line = line.Trim();
                                if (line.Length != 0)
                                {
                                    int pid = int.Parse(line.Substring(0, line.IndexOf(' ')));
                                    try
                                    {
                                        System.Diagnostics.Process proc = System.Diagnostics.Process.GetProcessById(pid);
                                        jobsrunning++;
                                        fsout.WriteLine(line);
                                        if (line.StartsWith(selflinestart))
                                        {
                                            foundselfjob = true;
                                        }
                                    }
                                    catch (System.ArgumentException e)
                                    {
                                        // System.ArgumentException: The process specified by the processId parameter is not running.
                                        // Don't copy this line through; the process isn't actually running anymore.
#if CLIENT_LOG_ALL
                                        LogOutputToFile("CLIENT_LOG_ALL: EnterAdminCmd: ArgumentException: " + e.ToString());
#endif
                                    }
                                }
                            }
                        }
                    }
                }

                if (!BypassJobs)
                {
                    if (jobsrunning - (foundselfjob ? 1 : 0) > 0)
                    {
                        lm.ReleaseMutex();
                        Console.Error.WriteLine("{0} jobs are currently running", jobsrunning);
                        ConsoleColor oldc = Console.ForegroundColor;
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.Error.WriteLine("Cannot perform an administrative task while jobs are running");
                        Console.ForegroundColor = oldc;
                        SetFailure();
                    }
                }

                string locktext = sthispid + Environment.NewLine;

                System.IO.File.WriteAllText(lockfn, locktext);

                IsAdminCmd = true; // !

                lm.ReleaseMutex();
            }
        }

        static bool doexcl
        {
            get
            {
                if (null == douser)
                {
                    return false;
                }
                return 0 == CompareUsers("drule", douser)
                    || 0 == CompareUsers("clok", douser)
                    || 0 == CompareUsers("cmiller", douser);
            }
        }

        static void _LeaveAdminCmd_unlocked()
        {
#if CLIENT_LOG_ALL
            LogOutputToFile("CLIENT_LOG_ALL: _LeaveAdminCmd_unlocked");
#endif
            System.IO.File.Delete(AELight_Dir + @"\execlock.dat");
        }


        static void _CleanPidFile_unlocked()
        {
#if CLIENT_LOG_ALL
            LogOutputToFile("CLIENT_LOG_ALL: _CleanPidFile_unlocked");
#endif
            if (null != pidfile)
            {
                pidfile.Close();
                try
                {
                    System.IO.File.Delete(pidfilename);
                }
                catch(Exception e)
                {
                    LogOutputToFile(e.ToString());
                }
                pidfile = null;
            }
        }

        static void _CleanJidFile_unlocked()
        {
#if CLIENT_LOG_ALL
            LogOutputToFile("CLIENT_LOG_ALL: _CleanJidFile_unlocked");
#endif
            try
            {
                System.IO.File.Delete(jidfilename);
            }
            catch (Exception e)
            {
                LogOutputToFile(e.ToString());
            }
        }


        static IList<string> ExcludeUnhealthySlaveMachines(IList<string> slaves, List<string> removed, bool verbose, int nthreads)
        {
            List<string> result = new List<string>(slaves.Count);
            result.AddRange(slaves);
            //for (int si = 0; si < slaves.Count; si++)
            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string slave)
                {
                    //string slave = slaves[si];
                    string reason;
                    if (!Surrogate.IsHealthySlaveMachine(slave, out reason))
                    {
                        lock (slaves)
                        {
                            if (null != removed)
                            {
                                removed.Add(slave);
                            }
                            result.Remove(slave);
                            if (verbose)
                            {
                                //lock (slaves)
                                {
                                    LogOutputToFile("Excluding '" + slave + "': " + reason);
                                    {
                                        ConsoleColor oldc = ConsoleColor.Gray;
                                        if (!isdspace)
                                        {
                                            oldc = Console.ForegroundColor;
                                            Console.ForegroundColor = ConsoleColor.Red;
                                        }
                                        Console.WriteLine("{0}Warning: excluding '{1}', unable to connect{2}", isdspace ? "\u00014" : "", slave, isdspace ? "\u00010" : "");
                                        if (!isdspace)
                                        {
                                            Console.ForegroundColor = oldc;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ), slaves, nthreads);
            return result;
        }

        static IList<string> ExcludeUnhealthySlaveMachines(IList<string> slaves, List<string> removed, bool verbose)
        {
            return ExcludeUnhealthySlaveMachines(slaves, removed, verbose, slaves.Count);
        }

        static IList<string> ExcludeUnhealthySlaveMachines(IList<string> slaves, bool verbose)
        {
            return ExcludeUnhealthySlaveMachines(slaves, null, verbose);
        }


        static IList<string> RemoveFromHosts(IList<string> remove, IList<string> hosts)
        {
            List<string> result = new List<string>();
            for (int ih = 0; ih < hosts.Count; ih++)
            {
                for (int ir = 0; ir < remove.Count; ir++)
                {
                    if (0 != string.Compare(IPAddressUtil.GetName(remove[ir]), IPAddressUtil.GetName(hosts[ih]), StringComparison.OrdinalIgnoreCase))
                    {
                        result.Add(hosts[ih]);
                    }
                }
            }
            return result;
        }

        static string RemoveFromHosts(IList<string> remove, string hosts)
        {
            return string.Join(";", RemoveFromHosts(remove, hosts.Split(';')).ToArray());
        }

        static string RemoveFromHosts(string remove, string hosts)
        {
            List<string> xremove = new List<string>(1);
            xremove.Add(remove);
            return string.Join(";", RemoveFromHosts(xremove, hosts.Split(';')).ToArray());
        }


        // Warning: does not care who initialized the admin lock!
        static void _LeaveAdminCmd()
        {
            using (System.Threading.Mutex lm = new System.Threading.Mutex(false, "DOexeclog"))
            {
                _LeaveAdminCmd_unlocked();
            }
        }


        public static bool CleanExecQ(int pidClean, long jidClean)
        {
            bool result = false;
            using (System.Threading.Mutex lm = new System.Threading.Mutex(false, "DOexeclog"))
            {
                lm.WaitOne(); // Lock also taken by kill.

                if (IsAdminCmd)
                {
                    try
                    {
                        _LeaveAdminCmd_unlocked();
                    }
                    catch(Exception e)
                    {
#if CLIENT_LOG_ALL
                        LogOutputToFile("CLIENT_LOG_ALL: CleanExecQ error: " + e.ToString());
#endif
                    }
                }

                _CleanPidFile_unlocked();

                _CleanJidFile_unlocked();

                string fn = AELight_Dir + @"\execq.dat";
                string fnNew = fn + "$";
                if (System.IO.File.Exists(fn))
                {
                    System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(
                        @"^\d+ " + jidClean.ToString() + " ",
                        System.Text.RegularExpressions.RegexOptions.Compiled
                        | System.Text.RegularExpressions.RegexOptions.Singleline);
                    using (System.IO.StreamReader fsin = new System.IO.StreamReader(fn))
                    {
                        using (System.IO.StreamWriter fsout = new System.IO.StreamWriter(fnNew))
                        {
                            string line;
                            while (null != (line = fsin.ReadLine()))
                            {
                                if (rex.IsMatch(line))
                                {
                                    if (pidClean >= 1)
                                    {
                                        string linestart = pidClean.ToString() + " " + jidClean.ToString() + " ";
                                        if (line.StartsWith(linestart))
                                        {
                                            result = true;
                                        }
                                    }
                                    else
                                    {
                                        result = true;
                                    }
                                }
                                else
                                {
                                    fsout.WriteLine(line);
                                }
                            }
                            fsout.Close();
                        }
                        fsin.Close();
                    }
                    {
                        System.IO.File.Delete(fn);
                        System.IO.File.Move(fnNew, fn);
                    }
                }

                lm.ReleaseMutex();
            }
            return result;
        }


        public static void CleanThisExecQ()
        {
            if (0 == jid)
            {
                return;
            }
            bool cleaned = CleanExecQ(System.Diagnostics.Process.GetCurrentProcess().Id, jid);
#if DEBUG
            if (!cleaned)
            {
                Console.Error.WriteLine("DEBUG:  Warning: CleanThisExecQ: (!cleaned)");
            }
#endif
        }


        // A normal failure occured; not considered abrupt.
        // e.g. bad user input, expected file not found, etc.
        public static void SetFailure()
        {
#if DEBUG
            if (System.Threading.Thread.CurrentThread.ManagedThreadId != MainThreadId)
            {
                Console.Error.WriteLine("DEBUG:  SetFailure: (System.Threading.Thread.CurrentThread.ManagedThreadId != MainThreadId)");
                Environment.Exit(0xf00);
            }
#endif
            System.Threading.Thread.CurrentThread.Abort("SetFailure{5653B981-4E2F-4b79-ACB5-8D504644D569}");
        }


        static int StreamReadLoop(System.IO.Stream stm, byte[] buf, int len)
        {
            int sofar = 0;
            try
            {
                while (sofar < len)
                {
                    int xread = stm.Read(buf, sofar, len - sofar);
                    if (xread <= 0)
                    {
                        break;
                    }
                    sofar += xread;
                }
                return sofar;
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException("StreamRead* Requested " + len.ToString() + " bytes; " + e.Message, e);
            }
        }

        static void StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
        {
            if (len != StreamReadLoop(stm, buf, len))
            {
                throw new System.IO.IOException("Unable to read from stream");
            }
        }


        static string[] SubArray(string[] arr, int startindex, int length)
        {
            string[] result = new string[length];
            for (int i = 0; i < length; i++)
            {
                result[i] = arr[startindex + i];
            }
            return result;
        }

        static string[] SubArray(string[] arr, int startindex)
        {
            return SubArray(arr, startindex, arr.Length - startindex);
        }


        public static long ParseLongCapacity(string capacity)
        {
            try
            {
                if (null == capacity || 0 == capacity.Length)
                {
                    throw new Exception("Invalid capacity: capacity not specified");
                }
                if ('-' == capacity[0])
                {
                    throw new FormatException("Invalid capacity: negative");
                }
                switch (capacity[capacity.Length - 1])
                {
                    case 'B':
                        if (1 == capacity.Length)
                        {
                            throw new Exception("Invalid capacity: " + capacity);
                        }
                        switch (capacity[capacity.Length - 2])
                        {
                            case 'K': // KB
                                return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024;

                            case 'M': // MB
                                return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024;

                            case 'G': // GB
                                return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024;

                            case 'T': // TB
                                return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024 * 1024;

                            case 'P': // PB
                                return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024 * 1024 * 1024;

                            default: // Just bytes with B suffix.
                                return long.Parse(capacity.Substring(0, capacity.Length - 1));
                        }
                    //break;

                    default: // Assume just bytes without a suffix.
                        return long.Parse(capacity);
                }
            }
            catch (FormatException e)
            {
                throw new FormatException("Invalid capacity: bad format: '" + capacity + "' problem: " + e.ToString());
            }
            catch (OverflowException e)
            {
                throw new OverflowException("Invalid capacity: overflow: '" + capacity + "' problem: " + e.ToString());
            }
        }


        static int ParseCapacity(string capacity)
        {
            try
            {
                if (null == capacity || 0 == capacity.Length)
                {
                    throw new Exception("Invalid capacity: capacity not specified");
                }
                if ('-' == capacity[0])
                {
                    throw new FormatException("Invalid capacity: negative");
                }
                switch (capacity[capacity.Length - 1])
                {
                    case 'B':
                        if (1 == capacity.Length)
                        {
                            throw new Exception("Invalid capacity: " + capacity);
                        }
                        switch (capacity[capacity.Length - 2])
                        {
                            case 'K': // KB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024;

                            case 'M': // MB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024;

                            case 'G': // GB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024;

                            case 'T': // TB
                                //return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024 * 1024;
                                throw new OverflowException("Terabyte (TB) sizes not supported by this function");

                            case 'P': // PB
                                //return long.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024 * 1024 * 1024;
                                throw new OverflowException("Petabyte (PB) sizes not supported by this function");

                            default: // Just bytes with B suffix.
                                return int.Parse(capacity.Substring(0, capacity.Length - 1));
                        }
                    //break;

                    default: // Assume just bytes without a suffix.
                        return int.Parse(capacity);
                }
            }
            catch (FormatException e)
            {
                throw new FormatException("Invalid capacity: bad format: '" + capacity + "' problem: " + e.ToString());
            }
            catch (OverflowException e)
            {
                throw new OverflowException("Invalid capacity: overflow: '" + capacity + "' problem: " + e.ToString());
            }
        }


        class VerifyHostInfo
        {
            internal string myhost;
            internal System.Net.Sockets.NetworkStream nstm;
            internal string[] slaves;
            internal List<string> netpaths;
            internal System.Threading.Thread thread;
            internal bool fail = false;

            internal void firstthreadproc()
            {
                System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                try
                {
                    sock.Connect(myhost, 55900);
                }
                catch (Exception e)
                {
                    LogOutput(" <!> Unable to verify host permissions; unable to connect to DistributedObjects service on " + myhost + ": " + e.ToString() + "  [Note: ensure the Windows service is running]");
                    fail = true;
                    return;
                }
                nstm = new XNetworkStream(sock);

                nstm.WriteByte((byte)'d'); // Get current directory.
                if ((int)'+' != nstm.ReadByte())
                {
                    LogOutput(" <!> Unable to verify host permissions; " + myhost + " did not respond correctly");
                    fail = true;
                    return;
                }

                string dir = XContent.ReceiveXString(nstm, null);
                if (dir.Length < 3
                    || ':' != dir[1]
                    || '\\' != dir[2]
                    || !char.IsLetter(dir[0])
                    )
                {
                    LogOutput(" <!> Unable to verify host permissions; " + myhost + " responded with an unexpected directory: " + dir + "  [expecting X:\\...]");
                    fail = true;
                    return;
                }
                string netdir = @"\\" + myhost + @"\" + dir.Substring(0, 1) + @"$" + dir.Substring(2);
                lock (netpaths)
                {
                    netpaths.Add(netdir);
                }
            }

            internal void secondthreadproc()
            {
                //string netdir = netpaths[si];
                for (int j = 0; j < slaves.Length; j++)
                {
                    //if (si != j)
                    {
                        string rhost = slaves[j];
                        string rnetdir = netpaths[j];
                        nstm.WriteByte((byte)'v'); // Verify file permissions.
                        XContent.SendXContent(nstm, rnetdir + @"\" + Guid.NewGuid().ToString());
                        if ((int)'+' != nstm.ReadByte())
                        {
                            LogOutput(" <!> Permissions error when " + myhost + " accessed " + rhost + " at " + rnetdir + "  [update file permissions on " + rhost + "]");
                            fail = true;
                            return;
                        }
                    }
                }
            }
            
        }


        static bool VerifyHostPermissions(string[] hosts)
        {
            List<string> netpaths = new List<string>(hosts.Length);
            List<VerifyHostInfo> hinfos = new List<VerifyHostInfo>(hosts.Length);
            try
            {
                for (int si = 0; si < hosts.Length; si++)
                {
                    VerifyHostInfo hinfo = new VerifyHostInfo();
                    hinfo.myhost = hosts[si];
                    hinfo.netpaths = netpaths;
                    hinfo.slaves = hosts;
                    hinfos.Add(hinfo);
                    hinfo.thread = new System.Threading.Thread(new System.Threading.ThreadStart(hinfo.firstthreadproc));
                    hinfo.thread.Start();
                }
                for (int si = 0; si < hosts.Length; si++)
                {
                    VerifyHostInfo hinfo = hinfos[si];
                    hinfo.thread.Join();
                    if (hinfo.fail)
                    {
                        return false;
                    }
                }
                for (int si = 0; si < hosts.Length; si++)
                {
                    VerifyHostInfo hinfo = hinfos[si];
                    hinfo.thread = new System.Threading.Thread(new System.Threading.ThreadStart(hinfo.secondthreadproc));
                    hinfo.thread.Start();
                }
                for (int si = 0; si < hosts.Length; si++)
                {
                    VerifyHostInfo hinfo = hinfos[si];
                    hinfo.thread.Join();
                    if (hinfo.fail)
                    {
                        return false;
                    }
                }
                return true;
            }
            finally
            {
                for (int si = 0; si < hinfos.Count; si++)
                {
                    VerifyHostInfo hinfo = hinfos[si];
                    if (null != hinfo.nstm)
                    {
                        hinfo.nstm.Close();
                    }
                }
            }
        }


        //LoadDfsConfig
        static bool VerifyHostPermissions()
        {
            //System.Threading.Thread.Sleep(8000);

            if (!dfs.DfsConfigExists(DFSXMLPATH))
            {
                return true; // ...
            }

            dfs dc = null;
            try
            {
                dc = LoadDfsConfig();
            }
            catch(Exception e)
            {
                LogOutput(" <!> Unable to verify host permissions; unable to load DFS configuration: " + e.ToString());
                return false;
            }

            if (dc.Slaves.SlaveList.Length == 0)
            {
                //LogOutput("<!> Unable to verify host permissions; missing SlaveList");
                return true; // ... No hosts, so they all passed.
            }
            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
            return VerifyHostPermissions(slaves);
        }


        public static string GetMemoryStatusForHost(string host)
        {
            using (System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp))
            {
                try
                {
                    sock.Connect(host, 55900);
                }
                catch (Exception e)
                {
                    return null;
                }
                using (System.Net.Sockets.NetworkStream nstm = new XNetworkStream(sock))
                {
                    nstm.WriteByte((byte)'M');
                    return XContent.ReceiveXString(nstm, null);
                }
            }
        }


        public static int CompareUsers(string user1, string user2)
        {
            int ia;

            ia = user1.IndexOf('@');
            if (-1 != ia)
            {
                user1 = user1.Substring(0, ia);
            }

            ia = user2.IndexOf('@');
            if (-1 != ia)
            {
                user2 = user2.Substring(0, ia);
            }

            return string.Compare(user1, user2, StringComparison.OrdinalIgnoreCase);
        }


#if AELIGHT_TRACE
        static List<System.Threading.Thread> AELight_TraceThreads = new List<System.Threading.Thread>();

#if AELIGHT_TRACE_PORT
        //static I<int> AELight_TracePorts = null;
#endif
#endif

        protected internal static void AELight_StartTraceThread(System.Threading.Thread thd)
        {
#if AELIGHT_TRACE
            lock (AELight_TraceThreads)
            {
                AELight_TraceThreads.Add(thd);
            }
#endif
            thd.Start();
        }

        protected internal static void AELight_JoinTraceThread(System.Threading.Thread thd)
        {
            thd.Join();
#if AELIGHT_TRACE
            lock (AELight_TraceThreads)
            {
                AELight_TraceThreads.Remove(thd);
            }
#endif
        }


        public static string OriginalUserDir;
        public static string AELight_Dir;
        public static bool isdspace = false; // Is this running as Qizmt?
        public static string appname = "N/A";
        static string douser = null;
        static string dousername = null;
        static string userdomain = null;
        static System.IO.StreamWriter pidfile = null;
        static string pidfilename;
        static string jidfilename;
        public static bool DebugSwitch = false;
        public static bool DebugStepSwitch = false;


        internal const string jidfn = "jid.dat";
        internal static string jidfp = "N/A";
        public static long jid = 0;
        public static string sjid = "0";

#if DEBUG
        static int MainThreadId;
#endif

        static void Main(string[] args)
        {
#if AELIGHT_TRACE
            AELight_TraceThreads.Add(System.Threading.Thread.CurrentThread);

#if DEBUG
            //System.Diagnostics.Debugger.Launch();
#endif
            try
            {
                System.Threading.Thread stthd = new System.Threading.Thread(
                    new System.Threading.ThreadStart(
                    delegate
                    {
                        string spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
                        try
                        {
                            string dotracefile = spid + ".trace";
                            const string tracefiledelim = "{C8683F6C-0655-42e7-ACD9-0DDED6509A7C}";
                            for (; ; )
                            {
                                System.IO.StreamWriter traceout = null;
                                for (System.Threading.Thread.Sleep(1000 * 60)
                                    ; !System.IO.File.Exists(dotracefile)
                                    ; System.Threading.Thread.Sleep(1000 * 60))
                                {
                                }
                                {
                                    string[] tfc;
                                    try
                                    {
                                        tfc = System.IO.File.ReadAllLines(dotracefile);
                                    }
                                    catch
                                    {
                                        continue;
                                    }
                                    if (tfc.Length < 1 || "." != tfc[tfc.Length - 1])
                                    {
                                        continue;
                                    }
                                    try
                                    {
                                        System.IO.File.Delete(dotracefile);
                                    }
                                    catch
                                    {
                                        continue;
                                    }
                                    if ("." != tfc[0])
                                    {
                                        string traceoutfp = tfc[0];
                                        try
                                        {
                                            traceout = System.IO.File.CreateText(traceoutfp);
                                            traceout.Write("BEGIN:");
                                            traceout.WriteLine(tracefiledelim);
                                        }
                                        catch
                                        {
                                            continue;
                                        }
                                    }
                                }
                                if (null == traceout)
                                {
                                    LogOutputToFile("AELIGHT_TRACE: " + spid + " Start");
                                }
                                for (; ; System.Threading.Thread.Sleep(1000 * 60))
                                {
#if AELIGHT_TRACE_PORT
                                    if (null == traceout)
                                    {
                                        try
                                        {
                                            StringBuilder sbtp = new StringBuilder();
                                            sbtp.Append("AELIGHT_TRACE_PORT: " + spid + ":");
                                            int tpc = 0;
                                            {
                                                foreach (int atp in DistributedObjects5.DistObject.AllOpenPorts)
                                                {
                                                    sbtp.Append(' ');
                                                    sbtp.Append(atp);
                                                    tpc++;
                                                }
                                            }
                                            if (0 == tpc)
                                            {
                                                sbtp.Append(" None");
                                            }
                                            LogOutputToFile(sbtp.ToString());
                                        }
                                        catch
                                        {
                                        }
                                    }
#endif
                                    foreach (System.Threading.Thread tthd in AELight_TraceThreads)
                                    {
                                        string tr = "";
                                        try
                                        {
                                            bool thdsuspended = false;
                                            try
                                            {
                                                tthd.Suspend();
                                                thdsuspended = true;
                                            }
                                            catch (System.Threading.ThreadStateException)
                                            {
                                            }
                                            try
                                            {
                                                System.Diagnostics.StackTrace st = new System.Diagnostics.StackTrace(tthd, false);
                                                StringBuilder sbst = new StringBuilder();
                                                const int maxframesprint = 15;
                                                for (int i = 0, imax = Math.Min(maxframesprint, st.FrameCount); i < imax; i++)
                                                {
                                                    if (0 != sbst.Length)
                                                    {
                                                        sbst.Append(", ");
                                                    }
                                                    string mn = "N/A";
                                                    try
                                                    {
                                                        System.Reflection.MethodBase mb = st.GetFrame(i).GetMethod();
                                                        mn = mb.ReflectedType.Name + "." + mb.Name;
                                                    }
                                                    catch
                                                    {
                                                    }
                                                    sbst.Append(mn);
                                                }
                                                if (st.FrameCount > maxframesprint)
                                                {
                                                    sbst.Append(" ... ");
                                                    sbst.Append(st.FrameCount - maxframesprint);
                                                    sbst.Append(" more");
                                                }
                                                if (null == traceout)
                                                {
                                                    LogOutputToFile("AELIGHT_TRACE: " + spid + " " + tthd.Name + " Trace: " + sbst.ToString());
                                                }
                                                else
                                                {
                                                    traceout.Write("Thread ");
                                                    string tthdname = tthd.Name;
                                                    if (null == tthdname || 0 == tthdname.Length)
                                                    {
                                                        //tthdname = "<unnamed>";
                                                        tthdname = tthd.ManagedThreadId.ToString();
                                                    }
                                                    traceout.Write(tthdname);
                                                    traceout.Write(": ");
                                                    traceout.WriteLine(sbst.ToString());
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                LogOutputToFile("AELIGHT_TRACE: " + spid + " " + tthd.Name + " Error: " + e.ToString());
                                            }
                                            finally
                                            {
                                                if (thdsuspended)
                                                {
                                                    tthd.Resume();
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            LogOutputToFile("AELIGHT_TRACE: " + spid + " " + tthd.Name + " Trace Error: Cannot access thread: " + e.ToString());
                                        }
                                    }

                                    if (null != traceout)
                                    {
                                        traceout.Write(tracefiledelim);
                                        traceout.WriteLine(":END");
                                        traceout.Close();
                                        break;
                                    }
                                }

                            }
                        }
                        catch(Exception e)
                        {
                            LogOutputToFile("AELIGHT_TRACE: " + spid + " Trace Failure: " + e.Message);
                        }
                    }));
                stthd.IsBackground = true;
                stthd.Start();
            }
            catch (Exception est)
            {
                LogOutputToFile("AELIGHT_TRACE: Thread start error: " + est.ToString());
            }
#endif

#if DEBUG
            MainThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
#endif
            bool success = false;
            bool setfailure = false;
            try
            {
                try
                {
                    AELightRun(args);
                }
                catch (System.Threading.ThreadAbortException tae)
                {
                    if ((string)tae.ExceptionState == "SetFailure{5653B981-4E2F-4b79-ACB5-8D504644D569}")
                    {
                        setfailure = true;
                        System.Threading.Thread.ResetAbort();
                    }
                }
                CleanThisExecQ();
                success = true;
            }
            finally
            {
                if (!success)
                {
                    if (0 == jid)
                    {
                        // It wasn't even given a JID yet, so just ignore this.
                    }
                    else
                    {
                        ConsoleColor oldc = ConsoleColor.Gray;
                        if (!isdspace)
                        {
                            oldc = Console.ForegroundColor;
                            Console.ForegroundColor = ConsoleColor.Red;
                        }
                        Console.Error.WriteLine(Environment.NewLine
                            + "{0}Job aborted abruptly; to clean up intermediate data and processes, issue command: {3} kill {4} {2}",
                            (isdspace ? "\u00014" : ""), (isdspace ? "\u00010" : ""), appname, sjid);
                        if (!isdspace)
                        {
                            Console.ForegroundColor = oldc;
                        }
                    }
                }
            }
            if (setfailure)
            {
                Environment.Exit(0xf00);
            }
#if CLIENT_LOG_ALL
            LogOutputToFile("CLIENT_LOG_ALL: clean return from Main");
#endif
        }

        static void AELightRun(string[] args)
        {
            //if (Environment.GetEnvironmentVariable("DOSERVICE") != null)
            {
                Console.OutputEncoding = Encoding.UTF8;
            }
            try
            {
                isdspace = null != Environment.GetEnvironmentVariable("DSPACE_EXE");
            }
            catch
            {
            }
            if (isdspace)
            {
                appname = "Qizmt";
            }
            else
            {
                appname = "AELight";
            }            
            if (args.Length >= 1 && args[0].StartsWith("-$"))
            {
                appname = "Qizmt";
                isdspace = true;
                int del = args[0].IndexOf(@"\", 4);
                if (del > -1)
                {
                    userdomain = args[0].Substring(4, del - 4);
                    douser = args[0].Substring(del + 1);
                }
                else
                {
                    userdomain = Environment.UserDomainName;
                    douser = args[0].Substring(2);
                }
                del = douser.IndexOf("@");
                if (del > -1)
                {
                    dousername = douser.Substring(0, del);
                }
                else
                {
                    dousername = douser;
                }
                args = SubArray(args, 1);
            }
            else
            {
                userdomain = Environment.UserDomainName;
                dousername = Environment.UserName;
                douser = Environment.UserName + "@" + System.Net.Dns.GetHostName();                
            }            
            OriginalUserDir = Environment.CurrentDirectory;
            AELight_Dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            jidfp = AELight_Dir + @"\" + jidfn;

            if (args.Length > 0
                && args[0].StartsWith("-debug"))
            {
                DebugSwitch = true;
                DebugStepSwitch = ("-debug-step" == args[0]);
#if DEBUG
                //Console.WriteLine("DEBUG:  DebugSwitch = true");
#endif
                args = SubArray(args, 1);
            }

#if DEBUG
            if (args.Length > 0
                && "-ddebug" == args[0])
            {
                args = SubArray(args, 1);
                System.Diagnostics.Debugger.Launch();
            }
#endif

            if (0 == args.Length)
            {
                ShowUsage();
                return;
            }

            string act = args[0].ToLower();
            if (act.StartsWith("-"))
            {
                act = act.Substring(1);
            }
            {
                using (System.Threading.Mutex lm = new System.Threading.Mutex(false, "DOexeclog"))
                {
                    lm.WaitOne(); // Lock also taken by kill.

                    if (System.IO.File.Exists(AELight_Dir + @"\excl.dat"))
                    {
                        bool isadminlocked = false;
                        if (doexcl)
                        {
                            isadminlocked = false;
                        }
                        else
                        {
                            string[] lines = System.IO.File.ReadAllLines(AELight_Dir + @"\excl.dat");
                            if (lines.Length > 0 && null != lines[0])
                            {
                                if (lines[0].StartsWith("persist=", StringComparison.OrdinalIgnoreCase))
                                {
                                    string adminlockuser = lines[0].Substring(8);
                                    if (0 == CompareUsers(adminlockuser, douser))
                                    {
                                        isadminlocked = false;
                                    }
                                }
                            }
                        }
                        if (isadminlocked)
                        {
                            lm.ReleaseMutex();
                            Console.Error.WriteLine("  {0} is currently admin-locked; please try again later", appname);
                            SetFailure();
                            return;
                        }
                    }

                    if (System.IO.File.Exists(AELight_Dir + @"\execlock.dat"))
                    {
                        bool islocked = true;
                        string[] lines = System.IO.File.ReadAllLines(AELight_Dir + @"\execlock.dat");
                        int pidLock = int.Parse(lines[0]);

                        {
                            try
                            {
                                System.Diagnostics.Process slaveproc = System.Diagnostics.Process.GetProcessById(pidLock);
                            }
                            catch (System.ArgumentException e)
                            {
#if CLIENT_LOG_ALL
                            LogOutputToFile("CLIENT_LOG_ALL: execlock: GetProcessById: ArgumentException: " + e.ToString());
#endif
                                // System.ArgumentException: The process specified by the processId parameter is not running.
                                islocked = false;
                                try
                                {
                                    System.IO.File.Delete(AELight_Dir + @"\execlock.dat");
                                }
                                catch
                                {
                                    islocked = true;
                                }
                            }
                        }

                        if (islocked) // If still locked out from this user...
                        {
                            lm.ReleaseMutex();
                            switch (act)
                            {
                                case "ps":
                                    SafePS();
                                    break;
                                case "perfmon":
                                    Perfmon.SafeGetCounters(SubArray(args, 1));
                                    break;
                                case "packetsniff":
                                    SafePacketSniff(args);
                                    break;
                                default:
                                    Console.Error.WriteLine("  {0} is currently locked for administrative tasks; please try again later", appname);
                                    SetFailure();
                                    break;
                            }
                            return; // !
                        }
                    }

                    {
                        // Acquire JID.
                        if (!System.IO.File.Exists(jidfp))
                        {
                            jid = 1;
                            sjid = jid.ToString();
                        }
                        else
                        {
                            string soldjid = System.IO.File.ReadAllText(jidfp).Trim();
                            long oldjid = long.Parse(soldjid);
                            long newjid = unchecked(oldjid + 1);
                            if (newjid < 1)
                            {
                                newjid = 1;
                                LogOutputToFile(string.Format("Warning: JID overflow: {0} to {1}", soldjid, newjid));
                            }
                            jid = newjid;
                            sjid = jid.ToString();
                        }
                        System.IO.File.WriteAllText(jidfp, sjid);
                    }

                    try
                    {
                        string sargs = "";
                        {
                            StringBuilder sb = new StringBuilder(1000);
                            for (int i = 0; i < args.Length; i++)
                            {
                                if (0 != sb.Length)
                                {
                                    sb.Append(' ');
                                }
                                if (-1 != args[i].IndexOf(' '))
                                {
                                    sb.Append('"');
                                    sb.Append(args[i].Replace("\"", "\\\""));
                                    sb.Append('"');
                                }
                                else
                                {
                                    sb.Append(args[i].Replace("\"", "\\\""));
                                }
                            }
                            sargs = sb.ToString();
                        }
                        if (-1 != sargs.IndexOf("73045AA6-2F6B-4166-BDE2-806F1E43854B"))
                        {
                            sargs = "\t" + sargs.GetHashCode();
                        }

                        string sayuser = Environment.UserName;
                        if (null != douser)
                        {
                            sayuser = douser + "(" + sayuser + ")";
                        }

                        {
                            const int iMAX_SECS_RETRY = 10; // Note: doesn't consider the time spent waiting on I/O.
                            const int ITER_MS_WAIT = 100; // Milliseconds to wait each retry.
                            int iters = iMAX_SECS_RETRY * 1000 / ITER_MS_WAIT;
                            for (; ; )
                            {
                                try
                                {
                                    System.IO.File.AppendAllText(AELight_Dir + @"\execlog.txt",
                                        sayuser
                                        + " [" + System.DateTime.Now.ToString() + "] "
                                        //+ OriginalUserDir + ">"
                                        + appname
                                        + " " + sargs
                                        + Environment.NewLine);
                                    break;
                                }
                                catch
                                {
                                    if (--iters < 0)
                                    {
                                        throw;
                                    }
                                    System.Threading.Thread.Sleep(ITER_MS_WAIT);
                                    continue;
                                }
                            }
                        }

                        {
                            const int iMAX_SECS_RETRY = 10; // Note: doesn't consider the time spent waiting on I/O.
                            const int ITER_MS_WAIT = 100; // Milliseconds to wait each retry.
                            int iters = iMAX_SECS_RETRY * 1000 / ITER_MS_WAIT;
                            for (; ; )
                            {
                                try
                                {
                                    System.IO.File.AppendAllText(AELight_Dir + @"\execq.dat",
                                        System.Diagnostics.Process.GetCurrentProcess().Id.ToString()
                                        + " " + sjid
                                        + " " + act
                                        + " +++"
                                        + " " + sayuser
                                        + " [" + System.DateTime.Now.ToString() + "] "
                                        //+ OriginalUserDir + ">"
                                        + appname
                                        + " " + sargs
                                        + Environment.NewLine);
                                    break;
                                }
                                catch
                                {
                                    if (--iters < 0)
                                    {
                                        throw;
                                    }
                                    System.Threading.Thread.Sleep(ITER_MS_WAIT);
                                    continue;
                                }
                            }
                        }

                        {
                            int pid = System.Diagnostics.Process.GetCurrentProcess().Id;
                            string spid = pid.ToString();
                            pidfilename = AELight_Dir + @"\" + spid + ".aelight.pid";
                            pidfile = new System.IO.StreamWriter(pidfilename);
                            pidfile.WriteLine(spid);
                            pidfile.WriteLine(System.DateTime.Now);
                            pidfile.WriteLine("jid={0}", sjid);
                            pidfile.Flush();

                            {
                                jidfilename = AELight_Dir + @"\" + sjid + ".jid";
                                System.IO.StreamWriter sw = new System.IO.StreamWriter(jidfilename);
                                sw.WriteLine(sjid);
                                sw.WriteLine(System.DateTime.Now);
                                sw.WriteLine("pid={0}", spid);
                                //sw.Flush();
                                sw.Close();
                            }
                        }

                    }
                    finally
                    {
                        lm.ReleaseMutex();
                    }
                }
            }
            try
            {
                System.Diagnostics.Process.GetCurrentProcess().PriorityClass = System.Diagnostics.ProcessPriorityClass.AboveNormal;
            }
            catch
            {
                Console.Error.WriteLine("Warning: unable to change priority class of {0} process", appname);
            }

            bool bypasshostverify = true;
            if (args.Length >= 1 && args[0] == "-nv")
            {
                bypasshostverify = true;
                args = SubArray(args, 1);
            }
            if (args.Length >= 1 && args[0] == "-v")
            {
                bypasshostverify = false;
                args = SubArray(args, 1);
            }
            if (args.Length > 2 && (0 == string.Compare(args[0], "-dfs", StringComparison.OrdinalIgnoreCase) || 0 == string.Compare(args[0], "dfs", StringComparison.OrdinalIgnoreCase))
                && args[1].Contains("format"))
            {
                bypasshostverify = true;
            }
            if (!bypasshostverify)
            {
                if (!VerifyHostPermissions())
                {
                    Console.Error.WriteLine("Host permissions verification error; aborting");
                    return;
                }
            }
            switch (act)
            {

                case "enqueue":
                    {
                        MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.QEntry qe;
                        try
                        {
#if DEBUG
                            //System.Threading.Thread.Sleep(1000 * 8);
#endif
                            qe = MySpace.DataMining.DistributedObjects.Scheduler.Enqueue(SubArray(args, 1), douser);
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine(e.Message);
                            SetFailure();
                            return;
                        }
                        Console.WriteLine("Queue Identifier: {0}", qe.ID);
                        Console.WriteLine("Enqueued: {0}", qe.Command);
                        Console.WriteLine("Position: {0}", MySpace.DataMining.DistributedObjects.Scheduler.GetQueueSnapshot().Count);
                    }
                    break;

                case "queuekill":
                    {
                        if (args.Length <= 1)
                        {
                            Console.Error.WriteLine("Error: expected QID");
                            SetFailure();
                            return;
                        }
                        string sqid = args[1];
                        long qid;
                        try
                        {
                            qid = long.Parse(sqid);
                            if (qid <= 0)
                            {
                                throw new Exception("Must be greater than 0");
                            }
                            //sqid = qid.ToString(); // Normalize.
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine("Invalid QID '{0}': {1}", sqid, e.Message);
                            SetFailure();
                            return;
                        }
                        if (!MySpace.DataMining.DistributedObjects.Scheduler.QueueKill(qid))
                        {
                            Console.Error.WriteLine("No such QID: {0}", sqid);
                            SetFailure();
                            return;
                        }
                        Console.WriteLine("Done");
                    }
                    break;

                case "clearqueue":
                    {
                        MySpace.DataMining.DistributedObjects.Scheduler.ClearQueue();
                        Console.WriteLine("Queue cleared");
                    }
                    break;

                case "schedule":
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif
                        MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry se;
                        try
                        {
                            se = MySpace.DataMining.DistributedObjects.Scheduler.Schedule(SubArray(args, 1), douser);
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine(e.Message);
                            SetFailure();
                            return;
                        }
                        Console.WriteLine("Schedule Identifier: {0}", se.ID);
                        Console.WriteLine("Scheduled: {0}", se.Command);
                        Console.WriteLine("First Run: {0}", se.NextRun);
#if DEBUG
                        if (!string.IsNullOrEmpty(se.texceptions))
                        {
                            DateTime dt = DateTime.MaxValue;
                            List<MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry.TimeSpec.Range> xrs
                                = MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry.ParseTExceptions(
                                se.texceptions, se.NextRun);
                            foreach (MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry.TimeSpec.Range xr
                                in xrs)
                            {
                                if (xr.first < dt)
                                {
                                    dt = xr.first;
                                }
                            }
                            Console.WriteLine("DEBUG First texception: {0}", dt);
                        }
#endif
#if DEBUG
                        if (!string.IsNullOrEmpty(se.wtexceptions))
                        {
                            DateTime dt = DateTime.MaxValue;
                            List<MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry.TimeSpec.Range> xrs
                                = MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry.ParseTExceptions(
                                se.wtexceptions, se.NextRun);
                            foreach (MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry.TimeSpec.Range xr
                                in xrs)
                            {
                                if (xr.first < dt)
                                {
                                    dt = xr.first;
                                }
                            }
                            Console.WriteLine("DEBUG First wtexception: {0}", dt);
                        }
#endif
                    }
                    break;

                case "pauseschedule":
                case "schedulepause":
                    {
                        if (args.Length <= 1)
                        {
                            Console.Error.WriteLine("Error: expected SID");
                            SetFailure();
                            return;
                        }
                        string ssid = args[1];
                        long sid;
                        try
                        {
                            sid = long.Parse(ssid);
                            if (sid <= 0)
                            {
                                throw new Exception("Must be greater than 0");
                            }
                            //ssid = sid.ToString(); // Normalize.
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine("Invalid SID '{0}': {1}", ssid, e.Message);
                            SetFailure();
                            return;
                        }
                        if (!MySpace.DataMining.DistributedObjects.Scheduler.PauseSchedule(sid, true))
                        {
                            Console.Error.WriteLine("No such SID: {0}", ssid);
                            SetFailure();
                            return;
                        }
                        Console.WriteLine("Done");
                    }
                    break;

                case "unpauseschedule":
                case "scheduleunpause":
                    {
                        if (args.Length <= 1)
                        {
                            Console.Error.WriteLine("Error: expected SID");
                            SetFailure();
                            return;
                        }
                        string ssid = args[1];
                        long sid;
                        try
                        {
                            sid = long.Parse(ssid);
                            if (sid <= 0)
                            {
                                throw new Exception("Must be greater than 0");
                            }
                            //ssid = sid.ToString(); // Normalize.
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine("Invalid SID '{0}': {1}", ssid, e.Message);
                            SetFailure();
                            return;
                        }
                        if (!MySpace.DataMining.DistributedObjects.Scheduler.PauseSchedule(sid, false))
                        {
                            Console.Error.WriteLine("No such SID: {0}", ssid);
                            SetFailure();
                            return;
                        }
                        Console.WriteLine("Done");
                    }
                    break;

                case "unschedule":
                    {
                        if (args.Length <= 1)
                        {
                            Console.Error.WriteLine("Error: expected SID");
                            SetFailure();
                            return;
                        }
                        string ssid = args[1];
                        long sid;
                        try
                        {
                            sid = long.Parse(ssid);
                            if (sid <= 0)
                            {
                                throw new Exception("Must be greater than 0");
                            }
                            //ssid = sid.ToString(); // Normalize.
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine("Invalid SID '{0}': {1}", ssid, e.Message);
                            SetFailure();
                            return;
                        }
                        if (!MySpace.DataMining.DistributedObjects.Scheduler.Unschedule(sid))
                        {
                            Console.Error.WriteLine("No such SID: {0}", ssid);
                            SetFailure();
                            return;
                        }
                        Console.WriteLine("Done");
                    }
                    break;

                case "clearschedule":
                    {
                        MySpace.DataMining.DistributedObjects.Scheduler.ClearSchedule();
                        Console.WriteLine("Schedule cleared");
                    }
                    break;

                case "spread":
                    {
                        if (args.Length <= 2)
                        {
                            Console.Error.WriteLine("Expected: {0} spread <input-dfsfile> <output-dfsfile>", appname);
                            SetFailure();
                            return;
                        }

                        string infn = args[1];
                        if (infn.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                        {
                            infn = infn.Substring(6);
                        }
                        if (-1 != infn.IndexOf('@'))
                        {
                            Console.Error.WriteLine("Record length not expected: {0}", infn);
                            SetFailure();
                            return;
                        }
                        string outfn = args[2];
                        if (outfn.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                        {
                            outfn = outfn.Substring(6);
                        }
                        if (-1 != outfn.IndexOf('@'))
                        {
                            Console.Error.WriteLine("Record length not expected: {0}", outfn);
                            SetFailure();
                            return;
                        }

                        dfs dc = LoadDfsConfig();

                        dfs.DfsFile inf = dc.FindAny(infn);
                        if (null == inf)
                        {
                            Console.Error.WriteLine("Input file not found in DFS: {0}", infn);
                            SetFailure();
                            return;
                        }
                        string dfsinput, dfsoutput;
                        if (inf.RecordLength > 0)
                        {
                            dfsinput = "dfs://" + infn + "@" + inf.RecordLength;
                            dfsoutput = "dfs://" + outfn + "@" + inf.RecordLength;
                        }
                        else
                        {
                            dfsinput = "dfs://" + infn;
                            dfsoutput = "dfs://" + outfn;
                        }

                        if (null != DfsFindAny(dc, outfn))
                        {
                            Console.Error.WriteLine("Output file already exists in DFS: {0}", outfn);
                            SetFailure();
                            return;
                        }

                        string tempfnpost = "." + Guid.NewGuid().ToString() + "." + System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
                        string jobsfn = "spread-jobs.xml" + tempfnpost;

                        try
                        {
                            using (System.IO.StreamWriter sw = System.IO.File.CreateText(jobsfn))
                            {
                                sw.Write((@"<SourceCode>
  <Jobs>
    <Job Name=`spread` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>" + dfsinput + @"</DFSInput>
        <DFSOutput>" + dfsoutput + @"</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            byte[] keybuf = null;
            ByteSlice keybs;
            int ikey;
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                if(null == keybuf)
                {
                    keybuf = new byte[4];
                    keybs = ByteSlice.Prepare(keybuf);
                    ikey = Qizmt_ProcessID;
                }
                Entry.ToBytes(ikey, keybuf, 0);
                output.Add(keybs, line);
                ikey = unchecked(ikey + 1);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                while(values.MoveNext())
                {
                    output.Add(values.Current);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
")
                                    .Replace('`', '"'));
                            }

                            Console.WriteLine("Spreading...");
                            //Exec("", LoadConfig(xpaths, jobsfn), new string[] { }, false, false);
                            Exec("", LoadConfig(jobsfn), new string[] { }, false, false);
                            Console.WriteLine();
                            Console.WriteLine("Successfully spread '{0}' to '{1}'", infn, outfn);
                        }
                        finally
                        {
                            try
                            {
                                System.IO.File.Delete(jobsfn);
                            }
                            catch
                            {
                            }
                        }
                    }
                    break;

                case "kill":
                case "killst":
                case "killmt":
                    {
                        bool singlethreaded = ("killst" == act);

                        if (args.Length > 1)
                        {
                            string killsjid = args[1];
                            long killjid;
                            try
                            {
                                killjid = long.Parse(killsjid);
                                if (killjid <= 0)
                                {
                                    throw new Exception("Must be greater than 0");
                                }
                                killsjid = killjid.ToString(); // Normalize.
                            }
                            catch (Exception e)
                            {
                                Console.Error.WriteLine("Invalid JID '{0}': {1}", killsjid, e.Message);
                                SetFailure();
                                return;
                            }
                            if (killjid == jid)
                            {
                                Console.WriteLine("Process suicide");
                                return;
                            }

                            {
                                bool qverbose = args.Length > 2 && "?" == args[2];
                                bool dotverbose = args.Length > 2 && "." == args[2];
                                int killAelightPid = 0;
                                string killAelightSPid = "0";
                                bool killjidexists = false;
                                using (System.Threading.Mutex killmutex = new System.Threading.Mutex(false, "DOkillj" + killsjid))
                                {
                                    killmutex.WaitOne(); // Can abandon if kill gets killed, but we should be alerted.
                                    string killsjidfp = AELight_Dir + @"\" + killsjid + ".jid";
                                    if (!System.IO.File.Exists(killsjidfp))
                                    {
                                        //killjidexists = false;
                                    }
                                    else
                                    {
                                        killjidexists = true;
                                        Console.WriteLine("Killing {0}: {1}", killsjid, "");
                                        dfs dc = LoadDfsConfig();
                                        string[] slaves = dc.Slaves.SlaveList.Split(';');
                                        int numthreads = 1;
                                        if (!singlethreaded)
                                        {
                                            numthreads = slaves.Length;
                                        }

                                        List<System.Threading.Mutex> mutexes = new List<System.Threading.Mutex>();
                                        bool mutexesNeedSafePoint = true;
                                        {
                                            mutexes.Add(new System.Threading.Mutex(false, "AEDFSM"));
                                            mutexes.Add(new System.Threading.Mutex(false, "DOexeclog"));
                                            // Also adding compiler mutex so that the frozen aelight being
                                            // killed doesn't prevent other processes from compiling...
                                            mutexes.Add(new System.Threading.Mutex(false, "DynCmp"));
                                        }

                                        string[] jidflines = null;
                                        try
                                        {
                                            jidflines = System.IO.File.ReadAllLines(killsjidfp);
                                        }
                                        catch (System.IO.FileNotFoundException)
                                        {
                                            // This can happen if the job finishes after the previous file check.
                                            killjidexists = false;
                                        }
                                        if (null != jidflines)
                                        {
                                            foreach (string ln in jidflines)
                                            {
                                                if (ln.StartsWith("pid="))
                                                {
                                                    killAelightSPid = ln.Substring(4);
                                                    killAelightPid = int.Parse(killAelightSPid);
                                                    killAelightSPid = killAelightPid.ToString(); // Normalize.
                                                    try
                                                    {
                                                        System.Diagnostics.Process xproc = System.Diagnostics.Process.GetProcessById(killAelightPid);
                                                        if (mutexesNeedSafePoint)
                                                        {
                                                            HogMutexes(true, mutexes);
                                                        }
                                                        try
                                                        {
                                                            foreach (System.Diagnostics.ProcessThread pt in xproc.Threads)
                                                            {
                                                                IntPtr hthd = OpenThread(0x2 /* suspend/resume */, false, (uint)pt.Id);
                                                                if (IntPtr.Zero == hthd)
                                                                {
                                                                    throw new Exception("Insufficient access to thread");
                                                                }
                                                                SuspendThread(hthd);
#if DEBUG
                                                                if (qverbose)
                                                                {
                                                                    lock (slaves)
                                                                    {
                                                                        Console.Write("(suspended thread {0})", pt.Id);
                                                                        ConsoleFlush();
                                                                    }
                                                                }
#endif
                                                            }
                                                            mutexesNeedSafePoint = false;
                                                        }
                                                        finally
                                                        {
                                                            HogMutexes(false, mutexes);
                                                        }
                                                        xproc.Close();
                                                    }
                                                    catch (Exception exf)
                                                    {
                                                        if (qverbose)
                                                        {
                                                            lock (slaves)
                                                            {
                                                                Console.Write("(Unable to suspend AELight threads: {0})", exf.Message);
                                                                ConsoleFlush();
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            System.Threading.Thread.Sleep(1000); // Allow slaves to initialize.
                                            {
                                                //foreach (string slave in slaves)
                                                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                                    new Action<string>(
                                                    delegate(string slave)
                                                    {
                                                        string netpath = Surrogate.NetworkPathForHost(slave);
                                                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath))
                                                            .GetFiles("*.j" + killsjid + ".slave.pid"))
                                                        {
                                                            string spidStopRemote = fi.Name.Substring(0, fi.Name.IndexOf('.'));
                                                            try
                                                            {
                                                                System.Net.Sockets.NetworkStream nstm = Surrogate.ConnectService(slave);
                                                                nstm.WriteByte((byte)'k');
                                                                XContent.SendXContent(nstm, spidStopRemote);
                                                                if ('+' != nstm.ReadByte())
                                                                {
                                                                    throw new Exception("Remote machine did not report a success during kill operation");
                                                                }
                                                                nstm.Close();
                                                                fi.Delete();
                                                            }
                                                            catch (Exception e)
                                                            {
                                                                LogOutputToFile("Unable to kill Slave PID " + netpath + "\\" + spidStopRemote + " belonging to JID " + killsjid + ": " + e.Message);
                                                            }
                                                        }
                                                    }), slaves, numthreads);
                                            }
                                            System.Threading.Thread.Sleep(1000); // Allow slaves to finalize.

                                            string killjzm = "zmap_*_*.j" + killsjid + ".zm";
                                            string killjzb = "zblock_*.j" + killsjid + ".zb";
                                            string killjoblog = "*_????????-????-????-????-????????????.j" + killsjid + "_log.txt";
                                            string killzf = "zfoil_*.j" + killsjid + ".zf";
                                            string killslaveconfig = "slaveconfig.j" + killsjid + ".xml";
                                            //foreach (string slave in slaves)
                                            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                                new Action<string>(
                                                delegate(string slave)
                                                {
                                                    string netpath = Surrogate.NetworkPathForHost(slave);
                                                    {
                                                        // Delete leaked chunks only! Have to check with DFS.xml
                                                        Dictionary<string, bool> dcnodes = new Dictionary<string, bool>(new Surrogate.CaseInsensitiveEqualityComparer());
                                                        foreach (dfs.DfsFile df in dc.Files)
                                                        {
                                                            for (int ifn = 0; ifn < df.Nodes.Count; ifn++)
                                                            {
                                                                string nn = df.Nodes[ifn].Name;
                                                                if (!dcnodes.ContainsKey(nn))
                                                                {
                                                                    dcnodes.Add(nn, true);
                                                                }
                                                            }
                                                        }
                                                        try
                                                        {
                                                            string killcheckjzd = "zd.*.*.j" + killsjid + ".zd";
                                                            foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(killcheckjzd))
                                                            {
                                                                if (!dcnodes.ContainsKey(fi.Name))
                                                                {
                                                                    for (int fiDeletes = 0; ; fiDeletes++)
                                                                    {
                                                                        try
                                                                        {
                                                                            fi.Delete();
                                                                            break;
                                                                        }
                                                                        catch
                                                                        {
                                                                            if (fiDeletes >= 100)
                                                                            {
                                                                                throw;
                                                                            }
                                                                            System.Threading.Thread.Sleep(100);
                                                                            continue;
                                                                        }
                                                                    }
                                                                    if (qverbose)
                                                                    {
                                                                        lock (slaves)
                                                                        {
                                                                            Console.Write("(deleted {0})", fi.FullName);
                                                                        }
                                                                    }
                                                                    if (dotverbose || qverbose)
                                                                    {
                                                                        lock (slaves)
                                                                        {
                                                                            Console.Write('.');
                                                                            ConsoleFlush();
                                                                        }
                                                                    }
                                                                    try
                                                                    {
                                                                        string fisamplename = fi.FullName + ".zsa";
                                                                        System.IO.File.Delete(fisamplename);
                                                                        /*if (dverbose)
                                                                        {
                                                                            lock (slaves)
                                                                            {
                                                                                Console.Write("(deleted {0})", fisamplename);
                                                                                //ConsoleFlush();
                                                                            }
                                                                        }*/
                                                                        if (dotverbose || qverbose)
                                                                        {
                                                                            lock (slaves)
                                                                            {
                                                                                Console.Write('.');
                                                                                ConsoleFlush();
                                                                            }
                                                                        }
                                                                    }
                                                                    catch
                                                                    {
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        catch (Exception e)
                                                        {
                                                            LogOutput("Unable to delete incomplete MR.DFS data belonging to JID " + killsjid + ": " + e.Message);
                                                        }
                                                    }
                                                    try
                                                    {
                                                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(killjzm))
                                                        {
                                                            for (int fiDeletes = 0; ; fiDeletes++)
                                                            {
                                                                try
                                                                {
                                                                    fi.Delete();
                                                                    break;
                                                                }
                                                                catch
                                                                {
                                                                    if (fiDeletes >= 100)
                                                                    {
                                                                        throw;
                                                                    }
                                                                    System.Threading.Thread.Sleep(100);
                                                                    continue;
                                                                }
                                                            }
                                                            /*if (dverbose)
                                                            {
                                                                lock (slaves)
                                                                {
                                                                    Console.Write("(deleted {0})", fi.FullName);
                                                                    //ConsoleFlush();
                                                                }
                                                            }*/
                                                            if (dotverbose || qverbose)
                                                            {
                                                                lock (slaves)
                                                                {
                                                                    Console.Write('.');
                                                                    ConsoleFlush();
                                                                }
                                                            }
                                                        }
                                                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(killjzb))
                                                        {
                                                            for (int fiDeletes = 0; ; fiDeletes++)
                                                            {
                                                                try
                                                                {
                                                                    fi.Delete();
                                                                    break;
                                                                }
                                                                catch
                                                                {
                                                                    if (fiDeletes >= 100)
                                                                    {
                                                                        throw;
                                                                    }
                                                                    System.Threading.Thread.Sleep(100);
                                                                    continue;
                                                                }
                                                            }
                                                            /*if (dverbose)
                                                            {
                                                                lock (slaves)
                                                                {
                                                                    Console.Write("(deleted {0})", fi.FullName);
                                                                    //ConsoleFlush();
                                                                }
                                                            }*/
                                                            if (dotverbose || qverbose)
                                                            {
                                                                lock (slaves)
                                                                {
                                                                    Console.Write('.');
                                                                    ConsoleFlush();
                                                                }
                                                            }
                                                        }
                                                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(killzf))
                                                        {
                                                            for (int fiDeletes = 0; ; fiDeletes++)
                                                            {
                                                                try
                                                                {
                                                                    fi.Delete();
                                                                    break;
                                                                }
                                                                catch
                                                                {
                                                                    if (fiDeletes >= 100)
                                                                    {
                                                                        throw;
                                                                    }
                                                                    System.Threading.Thread.Sleep(100);
                                                                    continue;
                                                                }
                                                            }
                                                            /*if (dverbose)
                                                            {
                                                                lock (slaves)
                                                                {
                                                                    Console.Write("(deleted {0})", fi.FullName);
                                                                    //ConsoleFlush();
                                                                }
                                                            }*/
                                                            if (dotverbose || qverbose)
                                                            {
                                                                lock (slaves)
                                                                {
                                                                    Console.Write('.');
                                                                    ConsoleFlush();
                                                                }
                                                            }
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        LogOutput("Unable to delete intermediate data belonging to JID " + killsjid + ": " + e.Message);
                                                    }
                                                    try
                                                    {
                                                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(killjoblog))
                                                        {
                                                            for (int fiDeletes = 0; ; fiDeletes++)
                                                            {
                                                                try
                                                                {
                                                                    fi.Delete();
                                                                    break;
                                                                }
                                                                catch
                                                                {
                                                                    if (fiDeletes >= 100)
                                                                    {
                                                                        throw;
                                                                    }
                                                                    System.Threading.Thread.Sleep(100);
                                                                    continue;
                                                                }
                                                            }
                                                            /*if (dverbose)
                                                            {
                                                                lock (slaves)
                                                                {
                                                                    Console.Write("(deleted {0})", fi.FullName);
                                                                    //ConsoleFlush();
                                                                }
                                                            }*/
                                                            if (dotverbose || qverbose)
                                                            {
                                                                lock (slaves)
                                                                {
                                                                    Console.Write('.');
                                                                    ConsoleFlush();
                                                                }
                                                            }
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        LogOutput("Unable to delete log files belonging to JID " + killsjid + ": " + e.Message);
                                                    }
                                                    try
                                                    {
                                                        System.IO.File.Delete(netpath + @"\" + killslaveconfig);
                                                    }
                                                    catch
                                                    {
                                                        // This is allowed to fail: the file might not exist.
                                                    }
                                                }), slaves, numthreads);

                                        }
                                        if (0 != killAelightPid)
                                        {
                                            try
                                            {
                                                System.Diagnostics.Process killproc = System.Diagnostics.Process.GetProcessById(killAelightPid);
                                                if (mutexesNeedSafePoint)
                                                {
                                                    HogMutexes(true, mutexes);
                                                }
                                                try
                                                {
                                                    killproc.Kill();
                                                    killproc.WaitForExit(1000 * 1);
                                                    mutexesNeedSafePoint = false;
                                                }
                                                finally
                                                {
                                                    HogMutexes(false, mutexes);
                                                }
                                                killproc.WaitForExit(1000 * 10); // Can wait longer outside mutexes.
                                                killproc.Close();
                                            }
                                            catch (Exception e)
                                            {
                                                LogOutputToFile("Unable to kill Surrogate PID " + killAelightSPid + " belonging to JID " + killsjid + ": " + e.Message);
                                            }
                                            try
                                            {
                                                System.IO.File.Delete(AELight_Dir + @"\" + killAelightSPid + ".aelight.pid");
                                            }
                                            catch
                                            {
                                            }
                                        }
                                        System.IO.File.Delete(killsjidfp);
                                    }
                                    killmutex.ReleaseMutex();
                                }
                                if (killjidexists)
                                {
                                    if (0 != killAelightPid)
                                    {
                                        if (CleanExecQ(killAelightPid, killjid))
                                        {
                                            Console.WriteLine("kill success");
                                        }
                                        else
                                        {
                                            CleanExecQ(0, killjid); // Still clean it from ps.
                                            Console.WriteLine("kill warning: Surrogate PID mismatch (ps)");
                                            return;
                                        }
                                    }
                                    else
                                    {
                                        CleanExecQ(0, killjid); // Still clean it from ps.
                                        Console.WriteLine("kill warning: unable to find Surrogate process");
                                        return;
                                    }
                                }
                                else
                                {
                                    if (CleanExecQ(0, killjid))
                                    {
                                        // Not running, but was still cleaned from ps.
                                        Console.WriteLine("kill warning: JID {0} not running", killsjid);
                                        return;
                                    }
                                    else
                                    {
                                        Console.Error.WriteLine("kill failure: JID {0} not running", killsjid);
                                        SetFailure();
                                        return;
                                    }
                                }
                            }

                        }
                        else
                        {
                            Console.Error.WriteLine("Expected JID to kill");
                            SetFailure();
                            return;
                        }
                    }
                    break;

                case "regressiontest":
                case "regressiontests":
                    RunRegressionTests(SubArray(args, 1));
                    break;

                case "recordsize":
                    if (args.Length > 1)
                    {
                        Console.WriteLine("{0}", Surrogate.GetRecordSize(args[1]));
                    }
                    else
                    {
                        Console.Error.WriteLine("Expected user-friendly record size string");
                        SetFailure();
                        return;
                    }
                    break;

                case "exec":
                    if (args.Length < 2)
                    {
                        Console.Error.WriteLine("Invalid arguments for " + args[0]);
                        ShowUsage();
                    }
                    else
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        try
                        {
                            int iarg = 1;
                            string ExecOpts = "";
                            List<string> xpaths = null;
                            //bool showjid = false;

                            while (iarg < args.Length)
                            {
                                switch (args[iarg][0])
                                {
                                    case '-':
                                        if (0 == string.Compare("-JID", args[iarg], true))
                                        {
                                            //showjid = true;
                                        }
                                        else
                                        {
                                            ExecOpts += " " + args[iarg].Substring(1);
                                        }
                                        iarg++; // Important.
                                        continue;

                                    case '/':
                                        if (null == xpaths)
                                        {
                                            xpaths = new List<string>();
                                        }
                                        xpaths.Add(args[iarg]);
                                        iarg++; // Important.
                                        continue;
                                }
                                break;
                            }

                            //if (showjid)
                            {
                                //Console.WriteLine("JID={0}", sjid);
                                Console.WriteLine("Job Identifier: {0}", sjid);
                            }

                            if (iarg >= args.Length)
                            {
                                Console.Error.WriteLine("Invalid arguments for " + args[0] + ": expected jobs file name to execute");
                                SetFailure();
                                return;
                            }

                            //#if DEBUG
                            if (args[iarg].StartsWith("file://", StringComparison.OrdinalIgnoreCase))
                            {
                                Exec(ExecOpts, LoadConfig(args[iarg].Substring(7)), SubArray(args, iarg + 1), true);
                                return;
                            }
                            //#endif    

                            dfs dc = LoadDfsConfig();
                            dfs.DfsFile dfjob = DfsFind(dc, args[iarg], DfsFileTypes.JOB);
                            if (null == dfjob)
                            {
                                Console.Error.WriteLine("exec jobs file not found in DFS: {0}", args[iarg]);
                                SetFailure();
                                return;
                            }
                            if (dfjob.Nodes.Count != 1)
                            {
                                throw new Exception("Error: exec jobs file not in correct jobs DFS format");
                            }
                            string ejnetpath = NetworkPathForHost(dfjob.Nodes[0].Host.Split(';')[0]) + @"\" + dfjob.Nodes[0].Name;

                            if (dc.LogExecHistory > 0)
                            {
                                LogExecHistory(args, ejnetpath, dc.LogExecHistory);
                            }

                            Exec(ExecOpts, LoadConfig(xpaths, ejnetpath), SubArray(args, iarg + 1), true);
                        }
                        catch (Exception e)
                        {
                            LogOutput(e.ToString());
                        }
                    }
                    break;

                case "ghost":
                case "ghostmt":
                case "ghostst":
                    {
                        dfs dc = LoadDfsConfig();

                        string[] hosts = dc.Slaves.SlaveList.Split(';');

                        bool singlethreaded = act == "ghostst";
                        int threadcount = singlethreaded ? 1 : hosts.Length;
                        if (threadcount > 15)
                        {
                            threadcount = 15;
                        }

                        Dictionary<string, bool> goodnames = new Dictionary<string, bool>(100); // Lowercase file name key.
                        List<System.Text.RegularExpressions.Regex> snowballregexes = new List<System.Text.RegularExpressions.Regex>();
                        List<string> mappedsamplenames = new List<string>();
                        foreach (dfs.DfsFile df in dc.Files)
                        {
                            if (0 == string.Compare(df.Type, DfsFileTypes.DELTA, StringComparison.OrdinalIgnoreCase))
                            {
                                string snowballname = df.Name;
                                string srex = Surrogate.WildcardRegexString(GetSnowballFilesWildcard(snowballname));
                                System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                                snowballregexes.Add(rex);
                                string fnms = "zsballsample_" + snowballname + ".zsb";
                                mappedsamplenames.Add(fnms);
                            }
                        }

                        long nghosts = 0;
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                new Action<string>(delegate(string host)
                                //for (int hi = 0; hi < hosts.Length; hi++)
                                {
                                    string netpath = Surrogate.NetworkPathForHost(host);

                                    {
                                        // Clean leaked snowballs...
                                        int snowballregexesCount = snowballregexes.Count;
                                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles("zsball_*.zsb"))
                                        {
                                            bool goodsnowball = false;
                                            for (int i = 0; i < snowballregexesCount; i++)
                                            {
                                                if (snowballregexes[i].IsMatch(fi.Name))
                                                {
                                                    goodsnowball = true;
                                                    break;
                                                }
                                            }
                                            if (!goodsnowball)
                                            {
                                                lock (hosts)
                                                {
                                                    nghosts++;
                                                    Console.WriteLine("  Ghost data file: {0}", fi.Name);
                                                }
                                            }
                                        }
                                        int mappedsamplenamesCount = mappedsamplenames.Count;
                                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles("zsballsample_*.zsb"))
                                        {
                                            bool goodmappedsamples = false;
                                            for (int i = 0; i < mappedsamplenamesCount; i++)
                                            {
                                                if (0 == string.Compare(mappedsamplenames[i], fi.Name))
                                                {
                                                    goodmappedsamples = true;
                                                    break;
                                                }
                                            }
                                            if (!goodmappedsamples)
                                            {
                                                lock (hosts)
                                                {
                                                    nghosts++;
                                                    Console.WriteLine("  Ghost data file: {0}", fi.Name);
                                                }
                                            }
                                        }
                                    }
                                }
                            ), hosts, threadcount);
                        Console.WriteLine("Found {0} ghost data files", nghosts);
                    }
                    break;

                case "restoresurrogate":
                    {
                        // restoresurrogate [-nostop] <metabackup-path> <target-dspace-path> [<new-metabackup-path>]
                        int iarg = 1;

                        bool stop = true;
                        while (args.Length > iarg && args[iarg][0] == '-')
                        {
                            string arg = args[iarg++];
                            switch (arg)
                            {
                                case "-nostop":
                                    stop = false;
                                    break;
                                default:
                                    Console.Error.WriteLine("Unknown switch for restoresurrogate: {0}", arg);
                                    SetFailure();
                                    return;
                            }
                        }

                        if (args.Length <= iarg)
                        {
                            Console.Error.WriteLine("Expected <metabackup-path>; not enough arguments for restoresurrogate");
                            SetFailure();
                            return;
                        }
                        string metabackuplocation = args[iarg++];

                        if (args.Length <= iarg)
                        {
                            Console.Error.WriteLine("Expected <target-" + appname + "-path>; not enough arguments for restoresurrogate");
                            SetFailure();
                            return;
                        }
                        string targetdspacepath = args[iarg++];

                        string newmetabackuppath = "";
                        if (args.Length > iarg)
                        {
                            newmetabackuppath = args[iarg++];
                        }

                        if (!string.IsNullOrEmpty(newmetabackuppath))
                        {
                            if (!System.IO.Directory.Exists(newmetabackuppath))
                            {
                                System.IO.Directory.CreateDirectory(newmetabackuppath);
                            }
                        }

                        string metabackupdfsxmlpath;
                        if (System.IO.Directory.Exists(metabackuplocation))
                        {
                            string[] xmlfiles = System.IO.Directory.GetFiles(metabackuplocation, "*.xml");
                            if (xmlfiles.Length > 1)
                            {
                                Console.Error.WriteLine("Error: Too many xml files found in metabackup location; remove all but one and try again: {0}", metabackuplocation);
                                SetFailure();
                                return;
                            }
                            else if (xmlfiles.Length < 1)
                            {
                                Console.Error.WriteLine("Error: {0} not found in metabackup location: {1}", dfs.DFSXMLNAME, metabackuplocation);
                                SetFailure();
                                return;
                            }
                            else //if (xmlfiles.Length == 1)
                            {
                                metabackupdfsxmlpath = xmlfiles[0];
                            }
                        }
                        else if (System.IO.File.Exists(metabackuplocation))
                        {
                            Console.WriteLine("Error: must speicfy directory of metabackup, not file: {0}", metabackuplocation);
                            SetFailure();
                            return;
                        }
                        else
                        {
                            Console.WriteLine("Error: metabackup directory not found: {0}", metabackuplocation);
                            SetFailure();
                            return;
                        }

                        string newmaster;
                        if (targetdspacepath.StartsWith(@"\\"))
                        {
                            int ixh = targetdspacepath.IndexOf('\\', 2);
                            if (-1 == ixh)
                            {
                                Console.Error.WriteLine("Error: problem parsing network from path: {0}", targetdspacepath);
                                SetFailure();
                                return;
                            }
                            newmaster = targetdspacepath.Substring(2, ixh - 2);
                        }
                        else
                        {
                            //newmaster = System.Net.Dns.GetHostName();
                            Console.WriteLine("Error: network path required for target dspace directory for surrogate: {0}", targetdspacepath);
                            SetFailure();
                            return;
                        }

                        Console.WriteLine("Loading metabackup metadata...", metabackupdfsxmlpath);
                        dfs mbdc;
                        try
                        {
                            mbdc = dfs.ReadDfsConfig_unlocked(metabackupdfsxmlpath);
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine("Unable to read metadata from '{0}': {1}", metabackupdfsxmlpath, e.Message);
                            SetFailure();
                            return;
                        }

                        string[] slaves = mbdc.Slaves.SlaveList.Split(';');
                        int threadcount = slaves.Length;
                        if (threadcount > 15)
                        {
                            threadcount = 15;
                        }

                        string[] allmachines;
                        {
                            List<string> am = new List<string>(slaves.Length + 1);
                            am.Add(newmaster); // Add surrogate first.
                            for (int si = 0; si < slaves.Length; si++)
                            {
                                // Add slave if it's not the new surrogate.
                                if (0 != string.Compare(IPAddressUtil.GetName(slaves[si]), IPAddressUtil.GetName(newmaster), StringComparison.OrdinalIgnoreCase))
                                {
                                    am.Add(slaves[si]);
                                }
                            }
                            allmachines = am.ToArray();
                        }

                        Console.WriteLine("Accessing target " + appname + " path {0} ...", targetdspacepath);
                        if (!System.IO.File.Exists(targetdspacepath + @"\aelight.exe"))
                        {
                            Console.Error.WriteLine("Problem accessing target " + appname + " path '{0}': {1}", targetdspacepath, appname + " is not installed at this location");
                            SetFailure();
                            return;
                        }
                        try
                        {
                            // Run a little test to verify...
                            string fp = targetdspacepath + "\\restoresurrogate." + Surrogate.SafeTextPath(System.Net.Dns.GetHostName()) + "." + Guid.NewGuid();
                            System.IO.File.WriteAllText(fp, "[" + DateTime.Now.ToString() + "] restoresurrogate command issued from " + System.Net.Dns.GetHostName() + " {7BCD3A7C-3FA6-466f-84CB-51D70BB2B686}" + Environment.NewLine);
                            if (-1 == System.IO.File.ReadAllText(fp).IndexOf("{7BCD3A7C-3FA6-466f-84CB-51D70BB2B686}"))
                            {
                                System.IO.File.Delete(fp);
                                throw new System.IO.IOException("Read verification error {7BCD3A7C-3FA6-466f-84CB-51D70BB2B686}");
                            }
                            System.IO.File.Delete(fp);
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine("Problem accessing target " + appname + " path '{0}': {1}", targetdspacepath, e.Message);
                            SetFailure();
                            return;
                        }

                        _CleanPidFile_unlocked(); // So stopping services doesn't kill this instance.

                        if (stop)
                        {
                            Console.WriteLine("Stopping services...");
                            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                new Action<string>(
                                delegate(string host)
                                {
                                    try
                                    {
                                        Shell("sc \\\\" + host + " stop DistributedObjects");
                                    }
                                    catch
                                    {
                                    }
                                }), allmachines, threadcount);
                            System.Threading.Thread.Sleep(1000 * 3); // Give a bit of extra time to shutdown.
                        }

                        {
                            Console.WriteLine("Restoring surrogate...");

                            Surrogate.SetNewMasterHost(newmaster);
                            Surrogate.SetNewMetaLocation(targetdspacepath);

                            Console.WriteLine("    Restoring jobs files...");
                            foreach (System.IO.FileInfo zdfi in (new System.IO.DirectoryInfo(metabackuplocation)).GetFiles("*.zd"))
                            {
                                System.IO.File.Copy(zdfi.FullName, targetdspacepath + @"\" + zdfi.Name, true);
                            }

                            try
                            {
                                string schedulerbackuplocation = newmetabackuppath;
                                if (string.IsNullOrEmpty(schedulerbackuplocation))
                                {
                                    schedulerbackuplocation = null;
                                }
                                if (MySpace.DataMining.DistributedObjects.Scheduler.BackupRestore(
                                    metabackuplocation, targetdspacepath, schedulerbackuplocation))
                                {
                                    //Console.WriteLine("Restored scheduled and queued tasks");
                                }
                                else
                                {
                                    //Console.WriteLine("No scheduled or queued tasks to restore");
                                }
                            }
                            catch (System.IO.FileNotFoundException e)
                            {
                                Console.WriteLine("Warning: unable to restore scheduled and queued tasks, perhaps it was never backed up from before this feature.");
                                Console.WriteLine("Message: {0}", e.Message);
                            }

                            mbdc.MetaBackup = newmetabackuppath;
                            if (!string.IsNullOrEmpty(newmetabackuppath))
                            {
                                EnsureMetaBackupLocation(mbdc);
                                // Important! Only do this AFTER restoring everything from metabackup location!
                                // Because the user might want to re-use the same directory.
                                foreach (string fn in System.IO.Directory.GetFiles(mbdc.GetMetaBackupLocation()))
                                {
                                    System.IO.File.Delete(fn);
                                }
                            }

                            // Save mbdc to targetdspacepath
                            Console.WriteLine("    Restoring metadata...");
                            try
                            {
                                System.IO.File.Delete(targetdspacepath + @"\dfs.xml");
                            }
                            catch
                            {
                            }
                            try
                            {
                                System.IO.File.Delete(targetdspacepath + @"\slave.dat");
                            }
                            catch
                            {
                            }
                            {
                                // Updating slave.dat if found...
                                // If no slave.dat, it's probably a participating surrogate.
                                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                    new Action<string>(
                                    delegate(string slave)
                                    {
                                        try
                                        {
                                            // Delete any dfs.xml found, so this can also work as a move-surrogate feature.
                                            System.IO.File.Delete(Surrogate.NetworkPathForHost(slave) + @"\dfs.xml");
                                        }
                                        catch
                                        {
                                        }
                                        try
                                        {
                                            string sdfp = Surrogate.NetworkPathForHost(slave) + @"\slave.dat";
                                            if (System.IO.File.Exists(sdfp))
                                            {
                                                string[] sd = System.IO.File.ReadAllLines(sdfp);
                                                string sdfpnew = sdfp + ".new";
                                                using (System.IO.StreamWriter sw = System.IO.File.CreateText(sdfpnew))
                                                {
                                                    bool fm = false;
                                                    for (int i = 0; i < sd.Length; i++)
                                                    {
                                                        string line = sd[i];
                                                        if (line.StartsWith("master=", StringComparison.OrdinalIgnoreCase))
                                                        {
                                                            line = "master=" + newmaster;
                                                            fm = true;
                                                        }
                                                        sw.WriteLine(line);
                                                    }
                                                    if (!fm)
                                                    {
                                                        throw new Exception("Invalid slave.dat on " + slave + " - master=host entry not found");
                                                    }
                                                }
                                                System.IO.File.Delete(sdfp);
                                                System.IO.File.Move(sdfpnew, sdfp);
                                            }
                                            else
                                            {
                                                // If it doesn't exist, write out a new one, but not if it is surrogate.
                                                if (0 != string.Compare(IPAddressUtil.GetName(newmaster),
                                                    IPAddressUtil.GetName(slave), StringComparison.OrdinalIgnoreCase))
                                                {
                                                    System.IO.File.WriteAllText(sdfp, "master=" + newmaster
                                                        + Environment.NewLine);
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            lock (slaves)
                                            {
                                                Console.Error.WriteLine("WARNING: Error on machine {0}: {1}", slave, e.Message);
                                            }
                                        }
                                    }), slaves, threadcount);
                            }
                            {
                                // Fix old surrogate jobs-files references.
                                foreach (dfs.DfsFile df in mbdc.Files)
                                {
                                    if (0 == string.Compare(df.Type, DfsFileTypes.JOB, StringComparison.OrdinalIgnoreCase))
                                    {
                                        foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                                        {
                                            fn.Host = newmaster;
                                        }

                                    }

                                }
                            }
                            // Write new dfs.xml...
                            UpdateDfsXml(mbdc, targetdspacepath + @"\" + dfs.DFSXMLNAME, mbdc.GetMetaBackupLocation());

                        }

                        if (stop)
                        {
                            Console.WriteLine("Starting services...");
                            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                new Action<string>(
                                delegate(string host)
                                {
                                    try
                                    {
                                        Shell("sc \\\\" + host + " start DistributedObjects");
                                    }
                                    catch
                                    {
                                    }
                                }), allmachines, threadcount);
                            System.Threading.Thread.Sleep(1000 * 1); // Give a sec to startup.
                        }

                        Console.WriteLine("Done");

                        if (!string.IsNullOrEmpty(newmetabackuppath))
                        {
                            Console.WriteLine("Type the following to backup the current meta-data:");
                            Console.WriteLine("    {0} metabackup -backup-now", appname);
                        }
                        else
                        {
                            Console.WriteLine("Use the metabackup command to re-enable metabackups");
                        }

                    }
                    break;

                case "metabackup":
                    try
                    {
                        dfs dc = LoadDfsConfig();
                        if (args.Length > 1)
                        {
                            EnterAdminCmd();
                            if (0 == string.Compare("-backup-now", args[1], true))
                            {
                                string metabackupdir = dc.GetMetaBackupLocation();
                                if (null == metabackupdir)
                                {
                                    Console.Error.WriteLine("Cannot backup, no meta backup location is set");
                                    SetFailure();
                                    return;
                                }
                                Console.WriteLine("Backing up all meta-data and jobs files...");
                                //foreach (dfs.DfsFile df in dc.Files)
                                int njobs = 0;
                                MySpace.DataMining.Threading.ThreadTools<dfs.DfsFile>.Parallel(
                                    new Action<dfs.DfsFile>(
                                        delegate(dfs.DfsFile df)
                                        {
                                            try
                                            {
                                                if (0 == string.Compare(DfsFileTypes.JOB, df.Type, StringComparison.OrdinalIgnoreCase))
                                                {
                                                    bool goodnode = 1 == df.Nodes.Count;
                                                    string mblfn = goodnode ? df.Nodes[0].Name : "<null>";
                                                    string mblfp = metabackupdir + @"\" + mblfn;
                                                    Console.WriteLine("  dfs://{0} -> {1}", df.Name, mblfp);
                                                    if (!goodnode)
                                                    {
                                                        throw new Exception("dfs://" + df.Name + " has invalid data node");
                                                    }
                                                    string mblfpx = mblfp + "$";
                                                    System.IO.File.Copy(Surrogate.NetworkPathForHost(df.Nodes[0].Host) + @"\" + df.Nodes[0].Name, mblfpx, true);
                                                    try
                                                    {
                                                        System.IO.File.Delete(mblfp);
                                                    }
                                                    catch
                                                    {
                                                    }
                                                    System.IO.File.Move(mblfpx, mblfp);
                                                    System.Threading.Interlocked.Increment(ref njobs);
                                                }
                                            }
                                            catch (Exception eb)
                                            {
                                                LogOutputToFile(eb.ToString());
                                                Console.Error.WriteLine(eb.Message);
                                            }
                                        }), dc.Files, 15);
                                Console.WriteLine("Backed up {0} jobs files", njobs);
                                {
                                    MySpace.DataMining.DistributedObjects.Scheduler.SetBackupLocation(metabackupdir);
                                    Console.WriteLine("Backed up schedule and queue tasks");
                                }
                            }
                            else if ("-" == args[1])
                            {
                                string oldmetabackup = dc.GetMetaBackupLocation();
                                dc.MetaBackup = null;
                                {
                                    UpdateDfsXml(dc);
                                    {
                                        MySpace.DataMining.DistributedObjects.Scheduler.SetBackupLocation(null);
                                    }
                                    Console.WriteLine("Setting updated successfully");
                                    Console.WriteLine("Backups will no longer be saved");
                                    Console.WriteLine("Existing backups are still located at: {0}", oldmetabackup);
                                }
                            }
                            else
                            {
                                string oldmetabackup = dc.GetMetaBackupLocation();
                                string newmetabackup = args[1];
                                if (!newmetabackup.StartsWith(@"\\"))
                                {
                                    newmetabackup = Surrogate.LocalPathToNetworkPath(newmetabackup, Surrogate.MasterHost);
                                }
                                dc.MetaBackup = newmetabackup;
                                {
                                    EnsureMetaBackupLocation(dc); // Throws if problem, bailing out before saving change.
                                    foreach (string fn in System.IO.Directory.GetFiles(dc.GetMetaBackupLocation()))
                                    {
                                        System.IO.File.Delete(fn);
                                    }
                                    UpdateDfsXml(dc); // Only if EnsureMetaBackupLocation was successful!
                                    {
                                        MySpace.DataMining.DistributedObjects.Scheduler.SetBackupLocation(dc.GetMetaBackupLocation());
                                    }
                                    Console.WriteLine("Setting updated successfully");
                                    Console.WriteLine("Type the following to backup the current meta-data:");
                                    Console.WriteLine("    {0} metabackup -backup-now", appname);
                                }
                            }
                        }
                        else
                        {
                            string metabackupdir = dc.GetMetaBackupLocation();
                            Console.WriteLine("Meta backup location is: {0}",
                                (null == metabackupdir) ? "<null>" : metabackupdir);
                        }
                    }
                    catch (Exception e)
                    {
                        LogOutputToFile("{Metabackup} " + e.ToString());
                        Console.Error.WriteLine("Metabackup error: {0}", e.Message);
                        SetFailure();
                        return;
                    }
                    break;

                case "metadelete":
                case "metadel":
                case "metarm":
                case "removemetafile":
                    if (args.Length < 2)
                    {
                        Console.Error.WriteLine("Invalid arguments for " + args[0]);
                        ShowUsage();
                    }
                    else
                    {
                        DfsMetaDelete(args[1]);
                    }
                    break;

                case "metaremovemachine":
                case "removemetamachine":
                case "removemetahost":
                case "removemetanode":
                case "metaremove":
                    if (args.Length < 2)
                    {
                        Console.Error.WriteLine("Invalid arguments for " + args[0]);
                        ShowUsage();
                    }
                    else
                    {
                        EnterAdminCmd();
                        string RMHost = args[1];
                        bool DontTouchRMHost = (args.Length > 2 && "-s" == args[2]);
                        MetaRemoveMachine(RMHost, DontTouchRMHost);
                    }
                    break;

                case "slavelogfind":
                    {
                        if (args.Length > 1)
                        {
                            string what = args[1];
                            dfs dc = LoadDfsConfig();
                            string[] slaves = dc.Slaves.SlaveList.Split(';');
                            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                new Action<string>(
                                delegate(string slave)
                                {
                                    try
                                    {
                                        string netdir = Surrogate.NetworkPathForHost(slave);
                                        System.IO.FileInfo fi = new System.IO.FileInfo(netdir + @"\slave-log.txt");
                                        if (fi.Exists)
                                        {
                                            long lastmatchline = -1;
                                            string lastmatchstring = null;
                                            string line;
                                            long curline = 0;
                                            using (System.IO.StreamReader sr = fi.OpenText())
                                            {
                                                while (null != (line = sr.ReadLine()))
                                                {
                                                    curline++;
                                                    if (-1 != line.IndexOf(what, StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        lastmatchline = curline;
                                                        lastmatchstring = line;
                                                    }
                                                }
                                            }
                                            if (-1 != lastmatchline)
                                            {
                                                lock (slaves)
                                                {
                                                    Console.WriteLine("{0}({1}): {2}", fi.FullName, lastmatchline, lastmatchstring);
                                                }
                                            }
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        lock (slaves)
                                        {
                                            Console.Error.WriteLine("Error with {0}: {1}", slave, e.Message);
                                        }
                                    }
                                }
                            ), slaves, slaves.Length);
                        }
                        else
                        {
                            Console.Error.WriteLine("String to find expected");
                            SetFailure();
                            return;
                        }
                    }
                    Console.WriteLine("Done");
                    break;

                case "slaveloglargest":
                    {
                        dfs dc = LoadDfsConfig();
                        string[] slaves = dc.Slaves.SlaveList.Split(';');
                        long largestsize = -1;
                        string largestpath = null;
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string slave)
                            {
                                try
                                {
                                    string netdir = Surrogate.NetworkPathForHost(slave);
                                    System.IO.FileInfo fi = new System.IO.FileInfo(netdir + @"\slave-log.txt");
                                    if (fi.Exists)
                                    {
                                        long sz = fi.Length;
                                        lock (slaves)
                                        {
                                            if (sz > largestsize)
                                            {
                                                largestpath = fi.FullName;
                                                largestsize = sz;
                                            }
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    lock (slaves)
                                    {
                                        Console.Error.WriteLine("Error with {0}: {1}", slave, e.Message);
                                    }
                                }
                            }
                        ), slaves, slaves.Length);
                        if (-1 == largestsize)
                        {
                            Console.Error.WriteLine("None found");
                        }
                        else
                        {
                            Console.WriteLine("{0} contains the largest slave log at {1} ({2} B)", largestpath, Surrogate.GetFriendlyByteSize(largestsize), largestsize);
                        }
                    }
                    break;

                case "slavelogdelete":
                    {
                        dfs dc = LoadDfsConfig();
                        string[] slaves = dc.Slaves.SlaveList.Split(';');
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string slave)
                            {
                                try
                                {
                                    string netdir = Surrogate.NetworkPathForHost(slave);
                                    System.IO.File.Delete(netdir + @"\slave-log.txt");
                                    lock (slaves)
                                    {
                                        Console.Write('.');
                                    }
                                }
                                catch (Exception e)
                                {
                                }
                            }
                        ), slaves, slaves.Length);
                        Console.WriteLine();
                        Console.WriteLine("Done");
                    }
                    break;

                case "clearlogs":
                    {
                        dfs dc = LoadDfsConfig();
                        string[] slaves = dc.Slaves.SlaveList.Split(';');
                        const int MAX_TRIES = 10;
                        List<string> errs = new List<string>(slaves.Length);

                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string slave)
                            {
                                int triesremain = MAX_TRIES;
                                string fn = Surrogate.NetworkPathForHost(slave) + @"\slave-log.txt";
                                
                                for (; ; )
                                {
                                    try
                                    {
                                        System.IO.File.Delete(fn);
                                        lock (slaves)
                                        {
                                            Console.Write('.');                                            
                                        }
                                        return;
                                    }
                                    catch (Exception e)
                                    {
                                        if (--triesremain <= 0)
                                        {
                                            lock (slaves)
                                            {
                                                errs.Add(slave);
                                            }
                                            break;
                                        }                                        
                                    }
                                }                                
                            }
                        ), slaves, slaves.Length);

                        Console.WriteLine();

                        if (errs.Count > 0)
                        {
                            Console.WriteLine("Errors encountered while trying to clear logs from these machines:");
                            foreach (string e in errs)
                            {
                                Console.WriteLine(e);
                            }
                        }
                        else
                        {
                            Console.WriteLine("Done");
                        }                        
                    }
                    break;

                case "slaveloglist":
                    {
                        dfs dc = LoadDfsConfig();
                        string[] slaves = dc.Slaves.SlaveList.Split(';');
                        List<string> list = new List<string>(slaves.Length);
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string slave)
                            {
                                try
                                {
                                    string netdir = Surrogate.NetworkPathForHost(slave);
                                    System.IO.FileInfo fi = new System.IO.FileInfo(netdir + @"\slave-log.txt");
                                    if (fi.Exists)
                                    {
                                        lock (slaves)
                                        {
                                            list.Add(fi.Length.ToString().PadLeft(8) + " B  " + fi.FullName);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                }
                            }
                        ), slaves, slaves.Length);
                        list.Sort();
                        foreach (string x in list)
                        {
                            Console.WriteLine(x);
                        }
                    }
                    break;

                case "viewlog":
                case "viewlogs":
                    {
                        int maxentries = 1000;
                        string machine = null;
                        if (args.Length > 1)
                        {
                            for (int i = 1; i < args.Length; i++)
                            {
                                string arg = args[i];
                                string optval = "";
                                string optname = "";
                                int del = arg.IndexOf("=");
                                if (del > -1)
                                {
                                    optname = arg.Substring(0, del).ToLower();
                                    optval = arg.Substring(del + 1);
                                }
                                switch (optname)
                                {
                                    case "machine":
                                        machine = optval;
                                        break;
                                    case "count":
                                        try
                                        {
                                            int _max = Int32.Parse(optval);
                                            if (_max > 0)
                                            {
                                                maxentries = _max;
                                            }                       
                                        }
                                        catch
                                        {
                                        }                                                         
                                        break;
                                    default:
                                        Console.Error.WriteLine("Invalid argument for viewlogs");
                                        return;
                                }
                            }
                        }

                        string[] hosts = null;
                        if (machine == null)
                        {
                            dfs dc = LoadDfsConfig();
                            hosts = dc.Slaves.SlaveList.Split(';');
                        }
                        else
                        {
                            hosts = new string[1] { machine };
                        }

                        List<string> logpaths = new List<string>();
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string host)
                            {
                                string fn = Surrogate.NetworkPathForHost(host) + @"\slave-log.txt";
                                if (System.IO.File.Exists(fn))
                                {
                                    lock (logpaths)
                                    {
                                        logpaths.Add(fn);
                                    }
                                }
                            }), hosts, hosts.Length);

                        if (logpaths.Count == 0)
                        {
                            Console.Error.WriteLine("No log file is found.");
                            return;
                        }

                        const int MAXBYTE = 1024 * 1024 * 64;
                        int maxbytepart = MAXBYTE / logpaths.Count;
                        int maxentriespart = maxentries / logpaths.Count;
                        if (maxentries % logpaths.Count != 0)
                        {
                            maxentriespart++;
                        }

                        List<string[]> allentries = new List<string[]>(logpaths.Count);
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string logpath)
                            {
                                if (!System.IO.File.Exists(logpath))
                                {
                                    return;
                                }
                                
                                string token = "----------------------------------------------------------------" + Environment.NewLine + Environment.NewLine;

                                System.IO.FileStream fs = null;
                                try
                                {
                                    fs = new System.IO.FileStream(logpath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite);
                                    if (fs.Length > maxbytepart * 2)
                                    {
                                        fs.Position = fs.Length - maxbytepart;
                                    }

                                    int ib = 0;
                                    List<long> idx = new List<long>();
                                    long entryStart = 0;
                                    while ((ib = fs.ReadByte()) > -1)
                                    {
                                        if (ib == (int)token[0])
                                        {
                                            bool istoken = true;
                                            for (int i = 1; i < token.Length; i++)
                                            {
                                                if (fs.ReadByte() != (int)token[i])
                                                {
                                                    istoken = false;
                                                    break;
                                                }
                                            }

                                            if (istoken)
                                            {
                                                idx.Add(entryStart);
                                                entryStart = fs.Position;
                                            }
                                        }
                                    }
                                    if (idx.Count == 0)
                                    {
                                        return;
                                    }

                                    long flen = fs.Length;
                                    int startidx = idx.Count > maxentriespart ? idx.Count - maxentriespart : 0;
                                    long offset = idx[startidx];
                                    long buflen = flen - offset;
                                    while (buflen > maxbytepart && startidx < idx.Count - 1)
                                    {
                                        startidx++;
                                        offset = idx[startidx];
                                        buflen = flen - offset;
                                    }
                                    if (buflen > maxbytepart)
                                    {
                                        throw new Exception("log too large");
                                    }

                                    byte[] buf = new byte[buflen];
                                    fs.Position = offset;
                                    fs.Read(buf, 0, buf.Length);
                                    fs.Close();
                                    fs = null;

                                    string[] entries = new string[idx.Count - startidx];
                                    for (int i = startidx; i < idx.Count; i++)
                                    {
                                        int pos = (int)(idx[i] - offset);
                                        int bytecount = 0;
                                        if (i < idx.Count - 1)
                                        {
                                            bytecount = (int)(idx[i + 1] - offset - pos);
                                        }
                                        else
                                        {
                                            bytecount = buf.Length - pos;
                                        }
                                        entries[i - startidx] = System.Text.Encoding.ASCII.GetString(buf, pos, bytecount);
                                    }
                                    lock (allentries)
                                    {
                                        allentries.Add(entries);
                                    }
                                }
                                catch
                                {
                                    if (fs != null)
                                    {
                                        fs.Close();
                                        fs = null;
                                    }
                                    throw;
                                }
                            }
                            ), logpaths, logpaths.Count);

                        if (allentries.Count == 0)
                        {
                            Console.Error.WriteLine("No log entries found.");
                            return;
                        }

                        Console.WriteLine("-");
                        Console.WriteLine("Log entries:");
                        Console.WriteLine("-");

                        if (allentries.Count == 1)
                        {
                            foreach (string entry in allentries[0])
                            {
                                Console.Write(entry);
                            }
                            Console.WriteLine("-");
                            Console.WriteLine("Entries displayed: {0}", allentries[0].Length);
                            Console.WriteLine("-");
                        }
                        else
                        {
                            List<KeyValuePair<DateTime, string>> list = new List<KeyValuePair<DateTime, string>>();
                            foreach (string[] entries in allentries)
                            {
                                foreach (string entry in entries)
                                {
                                    int del = entry.IndexOf('M');   //AM or PM
                                    string sdate = entry.Substring(1, del);
                                    try
                                    {
                                        DateTime dt = DateTime.Parse(sdate);
                                        list.Add(new KeyValuePair<DateTime, string>(dt, entry));
                                    }
                                    catch
                                    {
                                    }
                                }
                            }

                            list.Sort(delegate(KeyValuePair<DateTime, string> x, KeyValuePair<DateTime, string> y)
                            {
                                return x.Key.CompareTo(y.Key);
                            });

                            int start = list.Count > maxentries ? list.Count - maxentries : 0;
                            for (int i = start; i < list.Count; i++)
                            {
                                Console.Write(list[i].Value);
                            }

                            Console.WriteLine("-");
                            Console.WriteLine("Entries displayed: {0}", list.Count - start);
                            Console.WriteLine("-");
                        }              
                    }
                    break;

                case "xhealth":
                    {
                        // DFS sanity check...
                        dfs dc = LoadDfsConfig();
                        Dictionary<string, bool> dd = new Dictionary<string, bool>(new Surrogate.CaseInsensitiveEqualityComparer());
                        foreach (dfs.DfsFile df in dc.Files)
                        {
                            if (dd.ContainsKey(df.Name))
                            {
                                Console.Error.WriteLine("Error: duplicate file '{0}' detected in DFS; this file should be deleted or xrepair", df.Name);
                                SetFailure();
                                return;
                            }
                            dd.Add(df.Name, true);
                        }
                    }
                    Console.WriteLine("Done");
                    break;

                case "xrepair":
                    {
                        // DFS sanity check...
                        for (bool run = true; run; )
                        {
                            run = false;
                            dfs dc = LoadDfsConfig();
                            Dictionary<string, bool> dd = new Dictionary<string, bool>(new Surrogate.CaseInsensitiveEqualityComparer());
                            foreach (dfs.DfsFile df in dc.Files)
                            {
                                if (dd.ContainsKey(df.Name))
                                {
                                    Console.WriteLine("Deleting '{0}'", df.Name);
                                    DfsDelete(df.Name);
                                    run = true;
                                    break;
                                }
                                dd.Add(df.Name, true);
                            }
                        }
                    }
                    Console.WriteLine("Done");
                    break;

                case "nearprime":
                    if (args.Length > 1)
                    {
                        int x = int.Parse(args[1]);
                        if (x <= 0)
                        {
                            Console.Error.WriteLine("Please enter a positive number");
                        }
                        else
                        {
                            Console.WriteLine("{0} is {1}prime", x, IsPrime(x) ? "" : "not ");
                            if (x > 2)
                            {
                                Console.WriteLine("{0} is nearest prime less than {1}", NearestPrimeLE(x - 1), x);
                            }
                            Console.WriteLine("{0} is nearest prime greater than {1}", NearestPrimeGE(x + 1), x);
                        }
                    }
                    else
                    {
                        Console.Error.WriteLine("What number?");
                    }
                    break;

                // Obsolete, use servicestatusall...
                case "status":
                    {
                        string[] hosts;
                        if (args.Length > 1)
                        {
                            hosts = args[1].Split(';', ',');
                        }
                        else
                        {
                            dfs dc = LoadDfsConfig();
                            hosts = dc.Slaves.SlaveList.Split(';');
                        }
                        int threadcount = hosts.Length;
                        if (threadcount > 15)
                        {
                            threadcount = 15;
                        }

                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string host)
                            {
                                bool found = false;
                                try
                                {
                                    string[] exlines = Shell("sc \\\\" + host + " query DistributedObjects").Split('\n');
                                    for (int iex = 0; iex < exlines.Length; iex++)
                                    {
                                        var x = exlines[iex].Trim();
                                        if (x.Length > 6 && x.Substring(0, 6) == "STATE ")
                                        {
                                            var state = x.Substring(6);
                                            int ils = state.LastIndexOf(' ');
                                            if (-1 != ils)
                                            {
                                                state = state.Substring(ils + 1);
                                            }
                                            //state = state.Replace(" ", "");
                                            //state = state.Trim();
                                            if ("RUNNING" == state)
                                            {
                                                lock (hosts)
                                                {
                                                    Console.WriteLine(host + ": " + state);
                                                }
                                            }
                                            else
                                            {
                                                lock (hosts)
                                                {
                                                    Console.WriteLine(host + ": " + state + " *** WARNING ***");
                                                }
                                            }
                                            found = true;
                                        }
                                    }
                                }
                                catch
                                {
                                }
                                if (!found)
                                {
                                    lock (hosts)
                                    {
                                        Console.WriteLine(host + ": FAILED *** ERROR ***");
                                    }
                                }
                            }
                            ), hosts, threadcount);

                    }
                    break;

                case "callstack":
                    if (args.Length <= 1)
                    {
                        Console.Error.WriteLine("Invalid syntax for command: callstack: not enough arguments");
                        SetFailure();
                        return;
                    }
                    if (0 == string.Compare(args[1], "worker", true) || 0 == string.Compare(args[1], "workers", true))
                    {
                        if (args.Length <= 3)
                        {
                            Console.Error.WriteLine("Invalid syntax for command: callstack worker: not enough arguments");
                            SetFailure();
                            return;
                        }
                        string sjidcs = args[2];
                        long jidcs = -1;
                        if ("*" != sjidcs)
                        {
                            if (!long.TryParse(sjidcs, out jidcs) || jidcs < 0)
                            {
                                Console.Error.WriteLine("callstack: invalid Job Identifier: " + sjidcs);
                                SetFailure();
                                return;
                            }
                            sjidcs = jidcs.ToString(); // Normalize.
                        }
                        string hostcs = args[3];
                        {
                            string netpath = Surrogate.NetworkPathForHost(hostcs);
                            string[] fps = System.IO.Directory.GetFiles(netpath, "*.j" + sjidcs + ".slave.pid");
                            if (0 == fps.Length)
                            {
                                Console.Error.WriteLine("No workers for Job Identifier {0} found on host {1}", sjidcs, hostcs);
                            }
                            else
                            {
                                System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(@"\\(\d+)\.j(\d+).slave.pid$",
                                    System.Text.RegularExpressions.RegexOptions.Compiled
                                    | System.Text.RegularExpressions.RegexOptions.Singleline
                                    | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                                List<KeyValuePair<string, string>> slavecs = new List<KeyValuePair<string,string>>(fps.Length); // Key=slave; Value=jid
                                for (int i = 0; i < fps.Length; i++)
                                {
                                    System.Text.RegularExpressions.Match m = rex.Match(fps[i]);
                                    if (!m.Success)
                                    {
                                        throw new Exception("Internal error: slave pid file mismatch");
                                    }
                                    System.Text.RegularExpressions.GroupCollection gc = m.Groups;
                                    slavecs.Add(new KeyValuePair<string, string>(gc[1].Value, gc[2].Value));
                                }

                                Console.WriteLine("Waiting on {0} worker callstack{1}...", slavecs.Count, slavecs.Count == 1 ? "" : "s");

                                for (int i = 0; i < slavecs.Count; i++)
                                {
                                    string path = netpath + @"\" + slavecs[i].Key + ".trace";
                                    for (; System.IO.File.Exists(path); System.Threading.Thread.Sleep(1000 * 1))
                                    {
                                    }
                                    string tpath = sjid + "tracing.slave" + slavecs[i].Key + ".tof";
                                    System.IO.File.WriteAllText(path, tpath + Environment.NewLine + ".");
                                }

                                for (int tries = 0; slavecs.Count > 0; tries++)
                                {
                                    if (0 != tries)
                                    {
                                        System.Threading.Thread.Sleep(1000 * 3);
                                    }
                                    for (int i = 0; i < slavecs.Count; i++)
                                    {
                                        string tpath = netpath + @"\" + sjid + "tracing.slave" + slavecs[i].Key + ".tof";
                                        {
                                            string toutput = ReadTraceFromFile(tpath);
                                            if (null == toutput)
                                            {
                                                if (0 == System.IO.Directory.GetFiles(netpath, slavecs[i].Key + ".j*.slave.pid").Length)
                                                {
                                                    Console.WriteLine();
                                                    Console.WriteLine("Worker no longer running");
                                                    try
                                                    {
                                                        System.IO.File.Delete(netpath + @"\" + slavecs[i].Key + ".trace");
                                                    }
                                                    catch
                                                    {
                                                    }
                                                    slavecs.RemoveAt(i);
                                                    i--;
#if DEBUG
                                                    //System.Diagnostics.Debugger.Launch();
#endif
                                                }
                                            }
                                            else
                                            {
                                                Console.WriteLine();
                                                Console.WriteLine(toutput);
                                                try
                                                {
                                                    System.IO.File.Delete(tpath);
                                                }
                                                catch
                                                {
                                                }
                                                slavecs.RemoveAt(i);
                                                i--;
                                            }
                                        }
                                    }
                                }
                                Console.WriteLine();

                            }
                        }
                    }
                    else if (0 == string.Compare(args[1], "surrogate", true))
                    {
                        if (args.Length <= 2)
                        {
                            Console.Error.WriteLine("Invalid syntax for command: callstack surrogate: not enough arguments");
                            SetFailure();
                            return;
                        }
                        string sjidcs = args[2];
                        long jidcs;
                        if (!long.TryParse(sjidcs, out jidcs) || jidcs < 0)
                        {
                            Console.Error.WriteLine("callstack: invalid Job Identifier: " + sjidcs);
                            SetFailure();
                            return;
                        }
                        sjidcs = jidcs.ToString(); // Normalize.
                        string hostcs = System.Net.Dns.GetHostName();
                        {
                            string netpath = Surrogate.NetworkPathForHost(hostcs);
                            string jidcsfp = netpath + @"\" + sjidcs + ".jid";
                            string saelightpid = null;
                            int aelightpid = -1;
                            for (; ; System.Threading.Thread.Sleep(1000 * 3))
                            {
                                try
                                {
                                    string jidcscontent;
                                    using (System.IO.FileStream f = new System.IO.FileStream(jidcsfp,
                                        System.IO.FileMode.Open, System.IO.FileAccess.Read,
                                        System.IO.FileShare.Read | System.IO.FileShare.Write | System.IO.FileShare.Delete))
                                    {
                                        System.IO.StreamReader sr = new System.IO.StreamReader(f);
                                        jidcscontent = sr.ReadToEnd();
                                        sr.Close();
                                    }
                                    {
                                        // If any of this fails, try again;
                                        // it might not be written fully yet.
                                        int ipidequ = 0;
                                        for (; ; )
                                        {
                                            ipidequ = jidcscontent.IndexOf("pid=", ipidequ);
                                            if (-1 == ipidequ)
                                            {
                                                break;
                                            }
                                            if (0 == ipidequ || '\n' == jidcscontent[ipidequ - 1])
                                            {
                                                // Ensure newline to ensure the pid= entry was fully written.
                                                int iendpid = jidcscontent.IndexOf('\n', ipidequ + 4);
                                                if (-1 != iendpid)
                                                {
                                                    saelightpid = jidcscontent.Substring(ipidequ + 4, iendpid - (ipidequ + 4)).Trim();
                                                    aelightpid = int.Parse(saelightpid);
                                                    saelightpid = aelightpid.ToString(); // Normalize.
                                                    break;
                                                }
                                                else
                                                {
                                                    //ipidequ += 4;
                                                    //continue;
                                                    ipidequ = -1;
                                                    break;
                                                }
                                            }
                                            else
                                            {
                                                ipidequ += 4;
                                                continue;
                                            }
                                        }
                                        if (-1 == ipidequ)
                                        {
                                            continue;
                                        }
                                    }
                                }
                                catch (System.IO.FileNotFoundException)
                                {
                                }
                                catch
                                {
                                    continue;
                                }
                                break;
                            }
                            if (null == saelightpid)
                            {
                                Console.Error.WriteLine("No surrogate process for Job Identifier {0}", sjidcs);
                            }
                            else
                            {
                                Console.WriteLine("Waiting on surrogate callstacks...");

                                {
                                    string path = netpath + @"\" + saelightpid + ".trace";
                                    for (; System.IO.File.Exists(path); System.Threading.Thread.Sleep(1000 * 1))
                                    {
                                    }
                                    string tpath = sjid + "tracing.aelight" + saelightpid + ".tof";
                                    System.IO.File.WriteAllText(path, tpath + Environment.NewLine + ".");
                                }

                                for (int tries = 0; ; tries++)
                                {
                                    if (0 != tries)
                                    {
                                        System.Threading.Thread.Sleep(1000 * 3);
                                    }
                                    {
                                        string tpath = netpath + @"\" + sjid + "tracing.aelight" + saelightpid + ".tof";
                                        {
                                            string toutput = ReadTraceFromFile(tpath);
                                            if (null == toutput)
                                            {
                                                if (!System.IO.File.Exists(jidcsfp))
                                                {
                                                    Console.WriteLine();
                                                    Console.WriteLine("Worker no longer running");
                                                    try
                                                    {
                                                        System.IO.File.Delete(netpath + @"\" + saelightpid + ".trace");
                                                    }
                                                    catch
                                                    {
                                                    }
                                                    break;
                                                }
                                            }
                                            else
                                            {
                                                Console.WriteLine();
                                                Console.WriteLine(toutput);
                                                try
                                                {
                                                    System.IO.File.Delete(tpath);
                                                }
                                                catch
                                                {
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                                Console.WriteLine();

                            }
                        }
                    }
                    else
                    {
                        Console.Error.WriteLine("Invalid syntax for command: callstack: didn't expect " + args[1]);
                        SetFailure();
                        return;
                    }
                    break;

                case "healthst":
                case "healthmt":
                case "health":
                    {
                        dfs dc = LoadDfsConfig();
                        

                        bool all = false;
                        bool verify = false;
                        string[] hosts = null;
                        
                        bool plugininfo = false;
                        bool mt = "healthst" != act;

                        if (args.Length >= 2)
                        {
                            for (int i = 1; i < args.Length; i++)
                            {
                                switch (args[i])
                                {
                                    case "-a":
                                        all = true;
                                        break;

                                    case "-v":
                                        verify = true;
                                        break;

                                    case "-mt":
                                        mt = true;
                                        break;

#if DEBUG
                                    case "-pi":
                                        plugininfo = true;
                                        break;
#endif

                                    default:
                                        {
                                            string shosts = args[i];
                                            if (shosts.StartsWith("@"))
                                            {
                                                hosts = Surrogate.GetHostsFromFile(shosts.Substring(1));
                                            }
                                            else
                                            {
                                                hosts = shosts.Split(';', ',');
                                            }
                                        }
                                        break;
                                }
                            }
                        }

                        List<KeyValuePair<string, Surrogate.HealthMethod>> plugins
                            = new List<KeyValuePair<string, Surrogate.HealthMethod>>();
                        try
                        {
                            string cacdir = null;
                            List<dfs.DfsFile> healthdlls = dc.FindAll("QizmtHealth*.dll");
                            if (plugininfo)
                            {
                                Console.WriteLine("*PluginInfo: Found {0} matching plugin DLLs in DFS", healthdlls.Count);
                            }
                            foreach (dfs.DfsFile healthplugin in healthdlls)
                            {
                                if (null == cacdir)
                                {
#if HEALTHPLUGIN_FINDCAC
                                    foreach (string cdh in dc.Slaves.SlaveList.Split(',', ';'))
                                    {
                                        System.Threading.Thread cdthd = new System.Threading.Thread(
                                            new System.Threading.ThreadStart(
                                            delegate()
                                            {
                                                if (Surrogate.IsHealthySlaveMachine(cdh))
                                                {
                                                    string cddir = Surrogate.NetworkPathForHost(cdh) + @"\" + dfs.DLL_DIR_NAME;
                                                    if (System.IO.Directory.Exists(cddir))
                                                    {
                                                        cacdir = cddir;
                                                    }
                                                }
                                            }));
                                        cdthd.Start();
                                        cdthd.Join(1000 * 30);
                                        if (null != cacdir)
                                        {
                                            break;
                                        }
                                    }
#else
                                    // Needs participating surrogate.
                                    string cddir = AELight_Dir + @"\" + dfs.DLL_DIR_NAME;
                                    if (System.IO.Directory.Exists(cddir))
                                    {
                                        cacdir = cddir;
                                    }
                                    if (null == cacdir)
                                    {
                                        throw new Exception("Unable to locate CAC directory on surrogate (must be participating surrogate for health plugins)");
                                    }
#endif
                                }
                                if (null == cacdir)
                                {
                                    throw new Exception("Unable to locate healthy CAC directory");
                                }
                                if (plugininfo)
                                {
                                    Console.WriteLine("*PluginInfo: Found CAC dir at: {0}", cacdir);
                                }
                                bool foundhealthmethod = false;
                                try
                                {
                                    System.Reflection.Assembly hasm = System.Reflection.Assembly.LoadFrom(cacdir + @"\" + healthplugin.Name);
                                    foreach (Type t in hasm.GetTypes())
                                    {
                                        if (-1 != t.Name.IndexOf("Health", StringComparison.OrdinalIgnoreCase))
                                        {
                                            System.Reflection.MethodInfo mi = t.GetMethod("CheckHealth",
                                                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                                            if (null != mi)
                                            {
                                                Surrogate.HealthMethod hm = (Surrogate.HealthMethod)Delegate.CreateDelegate(typeof(Surrogate.HealthMethod), mi);
                                                plugins.Add(new KeyValuePair<string, Surrogate.HealthMethod>(healthplugin.Name + " " + t.Name, hm));
                                                foundhealthmethod = true;
                                                if (plugininfo)
                                                {
                                                    Console.WriteLine("*PluginInfo: CheckHealth method found: {0} {1}", healthplugin.Name, t.Name);
                                                }
                                            }
                                        }
                                    }
                                    if (!foundhealthmethod)
                                    {
                                        throw new Exception("Did not find a Health public class with CheckHealth public static method (HealthMethod)");
                                    }
                                }
                                catch (Exception epl)
                                {
                                    throw new Exception("Unable to use plugin " + healthplugin.Name + ": " + epl.Message, epl);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine("Health plugin error: " + e.Message);
                        }

                        if (null == hosts)
                        {
                            hosts = dc.Slaves.SlaveList.Split(';');
                        }

#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        int nthreads = 1;
                        if (mt)
                        {
                            nthreads = hosts.Length;
                            if (nthreads > 15)
                            {
                                nthreads = 15;
                            }
                        }

                        {
                            if (all)
                            {
                                Console.WriteLine("[Machines Health]");
                            }
                            int badones = 0;
                            //for (int si = 0; si < hosts.Length; si++)
                            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                new Action<string>(
                                delegate(string host)
                                {
                                    //string host = hosts[si];
                                    string reason = null;
                                    if (!all) // Only do this here if not -a because it'll be done later in more detail.
                                    {
                                        foreach (KeyValuePair<string, Surrogate.HealthMethod> plugin in plugins)
                                        {
                                            Surrogate.HealthMethod hm = plugin.Value;
                                            if (Surrogate.SafeCallHealthMethod(hm, host, out reason))
                                            {
                                                reason = null;
                                            }
                                            else
                                            {
                                                badones++;
                                                break;
                                            }
                                        }
                                    }
                                    if (null == reason)
                                    {
                                        if (Surrogate.IsHealthySlaveMachine(host, out reason))
                                        {
                                            reason = null;
                                        }
                                        else
                                        {
                                            badones++;
                                        }
                                    }
                                    if (reason != null)
                                    {
                                        Console.WriteLine("  {0}: {1}", host, reason);
                                    }
                                }), hosts, nthreads);
                            Console.WriteLine("      {0}% healthy", Math.Round((double)(hosts.Length - badones) * 100.0 / (double)hosts.Length, 0));
                        }

                        if (all)
                        {
                            Console.WriteLine("[DFS Health]");
                            int badones = 0;
                            int totalchecked = 0;
                            byte[] one = new byte[1];
                            //foreach (dfs.DfsFile df in dc.Files)
                            MySpace.DataMining.Threading.ThreadTools<dfs.DfsFile>.Parallel(
                                new Action<dfs.DfsFile>(
                                delegate(dfs.DfsFile df)
                                {
                                    if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                                        || 0 == string.Compare(df.Type, DfsFileTypes.JOB, StringComparison.OrdinalIgnoreCase))
                                    {
                                        totalchecked++;
                                        string msg = null; // Note: doesn't print Success.
                                        bool thisbad = false;
                                        MySpace.DataMining.Threading.ThreadTools<dfs.DfsFile.FileNode>.Parallel(
                                            new Action<dfs.DfsFile.FileNode>(
                                            delegate(dfs.DfsFile.FileNode fn)
                                            {
                                                if (thisbad)
                                                {
                                                    // Only one error per DFS file.
                                                    return;
                                                }
                                                string onhost = null;
                                                try
                                                {
                                                    string[] fnHosts = fn.Host.Split(';');
                                                    {
                                                        if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                                                            || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                                                        {
                                                            if (fnHosts.Length < dc.Replication)
                                                            {
                                                                throw new Exception("DFS file '" + df.Name + "' only has " + fnHosts.Length.ToString() + " replicates (chunk '" + fn.Name + "')");
                                                            }
                                                        }
                                                    }
                                                    {
                                                        Dictionary<string, bool> hd = new Dictionary<string, bool>(new Surrogate.CaseInsensitiveEqualityComparer());
                                                        foreach (string chost in fnHosts)
                                                        {
                                                            onhost = chost;
                                                            string xchost = IPAddressUtil.GetName(chost);
                                                            if (hd.ContainsKey(xchost))
                                                            {
                                                                throw new Exception("DFS file '" + df.Name + "' has invalid replicate data: multiple replicates on a single machine");
                                                            }
                                                            hd.Add(xchost, true);
                                                        }
                                                        onhost = null;
                                                    }
                                                    {
                                                        foreach (string chost in fnHosts)
                                                        {
                                                            onhost = chost;
                                                            using (System.IO.FileStream fs = new System.IO.FileStream(Surrogate.NetworkPathForHost(chost) + @"\" + fn.Name, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, 1)) // bufferSize=1
                                                            {
                                                                //if (fn.Length > 0) // Should always be > 0 or it wouldn't be here...
                                                                {
                                                                    // Note: multiple threads writing to 'one' but I don't need it.
                                                                    fs.Read(one, 0, 1);
                                                                }
                                                            }
                                                        }
                                                        onhost = null;
                                                    }
                                                }
                                                catch (Exception e)
                                                {
                                                    lock (df)
                                                    {
                                                        thisbad = true;
                                                        msg = e.Message;
                                                        if (null != onhost)
                                                        {
                                                            msg += " (host " + onhost + ")";
                                                        }
                                                    }
                                                }
                                            }), df.Nodes, hosts.Length);
                                        if (thisbad)
                                        {
                                            badones++;
                                        }
                                        if (msg != null)
                                        {
                                            Console.WriteLine("  {0}: {1}", df.Name, msg);
                                        }
                                    }
                                }), dc.Files, nthreads);
                            int percent = 100;
                            if (totalchecked > 0)
                            {
                                percent = (int)Math.Round((double)(totalchecked - badones) * 100.0 / (double)totalchecked, 0);
                            }
                            Console.WriteLine("      {0}% healthy", percent);

                            foreach (KeyValuePair<string, Surrogate.HealthMethod> plugin in plugins)
                            {
                                Console.WriteLine("[{0}]", plugin.Key);
                                Surrogate.HealthMethod hm = plugin.Value;
                                badones = 0;
                                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                    new Action<string>(
                                    delegate(string host)
                                    {
                                        string reason;
                                        if (Surrogate.SafeCallHealthMethod(hm, host, out reason))
                                        {
                                            reason = null;
                                        }
                                        else
                                        {
                                            badones++;
                                        }
                                        if (reason != null)
                                        {
                                            Console.WriteLine("  {0}: {1}", host, reason);
                                        }
                                    }), hosts, nthreads);
                                Console.WriteLine("      {0}% healthy", Math.Round((double)(hosts.Length - badones) * 100.0 / (double)hosts.Length, 0));
                            }

                            Console.WriteLine("[Checking GetFiles()]");
                            int getfileserr = 0;
                            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                               new Action<string>(
                               delegate(string host)
                               {
                                   string netpath = Surrogate.NetworkPathForHost(host);
                                   System.IO.DirectoryInfo dir = new System.IO.DirectoryInfo(netpath);
                                   try
                                   {
                                       System.IO.FileInfo[] files = dir.GetFiles("zd*.zd");
                                   }
                                   catch (Exception e)
                                   {
                                       lock (hosts)
                                       {
                                           getfileserr++;
                                           Console.WriteLine("GetFiles() failed for host: {0}.  Error: {1}", host, e.ToString());
                                       }
                                   }                                                
                               }), hosts, nthreads);
                            if (getfileserr > 0)
                            {
                                Console.WriteLine("      GetFiles() failed");
                            }
                            else
                            {
                                Console.WriteLine("      GetFiles() succeeded");
                            }
                        }

                        if (verify)
                        {
                            Console.WriteLine("[Verify Drivers]");

                            string[] sl = new string[1];
                            bool vOK = true;

                            foreach (string s in hosts)
                            {
                                sl[0] = s;
                                if (!VerifyHostPermissions(sl))
                                {
                                    Console.Error.WriteLine("Ensure the Windows service is installed and running on '{0}'", s);
                                    vOK = false;
                                }
                            }

                            if (vOK)
                            {
                                Console.WriteLine("      All machines are verified.");
                            }
                        }

                    }
                    break;

                case "repair":
                    {
                        dfs dc = LoadDfsConfig();
                        string[] slaves = dc.Slaves.SlaveList.Split(';');
                        bool unhealthy = false;
                        for (int si = 0; si < slaves.Length; si++)
                        {
                            string reason;
                            if (!Surrogate.IsHealthySlaveMachine(slaves[si], out reason))
                            {
                                unhealthy = true;
                                Console.WriteLine("{0} is unhealthy: {1}", slaves[si], reason);
                            }
                        }
                        if (unhealthy)
                        {
                            Console.WriteLine("Cluster is unhealthy.  Use " + appname + " removemachine to repair cluster.");
                        }
                        else
                        {
                            Console.WriteLine("Cluster is 100% healthy");
                        }
                    }
                    break;

                case "replicationphase":
                    if (!ReplicationPhase(null, true, 0, null))
                    {
                        Console.WriteLine("Nothing to replicate");
                    }
                    break;

                case "replicationfix":
                    {
                        dfs dc = LoadDfsConfig();
                        Dictionary<string, bool> hd = new Dictionary<string, bool>(new Surrogate.CaseInsensitiveEqualityComparer());
                        StringBuilder sbHosts = new StringBuilder();
                        bool changedany = false;
                        foreach (dfs.DfsFile df in dc.Files)
                        {
                            bool changedfile = false;
                            foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                            {
                                hd.Clear();
                                sbHosts.Length = 0;
                                bool changednode = false;
                                foreach (string chost in fn.Host.Split(';'))
                                {
                                    string xchost = IPAddressUtil.GetName(chost);
                                    if (hd.ContainsKey(xchost))
                                    {
                                        if (!changedfile)
                                        {
                                            Console.WriteLine("  Fixing {0}", df.Name);
                                        }
                                        changednode = true;
                                        changedfile = true;
                                        changedany = true;
                                    }
                                    else
                                    {
                                        if (0 != sbHosts.Length)
                                        {
                                            sbHosts.Append(';');
                                        }
                                        sbHosts.Append(chost);
                                        hd.Add(xchost, true);
                                    }
                                }
                                if (changednode)
                                {
                                    fn.Host = sbHosts.ToString();
                                }

                            }
                        }
                        if (changedany)
                        {
                            UpdateDfsXml(dc);
                            Console.WriteLine("Replication error fixed; to perform replication, issue command:");
                            Console.WriteLine("    {0} replicationphase", appname);
                        }
                        else
                        {
                            Console.WriteLine("No replication errors to fix");
                        }
                    }
                    break;

                case "replicationfactorupdate":
                case "replicationupdate":
                    {
                        if (args.Length <= 1)
                        {
                            Console.Error.WriteLine("Expected new replication factor");
                            SetFailure();
                            return;
                        }
                        else
                        {
                            EnterAdminCmd();

                            int newrf = int.Parse(args[1]);
                            if (newrf < 1)
                            {
                                Console.Error.WriteLine("Replication factor must be at least 1");
                                SetFailure();
                                return;
                            }
                            else
                            {
                                int oldrf;
                                dfs dc = LoadDfsConfig();
                                string[] slaves = dc.Slaves.SlaveList.Split(';');
                                oldrf = dc.Replication;
                                if (newrf > slaves.Length)
                                {
                                    Console.Error.WriteLine("Replication factor cannot be higher than the number of machines in the cluster ({0} is the maximum)", slaves.Length);
                                    SetFailure();
                                    return;
                                }
                                if (newrf > oldrf)
                                {
                                    checked
                                    {
                                        // Early disk space check...
                                        long freemin = long.MaxValue;
                                        long newspacezdcount = 0;
                                        long newspacezdsizes = 0;
                                        long newspacezsasizes = 0;
                                        //for (int si = 0; si < slaves.Length; si++)
                                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                            new Action<string>(
                                            delegate(string slave)
                                            {
                                                //string slave = slaves[si];
                                                string snetdir = Surrogate.NetworkPathForHost(slave);

                                                long x = (long)GetDiskFreeBytes(snetdir);
                                                lock (slaves)
                                                {
                                                    if (x < freemin)
                                                    {
                                                        freemin = x;
                                                    }
                                                }

                                                System.IO.DirectoryInfo netdi = new System.IO.DirectoryInfo(snetdir);
                                                {
                                                    System.IO.FileInfo[] fis = (netdi).GetFiles("zd*.zd");
                                                    lock (slaves)
                                                    {
                                                        foreach (System.IO.FileInfo fi in fis)
                                                        {
                                                            newspacezdcount++;
                                                            newspacezdsizes += fi.Length;
                                                        }
                                                    }
                                                }
                                                {
                                                    System.IO.FileInfo[] fis = (netdi).GetFiles("zd*.zd.zsa");
                                                    lock (slaves)
                                                    {
                                                        foreach (System.IO.FileInfo fi in fis)
                                                        {
                                                            newspacezsasizes += fi.Length;
                                                        }
                                                    }
                                                }
                                            }), slaves);
                                        //freemin

                                        // If replication were 1, these are the file sizes on whole cluster.
                                        long singlereplicatezdsizetotal = newspacezdsizes / dc.Replication;
                                        long singlereplicatezsasizetotal = newspacezsasizes / dc.Replication;

                                        long freemintotal = freemin * slaves.Length;

                                        // Add a little padding (another average size data-node-chunk [64MB?])
                                        long spacepaddingtotal = (newspacezdsizes / newspacezdcount) * slaves.Length;

                                        int morereplicates = newrf - oldrf;
#if DEBUG
                                        if (morereplicates < 1)
                                        {
                                            throw new Exception("DEBUG: (morereplicates < 1)");
                                        }
#endif
                                        if ((morereplicates *
                                            (singlereplicatezdsizetotal + singlereplicatezsasizetotal + spacepaddingtotal))
                                            > freemintotal)
                                        {
                                            Console.Error.WriteLine("Out of DFS disk space: there is not enough free space in DFS of the cluster to distribute the replicate data requested");
                                            SetFailure();
                                            return;
                                        }

                                    }
                                }
                                using (LockDfsMutex())
                                {
                                    dc = LoadDfsConfig();
                                    if (dc.Replication != oldrf)
                                    {
                                        Console.Error.WriteLine("Replication factor already updated to {0}", dc.Replication);
                                        SetFailure();
                                        return;
                                    }
                                    dc.Replication = newrf;
                                    UpdateDfsXml(dc);
                                }

                                Console.WriteLine("Replication factor set to {0}", newrf);
                                if (newrf > oldrf)
                                {
                                    if (!ReplicationPhase(null, true, 0, slaves))
                                    {
                                        if (!QuietMode)
                                        {
                                            Console.Error.WriteLine("Nothing to replicate");
                                        }
                                    }
                                }
                                else if (newrf < oldrf)
                                {
                                    LowerReplicationCount(true);
                                }
                            }
                        }
                    }
                    break;

                case "replicationfactorview":
                case "replicationview":
                    {
                        dfs dc = LoadDfsConfig();
                        Console.WriteLine("Replication factor is set to {0}", dc.Replication);
                    }
                    break;

                /* // Don't enable this due to admincmd.
                case "replicationfactor":
                case "replication":
                    // ...
                    break;
                 * */

                case "maxuserlogsupdate":
                    {
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("Expected new maxUserLogs.");
                            SetFailure();
                            return;
                        }
                        int max = 0;
                        try
                        {
                            max = Int32.Parse(args[1]);                            
                        }
                        catch
                        {
                            Console.Error.Write("maxUserLogs must be an integer.");
                            SetFailure();
                            return;
                        }
                        using (LockDfsMutex())
                        {
                            dfs dc = LoadDfsConfig();
                            dc.MaxUserLogs = max;
                            UpdateDfsXml(dc);
                        }
                        Console.WriteLine("MaxUserLogs set to {0}", max);
                    }
                    break;

                case "maxuserlogsview":
                    {
                        dfs dc = LoadDfsConfig();
                        Console.WriteLine("MaxUserLogs is set to {0}", dc.MaxUserLogs);
                    }
                    break;

                case "maxdglobalsupdate":
                    {
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("Expected new maxDGlobals.");
                            SetFailure();
                            return;
                        }
                        int max = 0;
                        try
                        {
                            max = Int32.Parse(args[1]);
                        }
                        catch
                        {
                            Console.Error.Write("maxDGlobals must be an integer.");
                            SetFailure();
                            return;
                        }
                        using (LockDfsMutex())
                        {
                            dfs dc = LoadDfsConfig();
                            dc.MaxDGlobals = max;
                            UpdateDfsXml(dc);
                        }
                        Console.WriteLine("MaxDGlobals set to {0}", max);
                    }
                    break;

                case "maxdglobalsview":
                    {
                        dfs dc = LoadDfsConfig();
                        Console.WriteLine("MaxDGlobals is set to {0}", dc.MaxDGlobals);
                    }
                    break;

                case "gen":
                case "generate":
                case "gendata":
                case "datagen":
                case "generatedata":

                case "genbin":
                case "bingen":
                case "genbinary":
                case "binarygen":
                case "generatebinary":

                case "asciigen":
                case "genascii":
                case "generateascii":

                case "wordgen":
                case "wordsgen":
                case "genword":
                case "genwords":
                case "generatewords":
                    {

                        int iarg = 1;
                        int iargseq = 1; // Index in forced sequence args; if int.MaxValue, an '=' was used.
                        List<string> xpaths = null;
                        string dfsoutput = null;
                        long sizeoutput = long.MinValue;
                        long rowsize = 100;
                        string rowsep = Environment.NewLine;
                        int writersCount = 0;
                        bool useCustomRandom = false;
                        GenerateType gentype = GetGenerateType(act);
                        for (; iarg < args.Length; iarg++)
                        {
                            string arg = args[iarg];
                            if (arg.StartsWith("/"))
                            {
                                if (null == xpaths)
                                {
                                    xpaths = new List<string>();
                                }
                                xpaths.Add(arg);
                            }
                            else
                            {
                                int ieq = arg.IndexOf('=');
                                int argid = 0;
                                if (-1 != ieq)
                                {
                                    iargseq = int.MaxValue;
                                    string argname = arg.Substring(0, ieq);
                                    arg = arg.Substring(ieq + 1);
                                    switch (argname.ToLower())
                                    {
                                        case "output-dfsfile":
                                        case "dfsfile":
                                        case "dfsoutput":
                                        case "output":
                                            argid = 1;
                                            break;
                                        case "size":
                                        case "outputsize":
                                            argid = 2;
                                            break;
                                        case "rowsize":
                                        case "row":
                                            argid = 3;
                                            break;
                                        case "writercount":
                                        case "writers":
                                            argid = 4;
                                            break;
                                        case "customrandom":
                                            //argid = 5;
                                            Console.Error.WriteLine("{0} is not valid", args[iarg]);
                                            SetFailure();
                                            return;

                                        case "random":
                                        case "rand":
                                            if (0 == string.Compare(arg, "custom", true)
                                                || 0 == string.Compare(arg, "customrandom", true))
                                            {
                                                useCustomRandom = true;
                                            }
                                            else if (0 == string.Compare(arg, "default", true))
                                            {
                                                useCustomRandom = false;
                                            }
                                            else
                                            {
                                                Console.Error.WriteLine("Unknown random setting {0}", arg);
                                                SetFailure();
                                                return;
                                            }
                                            continue; // Next arg...

                                        case "rows":
                                        case "rowcount":
                                            {
                                                if (long.MinValue == rowsize)
                                                {
                                                    Console.Error.WriteLine("Row size must be specified before row count");
                                                    SetFailure();
                                                    return;
                                                }
                                                long nrows = long.Parse(arg);
                                                sizeoutput = (rowsize + rowsep.Length) * nrows;
                                            }
                                            continue; // Next arg...

                                        case "type":
                                            gentype = GetGenerateType(arg);
                                            continue; // Next arg...

                                        default:
                                            Console.Error.WriteLine("Unknown named argument '{0}' in {1}", argname, args[iarg]);
                                            SetFailure();
                                            return;
                                    }
                                }
                                else
                                {
                                    if (int.MaxValue == iargseq)
                                    {
                                        Console.Error.WriteLine("Argument error: <name>=<value> expected for argument {0}: {1}", iarg + 1, arg);
                                        SetFailure();
                                        return;
                                    }
                                    argid = iargseq++;
                                }

                                switch (argid)
                                {
                                    case 1: // output-dfsfile
                                        {
                                            dfsoutput = arg;
                                            if (dfsoutput.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                            {
                                                dfsoutput = dfsoutput.Substring(6);
                                            }
                                            string reason = "";
                                            if (dfs.IsBadFilename(dfsoutput, out reason))
                                            {
                                                Console.Error.WriteLine("Invalid output-dfsfile: {0}", reason);
                                                SetFailure();
                                                return;
                                            }
                                        }
                                        break;

                                    case 2: // outputsize
                                        sizeoutput = ParseLongCapacity(arg);
                                        break;

                                    case 3: // rowsize
                                        rowsize = ParseLongCapacity(arg);
                                        break;

                                    case 4: // writercount
                                        writersCount = Int32.Parse(arg);
                                        break;

                                    case 5: // customrandom
                                        if (0 == string.Compare(arg, "customrandom", true))
                                        {
                                            useCustomRandom = true;
                                        }
                                        else
                                        {
                                            Console.Error.WriteLine("Expected customrandom or end of arguments, not {0}", arg);
                                            SetFailure();
                                            return;
                                        }
                                        break;

                                    default:
                                        throw new Exception(string.Format("DEBUG: Arguments parse failure: args[{0}]: {1}", iarg, args[iarg]));
                                }

                            }
                        }

                        if (string.IsNullOrEmpty(dfsoutput)
                            || long.MinValue == sizeoutput)
                        {
#if DEBUG
                            System.Diagnostics.Debugger.Launch();
#endif
                            Console.Error.WriteLine("Arguments expected: [\"</xpath>=<value>\"] <output-dfsfile> <outputsize> [type=<bin|ascii|word>] [row=<size>] [writers=<count>] [rand=custom]");
                            SetFailure();
                            return;
                        }

                        Generate(xpaths, dfsoutput, sizeoutput, rowsize, gentype, writersCount, useCustomRandom);
                    }
                    break;

                case "slaveinstalls":
                    {
                        // Note: doesn't include a non-participating surrogate! (it's not in SlaveList).
                        dfs dc = LoadDfsConfig();
                        string[] hosts = dc.Slaves.SlaveList.Split(';');
                        int goodcount = 0;
                        bool healthcheck = (args.Length > 1 && "-healthy" == args[1]);
                        foreach (string host in hosts)
                        {
                            if (!healthcheck || Surrogate.IsHealthySlaveMachine(host))
                            {
                                Console.WriteLine("{0} {1}", host, Surrogate.NetworkPathForHost(host));
                                goodcount++;
                            }
                        }
                        if (goodcount < dc.Replication)
                        {
                            Console.Error.WriteLine("Not enough healthy machines in cluster for replication factor of {0}", dc.Replication);
                            SetFailure();
                            return;
                        }
                    }
                    break;

                case "clusterconfigview":
                    //clusterconfigview_cmd:
                    if (args.Length <= 1)
                    {
                        Console.Error.WriteLine("Expected cluster config xpath");
                        return;
                    }
                    {
                        string xpath = dfs.FixXPath(args[1]);
                        System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                        using (LockDfsMutex())
                        {
                            xd.Load(DFSXMLPATH);
                        }
                        System.Xml.XmlNodeList xnl = xd.SelectNodes(xpath);
                        for (int j = 0; j < xnl.Count; j++)
                        {
                            //Console.WriteLine("  \"{0}\" = \"{1}\"", xnl[j].Name, xnl[j].InnerText);
                            Console.WriteLine(xnl[j].InnerText);
                        }
                    }
                    break;

                case "clusterconfigupdate":
                    //clusterconfigupdate_cmd:
                    if (args.Length <= 2)
                    {
                        Console.Error.WriteLine("Expected cluster config xpath and new value");
                        return;
                    }
                    {
                        string xpath = dfs.FixXPath(args[1]);
                        string value = args[2];
                        int nvalues = 0;
                        using (LockDfsMutex())
                        {
                            System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                            xd.Load(DFSXMLPATH);
                            System.Xml.XmlNodeList xnl = xd.SelectNodes(xpath);
                            for (int j = 0; j < xnl.Count; j++)
                            {
                                xnl[j].InnerText = value;
                                nvalues++;
                            }
                            if (nvalues > 0)
                            {
                                xd.Save(DFSXMLPATH);
                            }
                        }
                        if (1 == nvalues)
                        {
                            Console.WriteLine("Value updated");
                        }
                        else
                        {
                            Console.WriteLine("{0} values updated", nvalues);
                        }
                    }
                    break;

                /* // Don't enable this due to admincmd.
            case "clusterconfig":
                if (args.Length == 2)
                {
                    goto clusterconfigview_cmd;
                }
                else if (args.Length == 3)
                {
                    goto clusterconfigupdate_cmd;
                }
                else
                {
                    Console.WriteLine("Invalid number of arguments, expected:  clusterconfig <xpath> [<value>]");
                }
                break;
                 * */

                case "history":
                    try
                    {
                        int iarg = 1;

                        List<string> surrogates = null;

                        int nlines = 10;
                        if (args.Length > iarg)
                        {
                            int xnlines;
                            if (int.TryParse(args[iarg], out xnlines))
                            {
                                nlines = xnlines;
                                iarg++;
                            }
                        }

                        if (args.Length > iarg)
                        {
                            surrogates = new List<string>();

                            string shosts = args[iarg++];
                            if (shosts.StartsWith("@"))
                            {
                                shosts = System.IO.File.ReadAllText(shosts.Substring(1));
                                foreach (string host in Surrogate.GetHostsFromFile(shosts.Substring(1)))
                                {
                                    string surrogate = Surrogate.LocateMasterHost(Surrogate.NetworkPathForHost(host));
                                    if (!surrogates.Contains(surrogate))
                                    {
                                        surrogates.Add(surrogate);
                                    }
                                }
                            }
                            else
                            {
                                foreach (string host in shosts.Split(';', ','))
                                {
                                    string surrogate = Surrogate.LocateMasterHost(Surrogate.NetworkPathForHost(host));
                                    if (!surrogates.Contains(surrogate))
                                    {
                                        surrogates.Add(surrogate);
                                    }
                                }
                            }
                        }
                        else
                        {
                            //surrogates.Add(System.Net.Dns.GetHostName());
                        }

                        // Local function:
                        Action<string, string> printhistory = new Action<string, string>(
                            delegate(string host, string clustername)
                            {
                                string fp = Surrogate.NetworkPathForHost(host) + @"\execlog.txt";
                                string[] hlines;
                                {
                                    const int iMAX_SECS_RETRY = 10; // Note: doesn't consider the time spent waiting on I/O.
                                    const int ITER_MS_WAIT = 100; // Milliseconds to wait each retry.
                                    int iters = iMAX_SECS_RETRY * 1000 / ITER_MS_WAIT;
                                    for (; ; )
                                    {
                                        try
                                        {
                                            hlines = System.IO.File.ReadAllLines(fp);
                                            break;
                                        }
                                        catch
                                        {
                                            if (--iters < 0)
                                            {
                                                throw;
                                            }
                                            System.Threading.Thread.Sleep(ITER_MS_WAIT);
                                            continue;
                                        }
                                    }
                                }
                                int mynlines = nlines;
                                if (mynlines > hlines.Length)
                                {
                                    mynlines = hlines.Length;
                                }
                                Console.WriteLine("History of last {0} actions on {1} cluster:", mynlines, clustername);
                                for (int i = hlines.Length - mynlines; i < hlines.Length; i++)
                                {
                                    string ln = hlines[i];
                                    ln = ln.Replace(" -@log ", " ");
                                    Console.WriteLine("    {0}", ln.Replace("drule", "SYSTEM"));
                                }
                            });

                        if (null == surrogates)
                        {
                            string host = System.Net.Dns.GetHostName();
                            printhistory(host, "current");
                        }
                        else if (surrogates.Count < 1)
                        {
                            Console.Error.WriteLine("Error: no hosts");
                            SetFailure();
                            return;
                        }
                        else //if (surrogates.Count > 1)
                        {
                            for (int hi = 0; hi < surrogates.Count; hi++)
                            {
                                if (hi > 0)
                                {
                                    Console.WriteLine("--------------------------------");
                                }
                                printhistory(surrogates[hi], surrogates[hi]);
                            }
                        }
                    }
                    catch (Exception ehh)
                    {
                        LogOutputToFile(ehh.ToString());
                        Console.Error.WriteLine("No history");
                    }
                    break;

                case "exechistory":
                    {
                        dfs dc = LoadDfsConfig();
                        string[] execHistory = GetExecHistory(dc.LogExecHistory);

                        if (execHistory != null && execHistory.Length > 0)
                        {
                            Console.WriteLine("History of last {0} exec actions:", execHistory.Length);

                            for (int i = 0; i < execHistory.Length; i++)
                            {
                                int ast = execHistory[i].IndexOf('*');
                                Console.WriteLine("    {0} [ " + appname + " execview {1} ]", execHistory[i].Substring(ast + 1), execHistory[i].Substring(0, ast));
                            }
                        }
                        else
                        {
                            Console.Error.WriteLine("No exec history");
                        }
                    }
                    break;

                case "listinstalldir":
                    {
                        dfs dc = LoadDfsConfig();
                        string[] hosts = dc.Slaves.SlaveList.Split(';');

                        if (hosts.Length > 0)
                        {
                            foreach (string host in hosts)
                            {
                                Console.WriteLine(MySpace.DataMining.DistributedObjects5.DistObject.GetNetworkPath(host));
                            }
                        }
                    }
                    break;

                case "psstatus":
                    SafePS(true);
                    break;

                case "ps":
                    SafePS();
                    if (!dfs.DfsConfigExists(DFSXMLPATH))
                    {
                        //Console.Error.WriteLine("DFS not setup; use:  {0} dfs format", appname);
                        //SetFailure();
                        return;
                    }
                    {
                        checked
                        {
                            long totalmem = 0;
                            long nodeminmem = long.MaxValue;

                            long totalfreemem = 0;
                            long nodeminfreemem = long.MaxValue;

                            dfs dc = LoadDfsConfig();
                            string[] hosts = dc.Slaves.SlaveList.Split(';');
                            if (hosts.Length > 0)
                            {
                                //foreach (string host in hosts)
                                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                    new Action<string>(
                                    delegate(string host)
                                    {
                                        string meminfo = GetMemoryStatusForHost(host);
                                        if (null == meminfo)
                                        {
                                            Console.Error.WriteLine("Unable to get memory information for host '{0}'", host);
                                        }
                                        else
                                        {
                                            string[] memlines = meminfo.Split('\n');
                                            lock (hosts)
                                            {
                                                foreach (string _ml in memlines)
                                                {
                                                    string ml = _ml.Trim();
                                                    if (ml.StartsWith("TotalPhys: "))
                                                    {
                                                        long x = long.Parse(ml.Substring(ml.IndexOf(' ') + 1));
                                                        totalmem += x;
                                                        if (x < nodeminmem)
                                                        {
                                                            nodeminmem = x;
                                                        }
                                                    }
                                                    else if (ml.StartsWith("AvailPhys: "))
                                                    {
                                                        long x = long.Parse(ml.Substring(ml.IndexOf(' ') + 1));
                                                        totalfreemem += x;
                                                        if (x < nodeminfreemem)
                                                        {
                                                            nodeminfreemem = x;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                ), hosts, hosts.Length);
                                if (nodeminmem == long.MaxValue)
                                {
                                    nodeminmem = 0;
                                }
                                if (nodeminfreemem == long.MaxValue)
                                {
                                    nodeminfreemem = 0;
                                }

                                Console.WriteLine("        {0} Total Memory\r\n            {1} machine avg\r\n            {2} machine min\r\n            {3} process avg",
                                    GetFriendlyByteSize(totalmem), GetFriendlyByteSize(totalmem / (long)hosts.Length), GetFriendlyByteSize(nodeminmem), GetFriendlyByteSize((long)((double)nodeminmem / ((double)dc.Blocks.TotalCount / (double)hosts.Length))));
                                Console.WriteLine("        {0} Free Memory\r\n            {1} machine avg\r\n            {2} machine min\r\n            {3} process avg",
                                    GetFriendlyByteSize(totalfreemem), GetFriendlyByteSize(totalfreemem / (long)hosts.Length), GetFriendlyByteSize(nodeminfreemem), GetFriendlyByteSize((long)((double)nodeminfreemem / ((double)dc.Blocks.TotalCount / (double)hosts.Length))));
                                Console.WriteLine("        {0} Processes\r\n        {1} Machines",
                                    dc.Blocks.TotalCount, hosts.Length);
                            }
                        }
                    }
                    break;

                case "ver":
                case "version":
                    {
                        string buildtype;
#if DEBUG
                        buildtype = "debug";
#else
                        buildtype = "release";
#endif
                        //Console.WriteLine("AELight \"{0}\" {1} build {2}", appname, buildtype, GetBuildInfo());
                        Console.WriteLine("Version: " + (GetBuildDateTime().ToString()).Replace(":", ".").Replace(" ", ".").Replace("/", ".").Replace("AM", "A").Replace("PM", "P"));
                    }
                    break;

                case "#mem":
                    {
                        string memhost = "localhost"; // Note: probably local to master...
                        if (args.Length > 1)
                        {
                            memhost = args[1];
                        }
                        string meminfo = GetMemoryStatusForHost(memhost);
                        if (null == meminfo)
                        {
                            Console.Error.WriteLine("Unable to get memory information for host '{0}'", memhost);
                            SetFailure();
                            return;
                        }
                        Console.WriteLine(meminfo);
                    }
                    break;

                case "md5":
                    if (args.Length > 1)
                    {
                        GenerateHash("MD5", args[1]);
                    }
                    else
                    {
                        Console.Error.WriteLine("DFS file name expected");
                        SetFailure();
                        return;
                    }
                    break;

                case "sum":
                case "checksum":
                    if (args.Length > 1)
                    {
                        GenerateHash("Sum", args[1]);
                    }
                    else
                    {
                        Console.Error.WriteLine("DFS file name expected");
                        SetFailure();
                        return;
                    }
                    break;

                case "sum2":
                case "checksum2":
                    if (args.Length > 1)
                    {
                        GenerateHash("Sum2", args[1]);
                    }
                    else
                    {
                        Console.Error.WriteLine("DFS file name expected");
                        SetFailure();
                        return;
                    }
                    break;

                case "sorted":
                case "checksorted":
                case "issorted":
                    if (args.Length > 1)
                    {
                        CheckSorted(args[1]);
                    }
                    else
                    {
                        Console.Error.WriteLine("DFS file name expected");
                        SetFailure();
                        return;
                    }
                    break;

                case "dfs":
                    if (args.Length < 2)
                    {
                        Console.Error.WriteLine("DFS command expected");
                        ShowUsage();
                    }
                    else
                    {
                        string dfsarg = args[1];
                        Dfs(dfsarg, SubArray(args, 2));
                    }
                    break;

                case "combine":
                case "info":
                case "information":
                case "head":
                case "get":
                case "getbinary":
                case "put":
                case "putbinary":
                case "copy":
                case "cp":
                case "del":
                case "delete":
                case "rm":
                case "rename":
                case "ren":
                case "move":
                case "mv":
                case "getjobs":
                case "putjobs":
                case "ls":
                case "dir":
                case "invalidate":
                case "delmt":
                case "delst":
                case "\u0040format":
                case "format":
                case "countparts":
                case "filesize":
                case "bulkput":
                case "bulkget":
                case "swap":
                case "fput":
                case "fget":
                case "shuffle":
                    {
                        string dfsarg = args[0];
                        Dfs(dfsarg, SubArray(args, 1));
                    }
                    break;

                case "edit":
                case "editor":
                    Console.Error.WriteLine("Error:  must call " + appname + " to use jobs editor");
                    SetFailure();
                    break;

                case "dfsbind":
#if DEBUG
                    //System.Threading.Thread.Sleep(1000 * 8);
#endif
                    if (!dfs.DfsConfigExists(DFSXMLPATH))
                    {
                        Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                        SetFailure();
                        return;
                    }
                    if (args.Length < 5)
                    {
                        Console.Error.WriteLine("Invalid arguments");
                        ShowUsage();
                    }
                    else
                    {
                        // Note: dfsbind expects files to be in expected format with needed samples, etc.
                        // For internal use.

                        string newactualfilehost = args[1];
                        string newactualfilename = args[2];
                        string newprettyfilename = args[3];
                        string filetype = args[4];

                        /*if (0 != string.Compare(DfsFileTypes.JOB, filetype))
                        {
                            Console.Error.WriteLine("dfsbind not supported for DFS files of type " + filetype);
                            SetFailure();
                            return;
                        }*/

                        {
                            long flen = 0;
                            dfs.DfsFile df = new dfs.DfsFile();
                            df.Nodes = new List<dfs.DfsFile.FileNode>(1);
                            if (newactualfilename.Length > 0 && "/" != newactualfilename)
                            {
                                string ActualFile = Surrogate.NetworkPathForHost(newactualfilehost) + @"\" + newactualfilename;

                                System.IO.FileInfo finfo = new System.IO.FileInfo(ActualFile);
                                //if (finfo.Exists)
                                {
                                    flen = finfo.Length;
                                    dfs.DfsFile.FileNode fnode = new dfs.DfsFile.FileNode();
                                    fnode.Host = newactualfilehost;
                                    fnode.Position = 0;
                                    fnode.Length = flen;
                                    fnode.Name = newactualfilename;
                                    df.Nodes.Add(fnode);
                                }
                            }
                            df.Name = ".$" + newprettyfilename + ".$replicating-" + Guid.NewGuid().ToString();
                            df.Size = flen;
                            df.Type = filetype;
                            using (LockDfsMutex())
                            {
                                dfs dc = LoadDfsConfig(); // Reload in case of intermediate change.
                                if (null != DfsFindAny(dc, df.Name))
                                {
                                    Console.Error.WriteLine("Output file already exists:" + df.Name);
                                    SetFailure();
                                    return;
                                }
                                dc.Files.Add(df);
                                UpdateDfsXml(dc); // !
                            }
                            ReplicationPhase(df.Name, false, 0, null); // Note: doesn't use unhealthy-slaves exclusion list!
                            using (LockDfsMutex())
                            {
                                dfs dc = LoadDfsConfig(); // Reload in case of intermediate change.
                                dfs.DfsFile dfu = dc.FindAny(df.Name);
                                if (null != dfu)
                                {
                                    if (null != DfsFindAny(dc, newprettyfilename))
                                    {
                                        Console.Error.WriteLine("Output file already exists");
                                        SetFailure();
                                        return;
                                    }
                                    dfu.Name = newprettyfilename;
                                    UpdateDfsXml(dc);
                                }
                            }
                        }
                    }
                    break;

                case "@log":
                    // No action, it's just logged.
                    break;

                case "examples":
                case "example":
                    if (!dfs.DfsConfigExists(DFSXMLPATH))
                    {
                        Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                        SetFailure();
                        return;
                    }
                    try
                    {
                        Examples.Generate();
                    }
                    catch (Exception e)
                    {
                        LogOutput(e.ToString());
                    }
                    break;
                
                case "stresstests":
                    if (!dfs.DfsConfigExists(DFSXMLPATH))
                    {
                        Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                        SetFailure();
                        return;
                    }

                    string whattest = "sort";
                    if (args.Length > 1)
                    {
                        whattest = args[1].ToLower();
                    }

                    try
                    {
                        switch (whattest)
                        {
                            case "sort":
                                StressTests.SortTests.GenerateTests();
                                break;
                            case "valuesize":
                                StressTests.ValueSizeTests.GenerateTests();
                                break;
                            case "criticalsection":
                                StressTests.CriticalSectionTests.GenerateTests();
                                break;
                            default:
                                Console.Error.WriteLine("Invalid test names.  Try:  " + appname + " stresstests <sort | valuesize>");
                                SetFailure();
                                return;
                        }
                    }
                    catch (Exception e)
                    {
                        LogOutput(e.ToString());
                    }
                    break;

                case "addnode":
                case "addmachine":
                    EnterAdminCmd();
                    if (!dfs.DfsConfigExists(DFSXMLPATH))
                    {
                        Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                        SetFailure();
                        return;
                    }
                    {
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("addmachine error: new machine host expected");
                            SetFailure();
                            return;
                        }
                        string newhost = args[1];
                        AddMachine(newhost);
                    }
                    break;

                case "delnode":
                case "deletenode":
                case "removenode":
                case "remnode":
                case "delmachine":
                case "deletemachine":
                case "removemachine":
                case "remmachine":
                    EnterAdminCmd();
                    if (!dfs.DfsConfigExists(DFSXMLPATH))
                    {
                        Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                        SetFailure();
                        return;
                    }
                    {
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("removemachine error: old host expected");
                            SetFailure();
                            return;
                        }
                        string oldhost = args[1];
                        RemoveMachine(oldhost);
                    }
                    break;

                case "who":
                    {
                        List<string> hosts = new List<string>();
                        {
                            if (args.Length > 1)
                            {
                                string shosts = args[1];
                                if (shosts.StartsWith("@"))
                                {
                                    Surrogate.GetHostsFromFileAppend(shosts.Substring(1), hosts);
                                }
                                else
                                {
                                    hosts.AddRange(shosts.Split(';', ','));
                                }
                            }
                            else
                            {
                                dfs dc = LoadDfsConfig();
                                hosts.AddRange(dc.Slaves.SlaveList.Split(';'));
                                {
                                    // Always include self host if current cluster.
                                    if (null == GetSelfHost(hosts))
                                    {
                                        hosts.Add(System.Net.Dns.GetHostName());
                                    }
                                }
                            }
                        }

                        List<string> results = new List<string>();
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                                delegate(string host)
                                {
                                    string[] rr;
                                    try
                                    {
                                        // Don't "suppress errors" but drop them for "No User exists for *"
                                        rr = Shell("query user \"/server:" + host + "\"").Split('\n');
                                    }
                                    catch
                                    {
                                        //continue;
                                        return;
                                    }
                                    int usernameoffset = -1;
                                    int usernameend = -1;
                                    int lotimeoffset = -1;
                                    int lotimeend = -1;
                                    int idtimeoffset = -1;
                                    if (rr.Length > 0)
                                    {
                                        usernameoffset = rr[0].IndexOf(" USERNAME");
                                        if (usernameoffset > -1)
                                        {
                                            usernameend = rr[0].IndexOf("  ", usernameoffset + 1);
                                        }
                                        lotimeoffset = rr[0].IndexOf(" LOGON TIME");
                                        if (lotimeoffset > -1)
                                        {
                                            lotimeend = rr[0].IndexOf("  ", lotimeoffset + 1);
                                        }
                                        idtimeoffset = rr[0].IndexOf(" IDLE TIME");
                                    }
                                    if (usernameoffset > -1
                                        && usernameend > usernameoffset
                                        && lotimeoffset > usernameoffset
                                        && idtimeoffset > -1)
                                    {
                                        usernameoffset++;
                                        usernameend--;
                                        lotimeoffset++;
                                        if (lotimeend > -1)
                                        {
                                            lotimeend--;
                                        }
                                        idtimeoffset = idtimeoffset + 10;
                                        for (int j = 1; j < rr.Length; j++) // First line is a header, so skip it.
                                        {
                                            string line = rr[j].Trim('\r');
                                            if (line.Length <= lotimeoffset)
                                            {
                                                continue;
                                            }
                                            string username = line.Substring(usernameoffset, usernameend - usernameoffset).Trim();
                                            string logontime;
                                            if (lotimeend > -1)
                                            {
                                                logontime = line.Substring(lotimeoffset, lotimeend - lotimeoffset).Trim();
                                            }
                                            else
                                            {
                                                logontime = line.Substring(lotimeoffset).Trim();
                                            }
                                            string idletime = "";
                                            {
                                                string subline = rr[j].Substring(0, idtimeoffset).Trim();
                                                for (int li = subline.Length - 1; li >= 0; li--)
                                                {
                                                    if (subline[li] != ' ')
                                                    {
                                                        idletime = subline[li] + idletime;
                                                    }
                                                    else
                                                    {
                                                        break;
                                                    }
                                                }
                                            }
                                            string nodename = host;
                                            lock (results)
                                            {
                                                results.Add(username + " is logged on " + nodename + " since " + logontime + ". Idle for " + idletime + " min");
                                            }
                                        }
                                    }
                                }
                        ), hosts, hosts.Count);
                        if (results.Count > 0)
                        {
                            results.Sort();
                            for (int ir = 0; ir < results.Count; ir++)
                            {
                                Console.WriteLine(" {0}", results[ir]);
                            }
                        }
                        else
                        {
                            Console.WriteLine("No users logged on");
                        }
                    }
                    break;

                case "killall":
                    Console.Error.WriteLine("Cannot killall from here");
                    break;

                case "adminlock":
                    /*
                    if (args.Length >= 2 && args[1] == "-f")
                    {
                        EnterAdminCmd(true, true); // BypassJobs=true, PersistLock=true
                    }
                    else
                    {
                        EnterAdminCmd(false, true); // BypassJobs=false, PersistLock=true
                    }
                     * */
                    EnterAdminLock();
                    Console.WriteLine("Locked");
                    break;

                case "adminunlock":
                case "unlock":
                    /*
                    if (null == PersistAdminUser)
                    {
                        Console.WriteLine("No lock is in effect");
                    }
                    else
                    {
                        if (IsLockAllow)
                        {
                            _LeaveAdminCmd();
                            Console.WriteLine("Lock released");
                        }
                        else
                        {
                            // Won't reach here because I'm locked out from getting here anyway.
                            //Console.Error.WriteLine("Permission denied: cluster locked by another user");
                        }
                    }
                     * */
                    if (!LeaveAdminLock())
                    {
                        Console.WriteLine("No lock is in effect");
                    }
                    else
                    {
                        Console.WriteLine("Lock released");
                    }
                    break;

                case "deploy":
                case "deploymt":
                case "deployst":
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        EnterAdminCmd();
                        if (!dfs.DfsConfigExists(DFSXMLPATH))
                        {
                            Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                            SetFailure();
                            return;
                        }
                        if (isdspace)
                        {
                            Console.Error.WriteLine("Cannot deploy from {0}", appname);
                            SetFailure();
                            return;
                        }

                        string[] hosts;
                        bool thishostcheck = false;
                        if (args.Length > 1 && "-f" != args[1])
                        {
                            string shosts = args[1];
                            if (shosts.StartsWith("@"))
                            {
                                hosts = Surrogate.GetHostsFromFile(shosts.Substring(1));
                            }
                            else
                            {
                                hosts = shosts.Split(';', ',');
                            }
                            thishostcheck = true;
                        }
                        else
                        {
                            dfs dc = LoadDfsConfig();
                            hosts = dc.Slaves.SlaveList.Split(';');
                        }

                        {
                            string curdir = System.Environment.CurrentDirectory;
                            try
                            {
                                System.Environment.CurrentDirectory = AELight_Dir;

                                //Console.WriteLine(Shell("cleanup.bat", true));

                                bool singlethreaded = act == "deployst";
                                int threadcount = singlethreaded ? 1 : hosts.Length;
                                if (threadcount > 15)
                                {
                                    threadcount = 15;
                                }

                                List<string> copyfiles = new List<string>();
                                {
                                    string sr1 = Surrogate.WildcardRegexString("temp_*-????-????-????-*.dll");
                                    System.Text.RegularExpressions.Regex r1 = new System.Text.RegularExpressions.Regex(sr1, System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
                                    string sr2 = Surrogate.WildcardRegexString("dbg_*~*_????????-????-????-????-????????????.*");
                                    System.Text.RegularExpressions.Regex r2 = new System.Text.RegularExpressions.Regex(sr2, System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
                                    foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(".")).GetFiles("*.exe"))
                                    {
                                        string fn = fi.Name;
                                        if (!r2.IsMatch(fn))
                                        {
                                            copyfiles.Add(fn);
                                        }
                                    }
                                    foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(".")).GetFiles("*.dll"))
                                    {
                                        string fn = fi.Name;
                                        if (!r1.IsMatch(fn) && !r2.IsMatch(fn))
                                        {
                                            copyfiles.Add(fn);
                                        }
                                    }
                                    foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(".")).GetFiles("haarcascade_*.xml"))
                                    {
                                        string fn = fi.Name;
                                        copyfiles.Add(fn);
                                    }
                                    copyfiles.Add("cleanup.bat");
                                    copyfiles.Add("serviceconfig.xml");
                                    copyfiles.Add("MySpace.DataMining.DistributedObjects.DistributedObjectsSlave.exe.config");
                                    copyfiles.Add("licenses_and_attributions.txt");
                                }

                                int nrealdeploy = 0;
                                //for (int hi = 0; hi < hosts.Length; hi++)
                                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                    new Action<string>(
                                    delegate(string host)
                                    {
                                        //string host = hosts[hi];
                                        string netpath = NetworkPathForHost(host);

                                        try
                                        {
                                            // Do this before the host check,
                                            // so it includes a nonparticipating surrogate.
                                            System.IO.File.Delete(netpath + @"\execq.dat");
                                        }
                                        catch
                                        {
                                        }

                                        if (thishostcheck)
                                        {
                                            if (0 == string.Compare(IPAddressUtil.GetName(System.Net.Dns.GetHostName()), IPAddressUtil.GetName(host), StringComparison.OrdinalIgnoreCase))
                                            {
                                                lock (hosts)
                                                {
                                                    Console.WriteLine("Not deploying to {0}  (host check)", host);
                                                }
                                                return;
                                            }
                                        }
                                        else
                                        {
                                            if (dfs.DfsConfigExists(netpath + @"\" + dfs.DFSXMLNAME))
                                            {
                                                lock (hosts)
                                                {
                                                    Console.WriteLine("Not deploying to {0}  (metadata check)", host);
                                                }
                                                return;
                                            }
                                        }


                                        {
                                            string sout = Shell(@"sc \\" + host + " stop DistributedObjects", true);
                                            lock (hosts)
                                            {
                                                //Console.WriteLine("Deploying to {0}:", host);
                                                Console.WriteLine("Deploying to {0}", netpath);
                                                Console.WriteLine(sout); // Suppress error.
                                            }
                                            if (0 != System.IO.Directory.GetFiles(".", "*.pid").Length)
                                            {
                                                System.Threading.Thread.Sleep(1000 * 8);
                                            }
                                            System.Threading.Thread.Sleep(1000 * 2);
                                            for (int copyretry = 0; ; copyretry++)
                                            {
                                                try
                                                {
                                                    for (int i = 0; i < copyfiles.Count; i++)
                                                    {
                                                        string destfp = netpath + @"\" + copyfiles[i];
                                                        try
                                                        {
                                                            // Remove read-only.
                                                            System.IO.FileAttributes destattribs = System.IO.File.GetAttributes(destfp);
                                                            if ((destattribs & System.IO.FileAttributes.ReadOnly) == System.IO.FileAttributes.ReadOnly)
                                                            {
                                                                System.IO.File.SetAttributes(destfp, destattribs & ~System.IO.FileAttributes.ReadOnly);
                                                            }
                                                        }
                                                        catch
                                                        {
                                                        }
                                                        try
                                                        {
                                                            System.IO.File.Copy(copyfiles[i], destfp, true);
                                                        }
                                                        catch
                                                        {
                                                            if (copyretry < 10)
                                                            {
                                                                throw;
                                                            }
                                                            lock (hosts)
                                                            {
                                                                Console.WriteLine("(errcopy:{0}:{1})", host, copyfiles[i]);
                                                                ConsoleFlush();
                                                            }
                                                        }
                                                    }
                                                    break;
                                                }
                                                catch
                                                {
                                                    /*if (copyretry >= 20)
                                                    {
                                                        throw;
                                                    }*/
                                                    lock (hosts)
                                                    {
                                                        Console.Write('.');
                                                        ConsoleFlush();
                                                    }
                                                    //System.Threading.Thread.Sleep(1000 * 2);
                                                }
                                            }
                                            //Console.WriteLine("{0} files copied", copyfiles.Count);
                                            string uout = Shell(@"sc \\" + host + " start DistributedObjects", false);
                                            lock (hosts)
                                            {
                                                Console.WriteLine("Starting {0}:", host);
                                                Console.WriteLine(uout); // Throws on error.
                                                nrealdeploy++;
                                            }
                                        }
                                    }
                                ), hosts, threadcount);
                                Console.WriteLine("Deployed to {0} hosts", nrealdeploy);
                            }
                            finally
                            {
                                System.Environment.CurrentDirectory = curdir;
                            }
                        }

                    }
                    break;

                case "harddrivespeedtest":
                    {
                        ulong filesize = 64 * 1024 * 1024;

                        int iarg = 1;

                        if (args.Length > iarg)
                        {
                            try
                            {
                                filesize = (ulong)ParseLongCapacity(args[iarg]);
                                iarg++;
                            }
                            catch
                            {
                            }
                        }

                        if (filesize < 1024 * 1024)
                        {
                            Console.Error.WriteLine("Filesize must be at least 1MB", appname);
                            SetFailure();
                            return;
                        }

                        string[] hosts;
                        if (args.Length > iarg)
                        {
                            string shosts = args[iarg];
                            if (shosts.StartsWith("@"))
                            {
                                hosts = Surrogate.GetHostsFromFile(shosts.Substring(1));
                            }
                            else
                            {
                                hosts = shosts.Split(';', ',');
                            }
                        }
                        else
                        {
                            dfs dc = LoadDfsConfig();
                            hosts = dc.Slaves.SlaveList.Split(';');
                        }

                        Dictionary<string, double> reads = new Dictionary<string, double>();
                        Dictionary<string, double> writes = new Dictionary<string, double>();

                        for (int i = 0; i < hosts.Length; i++)
                        {
                            string host = hosts[i];
                            Console.WriteLine("Testing: {0}", host);
                            double write = 0;
                            double read = 0;
                            Surrogate.HardDriveSpeedTest(host, filesize, ref write, ref read);
                            reads.Add(host, read);
                            writes.Add(host, write);
                        }

                        List<KeyValuePair<string, double>> sReads = new List<KeyValuePair<string, double>>(reads);
                        sReads.Sort(
                           delegate(KeyValuePair<string, double> firstPair, KeyValuePair<string, double> nextPair)
                           {
                               return firstPair.Value.CompareTo(nextPair.Value);
                           }
                        );

                        List<KeyValuePair<string, double>> sWrites = new List<KeyValuePair<string, double>>(writes);
                        sWrites.Sort(
                           delegate(KeyValuePair<string, double> firstPair, KeyValuePair<string, double> nextPair)
                           {
                               return firstPair.Value.CompareTo(nextPair.Value);
                           }
                        );

                        Console.WriteLine();
                        Console.WriteLine("Read Speed");
                        foreach (KeyValuePair<string, double> p in sReads)
                        {
                            Console.WriteLine("{0}: {1} MB/s", p.Key, p.Value);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Write Speed");
                        foreach (KeyValuePair<string, double> p in sWrites)
                        {
                            Console.WriteLine("{0}: {1} MB/s", p.Key, p.Value);
                        }
                    }
                    break;

                case "cputemp":
                    {
                        dfs dc = LoadDfsConfig();

                        string[] hosts;
                        if (args.Length > 1)
                        {
                            string shosts = args[1];
                            if (shosts.StartsWith("@"))
                            {
                                hosts = Surrogate.GetHostsFromFile(shosts.Substring(1));
                            }
                            else
                            {
                                hosts = shosts.Split(';', ',');
                            }
                        }
                        else
                        {
                            hosts = dc.Slaves.SlaveList.Split(';');
                        }

                        Dictionary<string, double> temps = new Dictionary<string, double>();
                        int nThreads = hosts.Length / 15;

                        if (nThreads * 15 < hosts.Length)
                        {
                            nThreads++;
                        }

                        if (nThreads > 15)
                        {
                            nThreads = 15;
                        }

                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                        new Action<string>(
                        delegate(string slave)
                        {
                            lock (temps)
                            {
                                Console.WriteLine("Getting temp: {0}", slave);
                            }

                            double temp = Surrogate.GetCPUTemperature(slave);

                            lock (temps)
                            {
                                Console.WriteLine("Temp returned from {0}: {1}", slave, temp);
                                temps.Add(slave, temp);
                            }
                        }
                        ), hosts, nThreads);

                        //Sort
                        List<KeyValuePair<string, double>> sTemps = new List<KeyValuePair<string, double>>(temps);
                        sTemps.Sort(
                           delegate(KeyValuePair<string, double> firstPair, KeyValuePair<string, double> nextPair)
                           {
                               return firstPair.Value.CompareTo(nextPair.Value);
                           }
                        );

                        Console.WriteLine();
                        Console.WriteLine("Sorted temperature:");

                        double total = 0;

                        foreach (KeyValuePair<string, double> p in sTemps)
                        {
                            Console.WriteLine("{0}: {1} F", p.Key, p.Value);
                            total += p.Value;
                        }

                        double min = sTemps[0].Value;
                        double max = sTemps[sTemps.Count - 1].Value;

                        Console.WriteLine();
                        Console.WriteLine("Min Temp: {0} F", min);
                        Console.WriteLine("Max Temp: {0} F", max);
                        Console.WriteLine("Avg Temp: {0} F", total / (double)sTemps.Count);
                    }
                    break;

                case "packetsniff":
                    SafePacketSniff(args);
                    break;

                case "networkspeedtest":
                    {
                        ulong filesize = 64 * 1024 * 1024;

                        int iarg = 1;

                        if (args.Length > iarg)
                        {
                            try
                            {
                                filesize = (ulong)ParseLongCapacity(args[iarg]);
                                iarg++;
                            }
                            catch
                            {
                            }
                        }

                        if (filesize < 1024 * 1024)
                        {
                            Console.Error.WriteLine("Filesize must be at least 1MB", appname);
                            SetFailure();
                            return;
                        }

                        string[] hosts;
                        if (args.Length > iarg)
                        {
                            string shosts = args[iarg];
                            if (shosts.StartsWith("@"))
                            {
                                hosts = Surrogate.GetHostsFromFile(shosts.Substring(1));
                            }
                            else
                            {
                                hosts = shosts.Split(';', ',');
                            }
                        }
                        else
                        {
                            dfs dc = LoadDfsConfig();
                            hosts = dc.Slaves.SlaveList.Split(';');
                        }

                        List<List<double>> download = new List<List<double>>();
                        List<List<double>> upload = new List<List<double>>();

                        Surrogate.NetworkSpeedTest(hosts, filesize, download, upload);

                        Dictionary<string, double> avgDownload = new Dictionary<string, double>();
                        Dictionary<string, double> avgUpload = new Dictionary<string, double>();

                        for (int i = 0; i < hosts.Length; i++)
                        {
                            string host = hosts[i];

                            //Get avg download for this host.
                            double avg = 0;

                            for (int j = 0; j < download[i].Count; j++)
                            {
                                avg += download[i][j];
                            }

                            avg = avg / (double)download[i].Count;

                            avgDownload.Add(host, avg);

                            //Get avg upload for this host.
                            avg = 0;

                            for (int j = 0; j < upload[i].Count; j++)
                            {
                                avg += upload[i][j];
                            }

                            avg = avg / (double)upload[i].Count;

                            avgUpload.Add(host, avg);
                        }

                        //Sort
                        List<KeyValuePair<string, double>> sDown = new List<KeyValuePair<string, double>>(avgDownload);
                        sDown.Sort(
                           delegate(KeyValuePair<string, double> firstPair, KeyValuePair<string, double> nextPair)
                           {
                               return firstPair.Value.CompareTo(nextPair.Value);
                           }
                        );

                        List<KeyValuePair<string, double>> sUp = new List<KeyValuePair<string, double>>(avgUpload);
                        sUp.Sort(
                           delegate(KeyValuePair<string, double> firstPair, KeyValuePair<string, double> nextPair)
                           {
                               return firstPair.Value.CompareTo(nextPair.Value);
                           }
                        );

                        Console.WriteLine("Download speed");

                        foreach (KeyValuePair<string, double> p in sDown)
                        {
                            Console.WriteLine("{0}: {1} MB/s", p.Key, p.Value);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Upload speed");

                        foreach (KeyValuePair<string, double> p in sUp)
                        {
                            Console.WriteLine("{0}: {1} MB/s", p.Key, p.Value);
                        }

                    }
                    break;

                case "perfmon":
                    {
                        Perfmon.SafeGetCounters(SubArray(args, 1));
                    }
                    break;



                case "genhostname":
                case "genhostnames":
                    {
                        if (args.Length < 4)
                        {
                            Console.Error.WriteLine("Error: genhostnames command needs arguments: <pattern> <startNum> <endNum> [<delimiter>]");
                            return;
                        }

                        string pattern = args[1].Trim();
                        int startNum = 0;
                        int endNum = 0;
                        try
                        {
                            startNum = Int32.Parse(args[2]);
                            endNum = Int32.Parse(args[3]);
                        }
                        catch
                        {
                            Console.Error.WriteLine("Error: startNum / endNum are not valid integers.");
                            return;
                        }

                        string del = ";";
                        if (args.Length > 4)
                        {
                            del = args[4].Replace(@"\n", Environment.NewLine);
                        }

                        int pad = 0;
                        int shp = pattern.IndexOf('#');
                        if (shp > -1)
                        {
                            pad = pattern.LastIndexOf('#') - shp + 1;
                            pattern = pattern.Substring(0, shp);
                        }

                        for (int i = startNum; i <= endNum; i++)
                        {
                            string part = i.ToString().PadLeft(pad, '0');
                            Console.Write(pattern + part);
                            if (i != endNum)
                            {
                                Console.Write(del);
                            }
                        }
                    }
                    break;

                case "scrapeemptynames":
                    {
                        List<dfs.DfsFile> goods = new List<dfs.DfsFile>();
                        int badcount = 0;
                        using (LockDfsMutex())
                        {
                            dfs dc = LoadDfsConfig();
                            for (int i = 0; i < dc.Files.Count; i++)
                            {
                                dfs.DfsFile file = dc.Files[i];
                                if (file.Type != DfsFileTypes.NORMAL || file.Name.Trim().Length > 0)
                                {
                                    goods.Add(file);
                                }
                                else
                                {
                                    badcount++;
                                }
                            }
                            dc.Files = goods;
                            UpdateDfsXml(dc);
                        }
                        if (badcount > 0)
                        {
                            Console.WriteLine("{0} empty names are scraped.", badcount);
                        }
                    }
                    break;

                default:
                    if ('-' == args[0][0])
                    {
                        Console.Error.WriteLine("Error:  Unrecognized action: {0}", args[0]);
                    }
                    else
                    {
                        Console.Error.WriteLine("Error:  Action expected");
                    }
                    ShowUsage();
                    break;
            }
        }

        static string[] GetExecHistory(int maxDisplay)
        {
            try
            {
                string[] hlines = System.IO.File.ReadAllLines(AELight_Dir + @"\exechistorylog.txt");
                int start = Math.Max(hlines.Length - maxDisplay, 0);
                return SubArray(hlines, start);
            }
            catch
            {
                return null;
            }
        }

        static int ListContainsHostIndexOf(IList<string> hosts, string host)
        {
            int cnt = hosts.Count;
            //if (!exact)
            {
                host = IPAddressUtil.GetName(host);
            }
            for (int i = 0; i < cnt; i++)
            {
                string xhost = hosts[i];
                //if (!exact)
                {
                    xhost = IPAddressUtil.GetName(xhost);
                }
                if (0 == string.Compare(host, xhost, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            return -1;
        }

        static bool ListContainsHost(IList<string> hosts, string host)
        {
            return -1 != ListContainsHostIndexOf(hosts, host);
        }

        static bool ListContainsHost(string hosts, string host)
        {
            return -1 != ListContainsHostIndexOf(hosts.Split(';', ','), host);
        }


        // Important: returns null if it's not ready to be read yet!
        static string ReadTraceFromFile(string fp)
        {
            string result = null;
            try
            {
                const string tracefiledelim = "{C8683F6C-0655-42e7-ACD9-0DDED6509A7C}";
                using (System.IO.FileStream f = new System.IO.FileStream(fp, System.IO.FileMode.Open, System.IO.FileAccess.Read,
                    System.IO.FileShare.Read | System.IO.FileShare.Write | System.IO.FileShare.Delete))
                {
                    System.IO.StreamReader sr = new System.IO.StreamReader(f);
                    string s = sr.ReadToEnd();
                    sr.Close();
                    int i = s.IndexOf(tracefiledelim);
                    if (-1 != i)
                    {
                        s = s.Substring(i + tracefiledelim.Length);
                        i = s.IndexOf(tracefiledelim);
                        if (-1 != i)
                        {
                            result = s.Substring(0, i).Trim('\r', '\n');
                        }
                    }
                }
            }
            catch
            {
            }
            return result;
        }


        static void HogMutexes(bool acquire, IList<System.Threading.Mutex> mutexes)
        {
            if (acquire)
            {
                for (int im = 0; im < mutexes.Count; im++)
                {
                    try
                    {
                        if (!mutexes[im].WaitOne(1000 * 5, false))
                        {
                            // Mutex took too long, undo all and try again.
                            LogOutputToFile("HogMutexes(" + acquire.ToString() + "): Unable to access job in a timely fashion, trying again (unable to acquire mutexes in time)");
                            Console.WriteLine("Unable to access job in a timely fashion, trying again...");
                            for (im--; im >= 0; im--)
                            {
                                mutexes[im].ReleaseMutex();
                            }
                            System.Diagnostics.Debug.Assert(-1 == im);
                            System.Threading.Thread.Sleep(1000 * 3);
                        }
                    }
                    catch (System.Threading.AbandonedMutexException)
                    {
                    }
                }
            }
            else
            {
                for (int im = mutexes.Count - 1; im >= 0; im--)
                {
                    mutexes[im].ReleaseMutex();
                    mutexes[im].Close();
                }
            }
        }


        // Obsolete, use IPAddressUtil.FindCurrentHost(hosts)
        static string GetSelfHost(IList<string> hosts)
        {
            string myhost1 = System.Net.Dns.GetHostName();
            string myhost2 = IPAddressUtil.GetIPv4Address(myhost1);
            string myhost3 = "localhost";
            string myhost4 = "127.0.0.1";
            string myhost5 = Environment.MachineName;
            string myhost6 = IPAddressUtil.GetName(myhost2);
            string selfhost = null;
            for (int i = 0; i < hosts.Count; i++)
            {
                string xhost = hosts[i];
                if (0 == string.Compare(myhost1, xhost, true)
                    || 0 == string.Compare(myhost2, xhost)
                    || 0 == string.Compare(myhost3, xhost, true)
                    || 0 == string.Compare(myhost4, xhost)
                    || 0 == string.Compare(myhost5, xhost, true)
                    || 0 == string.Compare(myhost6, xhost, true)
                    )
                {
                    selfhost = xhost;
                    break;
                }
            }
            return selfhost;
        }

        public static void SafePacketSniff(string[] args)
        {
            DateTime startTime = DateTime.Now;
           
            string[] hosts = null;

            int sniffTime = 5000;
            bool verbose = false;
            bool excludeNonCluster = true;
            int nThreads = -1;

            if (args.Length > 1)
            {
                for (int i = 1; i < args.Length; i++)
                {
                    string s = args[i];
                    int del = s.IndexOf('=');
                    string nm = s.ToLower();
                    string val = "";
                    if (del > -1)
                    {
                        nm = s.Substring(0, del);
                        val = s.Substring(del + 1);
                    }
                    if (nm.StartsWith("-"))
                    {
                        nm = nm.Substring(1);
                    }

                    switch (nm)
                    {
                        case "v":
                            verbose = true;
                            break;

                        case "s":
                            try
                            {
                                sniffTime = Int32.Parse(val);
                            }
                            catch
                            {
                                Console.Error.WriteLine("Sniff time provided for 's' is not a valid number.");
                                SetFailure();
                                return;
                            }
                            break;

                        case "t":
                            try
                            {
                                nThreads = Int32.Parse(val);
                            }
                            catch
                            {
                                Console.Error.WriteLine("Thread count provided for 't' is not a valid number.");
                                SetFailure();
                                return;
                            }
                            break;

                        case "a":
                            excludeNonCluster = false;
                            break;

                        default:
                            if (i == args.Length - 1)
                            {
                                string shosts = args[i];
                                if (shosts.StartsWith("@"))
                                {
                                    hosts = Surrogate.GetHostsFromFile(shosts.Substring(1));
                                }
                                else
                                {
                                    hosts = shosts.Split(';', ',');
                                }
                            }
                            else
                            {
                                Console.Error.WriteLine("Action not invalid for packetsniff");
                                SetFailure();
                                return;
                            }
                            break;
                    }
                }
            }

            if (null == hosts)
            {
                dfs dc = LoadDfsConfig();
                hosts = dc.Slaves.SlaveList.Split(';');
            }

            if (nThreads <= 0)
            {
                nThreads = hosts.Length;
            }

            //IP lookups
            Dictionary<string, string> dtIP = new Dictionary<string, string>();
            foreach (string host in hosts)
            {
                System.Net.IPAddress[] addresslist = System.Net.Dns.GetHostAddresses(host);
                for (int i = 0; i < addresslist.Length; i++)
                {
                    if (System.Net.Sockets.AddressFamily.InterNetwork == addresslist[i].AddressFamily)
                    {
                        string ip = addresslist[i].ToString();
                        dtIP.Add(ip, host);
                    }
                }
            }

            //source, target, bytes
            Dictionary<string, Dictionary<string, ulong>> dtSources = new Dictionary<string, Dictionary<string, ulong>>();
            //target, source, bytes
            Dictionary<string, Dictionary<string, ulong>> dtTargets = new Dictionary<string, Dictionary<string, ulong>>();

            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string host)
            {
                lock (dtIP)
                {
                    Console.WriteLine("Sniffing {0}...", host);
                }

                Surrogate.PacketSniff(host, sniffTime);

                lock (dtIP)
                {
                    Console.WriteLine("Done sniffing {0}", host);
                }

                string filepath = Surrogate.NetworkPathForHost(host) + "\\packetsniff.txt";
                string[] lines = null;
                try
                {
                    lines = System.IO.File.ReadAllLines(filepath);
                }
                catch
                {
                    lock (dtIP)
                    {
                        Console.Error.WriteLine("Error while reading sniff data file: {0}", filepath);
                        return;
                    }
                }

                if (lines != null)
                {
                    Dictionary<string, Dictionary<string, ulong>> dtMatrix = new Dictionary<string, Dictionary<string, ulong>>();

                    foreach (string line in lines)
                    {
                        string[] parts = line.Split('|');
                        string type = parts[1].ToLower();

                        if (type != "tcp")
                        {
                            continue;
                        }

                        string srcIP = parts[2];
                        int del = srcIP.IndexOf(':');
                        if (del > -1)
                        {
                            srcIP = srcIP.Substring(0, del);
                        }

                        string tarIP = parts[3];
                        del = tarIP.IndexOf(':');
                        if (del > -1)
                        {
                            tarIP = tarIP.Substring(0, del);
                        }

                        ulong bc = ulong.Parse(parts[4]);

                        string srcName = srcIP;

                        if (dtIP.ContainsKey(srcIP))
                        {
                            srcName = dtIP[srcIP];
                        }
                        else if (excludeNonCluster)
                        {
                            continue;
                        }

                        string tarName = tarIP;

                        if (dtIP.ContainsKey(tarIP))
                        {
                            tarName = dtIP[tarIP];
                        }
                        else if (excludeNonCluster)
                        {
                            continue;
                        }

                        //If host is talking to itself, ignore this line.
                        if (string.Compare(tarName, srcName, true) == 0 && string.Compare(srcName, host, true) == 0)
                        {
                            continue;
                        }

                        if (!dtMatrix.ContainsKey(srcName))
                        {
                            dtMatrix.Add(srcName, new Dictionary<string, ulong>());
                        }

                        Dictionary<string, ulong> tars = dtMatrix[srcName];

                        if (!tars.ContainsKey(tarName))
                        {
                            tars.Add(tarName, bc);
                        }
                        else
                        {
                            tars[tarName] += bc;
                        }
                    }

                    //Put in the global dt.
                    lock (dtIP)
                    {
                        foreach (KeyValuePair<string, Dictionary<string, ulong>> j in dtMatrix)
                        {
                            string srcName = j.Key;

                            if (!dtSources.ContainsKey(srcName))
                            {
                                dtSources.Add(srcName, new Dictionary<string, ulong>());
                            }

                            Dictionary<string, ulong> tars = dtSources[srcName];

                            foreach (KeyValuePair<string, ulong> k in j.Value)
                            {
                                string tarName = k.Key;
                                ulong bc = k.Value;

                                if (!tars.ContainsKey(tarName))
                                {
                                    tars.Add(tarName, bc);
                                }
                                else if (bc > tars[tarName])
                                {
                                    tars[tarName] = bc; //only take the max one.
                                }

                                if (!dtTargets.ContainsKey(tarName))
                                {
                                    dtTargets.Add(tarName, new Dictionary<string, ulong>());
                                }

                                Dictionary<string, ulong> srcs = dtTargets[tarName];

                                if (!srcs.ContainsKey(srcName))
                                {
                                    srcs.Add(srcName, bc);
                                }
                                else if (bc > srcs[srcName])
                                {
                                    srcs[srcName] = bc;
                                }
                            }
                        }
                    }
                }
            }
            ), hosts, nThreads);

            Comparison<KeyValuePair<string, ulong>> compStrUlong = delegate(KeyValuePair<string, ulong> firstPair, KeyValuePair<string, ulong> nextPair)
            {
                return firstPair.Value.CompareTo(nextPair.Value);
            };

            foreach (string host in hosts)
            {
                Console.WriteLine();
                Console.WriteLine("Machine: {0}", host);

                if (dtSources.ContainsKey(host))
                {
                    Dictionary<string, ulong> readers = dtSources[host];
                    List<KeyValuePair<string, ulong>> sReaders = new List<KeyValuePair<string, ulong>>(readers);
                    sReaders.Sort(compStrUlong);

                    ulong bc = 0;
                    ulong avg = 0;

                    foreach (ulong v in readers.Values)
                    {
                        bc += v;
                    }

                    avg = bc / (ulong)readers.Count;

                    Console.WriteLine("Total readers: {0}", readers.Count);
                    Console.WriteLine("Total bytes read: {0}", GetFriendlyByteSize((long)bc));
                    Console.WriteLine("Avg bytes read: {0}", GetFriendlyByteSize((long)avg));

                    Console.WriteLine("Min reader:");
                    Console.WriteLine("{0}:     {1}", sReaders[0].Key, GetFriendlyByteSize((long)sReaders[0].Value));

                    Console.WriteLine("Max reader:");
                    Console.WriteLine("{0}:     {1}", sReaders[sReaders.Count - 1].Key, GetFriendlyByteSize((long)sReaders[sReaders.Count - 1].Value));

                    if (verbose)
                    {
                        Console.WriteLine("Sorted readers:");

                        foreach (KeyValuePair<string, ulong> pair in sReaders)
                        {
                            Console.WriteLine("{0}:     {1}", pair.Key, GetFriendlyByteSize((long)pair.Value));
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Total readers: 0");
                }

                if (dtTargets.ContainsKey(host))
                {
                    Dictionary<string, ulong> writers = dtTargets[host];
                    List<KeyValuePair<string, ulong>> sWriters = new List<KeyValuePair<string, ulong>>(writers);
                    sWriters.Sort(compStrUlong);

                    Console.WriteLine("Total writers: {0}", writers.Count);

                    ulong bc = 0;
                    ulong avg = 0;

                    foreach (ulong v in writers.Values)
                    {
                        bc += v;
                    }

                    avg = bc / (ulong)writers.Count;

                    Console.WriteLine("Total bytes written: {0}", GetFriendlyByteSize((long)bc));
                    Console.WriteLine("Avg bytes written: {0}", GetFriendlyByteSize((long)avg));

                    Console.WriteLine("Min writer:");
                    Console.WriteLine("{0}:     {1}", sWriters[0].Key, GetFriendlyByteSize((long)sWriters[0].Value));

                    Console.WriteLine("Max writer:");
                    Console.WriteLine("{0}:     {1}", sWriters[sWriters.Count - 1].Key, GetFriendlyByteSize((long)sWriters[sWriters.Count - 1].Value));

                    if (verbose)
                    {
                        Console.WriteLine("Sorted writers:");

                        foreach (KeyValuePair<string, ulong> pair in sWriters)
                        {
                            Console.WriteLine("{0}:     {1}", pair.Key, GetFriendlyByteSize((long)pair.Value));
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Total writers: 0");
                }
            }

            Comparison<KeyValuePair<string, int>> compStrInt = delegate(KeyValuePair<string, int> firstPair, KeyValuePair<string, int> nextPair)
            {
                return firstPair.Value.CompareTo(nextPair.Value);
            };

            if (dtSources.Count > 0)
            {
                Dictionary<string, int> dtTotalReaders = new Dictionary<string, int>();

                foreach (KeyValuePair<string, Dictionary<string, ulong>> pair in dtSources)
                {
                    dtTotalReaders.Add(pair.Key, pair.Value.Count);
                }

                List<KeyValuePair<string, int>> sTotalReaders = new List<KeyValuePair<string, int>>(dtTotalReaders);
                sTotalReaders.Sort(compStrInt);

                Console.WriteLine();
                Console.WriteLine("Machine with the least readers: {0} with reader count: {1}", sTotalReaders[0].Key, sTotalReaders[0].Value);
                Console.WriteLine("Machine with the most readers: {0} with reader count: {1}", sTotalReaders[sTotalReaders.Count - 1].Key, sTotalReaders[sTotalReaders.Count - 1].Value);

                List<string> noreaders = new List<string>();

                foreach (string host in hosts)
                {
                    if (!dtSources.ContainsKey(host))
                    {
                        noreaders.Add(host);
                    }
                }

                if (noreaders.Count > 0)
                {
                    Console.WriteLine("Machine in cluster with no readers:");
                    foreach (string s in noreaders)
                    {
                        Console.WriteLine(s);
                    }
                }
            }

            if (dtTargets.Count > 0)
            {
                Dictionary<string, int> dtTotalWriters = new Dictionary<string, int>();

                foreach (KeyValuePair<string, Dictionary<string, ulong>> pair in dtTargets)
                {
                    dtTotalWriters.Add(pair.Key, pair.Value.Count);
                }

                List<KeyValuePair<string, int>> sTotalWriters = new List<KeyValuePair<string, int>>(dtTotalWriters);
                sTotalWriters.Sort(compStrInt);

                Console.WriteLine();
                Console.WriteLine("Machine with the least writers: {0} with writer count: {1}", sTotalWriters[0].Key, sTotalWriters[0].Value);
                Console.WriteLine("Machine with the most writers: {0} with writer count: {1}", sTotalWriters[sTotalWriters.Count - 1].Key, sTotalWriters[sTotalWriters.Count - 1].Value);

                List<string> nowriter = new List<string>();

                foreach (string host in hosts)
                {
                    if (!dtTargets.ContainsKey(host))
                    {
                        nowriter.Add(host);
                    }
                }

                if (nowriter.Count > 0)
                {
                    Console.WriteLine("Machine in cluster with no writers:");
                    foreach (string s in nowriter)
                    {
                        Console.WriteLine(s);
                    }
                }
            }

            if (dtSources.Count > 0)
            {
                Dictionary<string, ulong> dtFlat = new Dictionary<string, ulong>();

                foreach (KeyValuePair<string, Dictionary<string, ulong>> j in dtSources)
                {
                    string src = j.Key;

                    foreach (KeyValuePair<string, ulong> k in j.Value)
                    {
                        string tar = k.Key;
                        dtFlat.Add(src + " | " + tar, k.Value);
                    }
                }

                List<KeyValuePair<string, ulong>> sFlat = new List<KeyValuePair<string, ulong>>(dtFlat);
                sFlat.Sort(compStrUlong);

                Console.WriteLine();
                Console.WriteLine("Min traffic:");
                Console.WriteLine("{0}: {1}", sFlat[0].Key, GetFriendlyByteSize((long)sFlat[0].Value));

                Console.WriteLine();
                Console.WriteLine("Max traffic:");
                Console.WriteLine("{0}: {1}", sFlat[sFlat.Count - 1].Key, GetFriendlyByteSize((long)sFlat[sFlat.Count - 1].Value));
            }

            Console.WriteLine();
            Console.WriteLine("packetsniff Request Time: {0}", startTime.ToString());
            Console.WriteLine("packetsniff End Time: {0}", DateTime.Now.ToString());
        }

        public static void SafePS()
        {
            SafePS(false);
        }

        public static void SafePS(bool ShowStatus)
        {
            using (System.Threading.Mutex lm = new System.Threading.Mutex(false, "DOexeclog"))
            {
                lm.WaitOne(); // Lock also taken by kill.

                string fn = AELight_Dir + @"\execq.dat";
                if (System.IO.File.Exists(fn))
                {
                    string[] lines;
                    {
                        const int iMAX_SECS_RETRY = 10; // Note: doesn't consider the time spent waiting on I/O.
                        const int ITER_MS_WAIT = 100; // Milliseconds to wait each retry.
                        int iters = iMAX_SECS_RETRY * 1000 / ITER_MS_WAIT;
                        for (; ; )
                        {
                            try
                            {
                                lines = System.IO.File.ReadAllLines(fn);
                                break;
                            }
                            catch
                            {
                                if (--iters < 0)
                                {
                                    throw;
                                }
                                System.Threading.Thread.Sleep(ITER_MS_WAIT);
                                continue;
                            }
                        }
                    }

                    //Console.WriteLine("Running:");
                    for (int il = 0; il < lines.Length; il++)
                    {
                        int ippp = lines[il].IndexOf("+++");
                        if (-1 != ippp)
                        {
                            string[] settings = lines[il].Substring(0, ippp).Split(' ');
                            string pssjid = "0";
                            if (settings.Length > 1)
                            {
                                pssjid = settings[1];
                            }
                            string ln = lines[il].Substring(ippp + 4);
                            Console.Write("  {0} {1}", pssjid, ln.Replace("drule", "SYSTEM"));
                            if (ShowStatus)
                            {
                                bool unk = true;
                                try
                                {
                                    long psjid = long.Parse(pssjid);
                                    pssjid = psjid.ToString(); // Normalize.
                                    if (psjid >= 1)
                                    {
                                        string[] jflines = System.IO.File.ReadAllLines(AELight_Dir + @"\" + pssjid + ".jid");
                                        int aelightpid = 0;
                                        foreach (string jln in jflines)
                                        {
                                            if (jln.StartsWith("pid="))
                                            {
                                                aelightpid = int.Parse(jln.Substring(4));
                                                break;
                                            }
                                        }
                                        if (aelightpid >= 1)
                                        {
                                            bool running = false;
                                            if (System.IO.File.Exists(AELight_Dir + @"\" + aelightpid.ToString() + ".aelight.pid"))
                                            {
                                                try
                                                {
                                                    System.Diagnostics.Process aelightproc = System.Diagnostics.Process.GetProcessById(aelightpid);
                                                    running = true;
                                                }
                                                catch (ArgumentException e)
                                                {
                                                    // The process specified by the processId parameter is not running.
                                                }
                                            }
                                            unk = false;
                                            if (running)
                                            {
                                                Console.Write("  {0}[running]{1}", isdspace ? "\u00011" : "", isdspace ? "\u00010" : "");
                                            }
                                            else
                                            {
                                                Console.Write("  {0}[zombie]{1}", isdspace ? "\u00015" : "", isdspace ? "\u00010" : "");
                                            }
                                        }
                                    }
                                }
                                catch
                                {
                                }
                                if (unk)
                                {
                                    Console.Write("  {0}[unknown]{1}", isdspace ? "\u00015" : "", isdspace ? "\u00010" : "");
                                }
                            }
                            Console.WriteLine();
                        }
                    }

                    {
                        Console.WriteLine("Queued:");
                        IList<MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.QEntry> qs
                            = MySpace.DataMining.DistributedObjects.Scheduler.GetQueueSnapshot();
                        int qsCount = qs.Count;
                        if (qsCount > 0)
                        {
                            for (int i = 0; i < qs.Count; i++)
                            {
                                Console.WriteLine("  {0} {1} [{2}] {3} (Position: {4})",
                                    qs[i].ID, qs[i].UserAdded, qs[i].TimeAdded.ToString(),
                                    qs[i].Command, i + 1);
                            }
                        }
                        else
                        {
                            Console.WriteLine("  None");
                        }
                    }

                    {
                        Console.WriteLine("Scheduled:");
                        IList<MySpace.DataMining.DistributedObjects.Scheduler.ScheduleInfo.SEntry> ss
                            = MySpace.DataMining.DistributedObjects.Scheduler.GetScheduleSnapshot();
                        int ssCount = ss.Count;
                        if (ssCount > 0)
                        {
                            for (int i = 0; i < ss.Count; i++)
                            {
                                Console.WriteLine("  {0} {1} [{2}] {3} (Next Run: {4})",
                                    ss[i].ID, ss[i].UserAdded, ss[i].TimeAdded.ToString(),
                                    ss[i].Command, ss[i].GetNextRunString());
                            }
                        }
                        else
                        {
                            Console.WriteLine("  None");
                        }
                    }

                }

                lm.ReleaseMutex();
            }
        }


        public static void ConsoleFlush()
        {
            if (isdspace)
            {
                Console.Write('\u0017'); // ETB
            }
            Console.Out.Flush();
            if (isdspace)
            {
                System.Threading.Thread.Sleep(50); // Prevent stdout/stderr out-of-order.
            }
        }


        public static bool DfsPutJobsFileContent(string dfspath, string filecontent)
        {
            dfs dc = LoadDfsConfig();
            if (null != DfsFind(dc, dfspath, DfsFileTypes.JOB))
            {
                //throw new Exception("Unable to write jobs file: jobs file already exists");
                return false;
            }

            string newactualfilehost;
            string newactualfilename;
            string newprettyfilename; // Pretty without dfs://
            string ActualFile;

            //+++metabackup+++
            string backupdir = dc.GetMetaBackupLocation();
            //---metabackup---

            newprettyfilename = dfspath;
            {
                if (newprettyfilename.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    newprettyfilename = newprettyfilename.Substring(6);
                }
            }
            string reason = "";
            if (dfs.IsBadFilename(newprettyfilename, out reason))
            {
                Console.Error.WriteLine("Invalid output file: {0}", reason);
                SetFailure();
                return false;
            }
            {
                //string[] slaves = dc.Slaves.SlaveList.Split(';');
                //newactualfilehost = slaves[(new Random()).Next() % slaves.Length];
                newactualfilehost = Surrogate.MasterHost;
                newactualfilename = GenerateZdFileDataNodeName(newprettyfilename);
            }
            ActualFile = NetworkPathForHost(newactualfilehost) + @"\" + newactualfilename;

            System.IO.File.WriteAllText(ActualFile, filecontent);
            //+++metabackup+++
            // Since this doesn't even exist in dfs.xml yet,
            // writing to the actual jobs file doesn't need to be transactional.
            if (null != backupdir)
            {
                try
                {
                    string backupfile = backupdir + @"\" + newactualfilename;
                    System.IO.File.WriteAllText(backupfile, filecontent);
                }
                catch (Exception eb)
                {
                    LogOutputToFile(eb.ToString());
                    throw new Exception("Error writing backup: " + eb.Message, eb);
                }
            }
            //---metabackup---

            System.IO.FileInfo finfo = new System.IO.FileInfo(ActualFile);
            // Need to add to DFS if it exists;
            // if it doesn't exist, the user probably just canceled it.
            if (finfo.Exists)
            {
                dfs.DfsFile df = new dfs.DfsFile();
                df.Nodes = new List<dfs.DfsFile.FileNode>(1);
                {
                    dfs.DfsFile.FileNode fnode = new dfs.DfsFile.FileNode();
                    fnode.Host = newactualfilehost;
                    fnode.Position = 0;
                    fnode.Length = finfo.Length;
                    fnode.Name = newactualfilename;
                    df.Nodes.Add(fnode);
                }
                df.Name = newprettyfilename;
                df.Size = finfo.Length;
                df.Type = DfsFileTypes.JOB;
                using (LockDfsMutex())
                {
                    dc = LoadDfsConfig(); // Reload in case of intermediate change.
                    if (null != DfsFindAny(dc, newactualfilehost))
                    {
                        Console.Error.WriteLine("Output file was created while editing");
                        SetFailure();
                        return false;
                    }
                    dc.Files.Add(df);
                    UpdateDfsXml(dc); // !
                }
            }
            return true; // !
        }


        public static string Shell(string line, bool suppresserrors)
        {
            return MySpace.DataMining.DistributedObjects.Exec.Shell(line, suppresserrors);
        }


        public static string Shell(string line)
        {
            return MySpace.DataMining.DistributedObjects.Exec.Shell(line, false);
        }


        static int maxconsole = 5;

        public static void LogOutput(string line)
        {
            lock (typeof(AELight))
            {
                if (maxconsole > 0)
                {
                    maxconsole--;
                    Console.WriteLine("{0}", line);
                }

                LogOutputToFile(line);
            }
        }

        public static void LogOutputToFile(string line)
        {
            try
            {
                lock (typeof(AELight))
                {
                    //System.IO.StreamWriter fstm = System.IO.File.AppendText("aelight-errors.txt");
                    System.IO.StreamWriter fstm = System.IO.File.AppendText(AELight_Dir + @"\aelight-errors.txt");
                    string build = "";
                    try
                    {
                        build = "(build:" + GetBuildInfo() + ") ";
                    }
                    catch
                    {
                    }
                    fstm.WriteLine("[{0}] {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
                    fstm.WriteLine("----------------------------------------------------------------");
                    fstm.Close();
                }
            }
            catch
            {
            }
        }

        private static void LogExecHistory(string[] args, string jobNetPath, int maxDisplay)
        {
            string guid = Guid.NewGuid().ToString();
            string command = "";

            foreach (string a in args)
            {
                command += a + " ";
            }

            string line = string.Format("{0}*[{1}] {2}{3}", guid, System.DateTime.Now.ToString(), command, Environment.NewLine);
            string fn = AELight_Dir + @"\exechistorylog.txt";
            string[] old = null;

            using (System.Threading.Mutex lm = new System.Threading.Mutex(false, "DOexeclog"))
            {
                lm.WaitOne(); // Lock also taken by kill.

                if (!System.IO.File.Exists(fn))
                {
                    System.IO.File.AppendAllText(fn, line);
                }
                else
                {
                    string[] lines = System.IO.File.ReadAllLines(fn);

                    if (lines.Length > maxDisplay * 2)
                    {
                        old = SubArray(lines, 0, lines.Length - maxDisplay);
                        lines = SubArray(lines, lines.Length - maxDisplay); 
                        
                        string alines = "";

                        foreach (string l in lines)
                        {
                            alines += l + Environment.NewLine;
                        }

                        alines += line;

                        System.IO.File.WriteAllText(fn, alines);
                    }
                    else
                    {
                        System.IO.File.AppendAllText(fn, line);
                    }
                }                

                lm.ReleaseMutex();
            }

            System.IO.File.Copy(jobNetPath, AELight_Dir + @"\history." + guid + ".txt");

            //Clean up old guid files.
            if (old != null)
            {
                foreach (string l in old)
                {
                    string oldguid = l.Substring(0, l.IndexOf('*'));
                    string oldfile = AELight_Dir + @"\history." + oldguid + ".txt";

                    if (System.IO.File.Exists(oldfile))
                    {
                        System.IO.File.Delete(oldfile);
                    }
                }
            }
        }

        static void GetRawBuildInfo(out int bn, out int rv)
        {
            System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();
            System.Reflection.AssemblyName an = asm.GetName();
            bn = an.Version.Build;
            rv = an.Version.Revision;
        }

        public static string GetBuildInfo()
        {
            int bn;
            int rv;
            GetRawBuildInfo(out bn, out rv);
            return bn.ToString() + "." + rv.ToString();
        }

        public static DateTime GetBuildDateTime()
        {
            int bn;
            int rv;
            GetRawBuildInfo(out bn, out rv);
            DateTime dt = new DateTime(2000, 1, 1, 0, 0, 0);
            dt += TimeSpan.FromDays(bn) + TimeSpan.FromSeconds(rv * 2);
            return dt;
        }


        [System.Runtime.InteropServices.DllImport("kernel32.dll")]
        static extern IntPtr OpenThread(uint dwDesiredAccess, bool bInheritHandle, uint dwThreadId);

        [System.Runtime.InteropServices.DllImport("kernel32.dll")]
        static extern uint SuspendThread(IntPtr hThread);


    }
    
}
