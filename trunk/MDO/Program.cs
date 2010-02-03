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

#define NOMASTER

//#define CLIENT_LOG_ALL

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;


namespace MySpace.DataMining.AELight
{
    class Program
    {
        //---------------------------------------------------------------

        static void Additional()
        {
        }
        static Dictionary<string, object> hashadd = new Dictionary<string,object>();

        //---------------------------------------------------------------

        public static string AELight_Dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        internal const string DFSXMLNAME = dfs.DFSXMLNAME;
        internal static readonly string DFSXMLPATH = AELight_Dir + @"\" + DFSXMLNAME;
        internal static bool logToFile = false;
        internal static string logFilePath;
        internal static bool retrylogmd5 = false;
        public static bool DebugSwitch = false;
        public static bool DebugStepSwitch = false;

#if DEBUG
        static bool _jobdebug = false;
        static string _jobdebug_cmdline = null;
        static DateTime _jobdebug_starttime;
        static List<string> _jobdebug_output = null;
        static int _jobdebug_exitcode = -999;
#endif

        static void Main(string[] args)
        {
#if DEBUG
            _jobdebug = System.IO.File.Exists("jobdebug.txt");
            if (_jobdebug)
            {
                _jobdebug_cmdline = Environment.CommandLine;
                _jobdebug_starttime = DateTime.Now;
                _jobdebug_output = new List<string>(1000);
            }
#endif

            int ex = 0;
            try
            {
                ProcessUser(args);
                Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);

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

                int aIndex = 0;

                for (; aIndex < args.Length; aIndex++)
                {
                    string thisArg = args[aIndex];
                    if (thisArg.StartsWith("-"))
                    {
                        thisArg = thisArg.Substring(1);
                    }

                    if (thisArg.ToUpper() == "LOG")
                    {
                        if (aIndex == args.Length - 1)
                        {
                            Console.Error.WriteLine("Invalid arguments for log");
                        }
                        else
                        {
                            logToFile = true;
                            logFilePath = args[aIndex + 1].Replace("\"", "\\\"");
                        }
                        break;
                    }
                    if (0 == string.Compare(thisArg, "retrylogmd5",
                        StringComparison.OrdinalIgnoreCase))
                    {
                        if (aIndex == args.Length - 1)
                        {
                            Console.Error.WriteLine("Invalid arguments for retrylogmd5");
                        }
                        else
                        {
                            logToFile = true;
                            logFilePath = args[aIndex + 1].Replace("\"", "\\\"");
                            retrylogmd5 = true;
                            retrylogbuf = new StringBuilder(0x400 * 8);
                            retrylogthread = new System.Threading.Thread(
                                new System.Threading.ThreadStart(retrylogthreadproc));
                            retrylogthread.Name = "retrylogthread";
                            Log("\r\n[StartExec time=\"" + DateTime.Now + "\"]\r\n");
                            retrylogthread.Start();
                        }
                        break;
                    }
                }

                if (logToFile)
                {
                    string[] cleanedArgs = new string[args.Length - 2];

                    for (int i = 0; i < args.Length; i++)
                    {
                        if (i != aIndex && i != aIndex + 1)
                        {
                            int k = i > aIndex ? i - 2 : i;
                            cleanedArgs[k] = args[i];
                        }
                    }

                    args = cleanedArgs;
                }

                if (args.Length > 0 && args[0].StartsWith("@=", StringComparison.OrdinalIgnoreCase))
                {
                    string otherhost = args[0].Substring(2);
                    string othermaster = Surrogate.LocateMasterHost(Surrogate.NetworkPathForHost(otherhost));
                    Surrogate.SetNewMasterHost(othermaster);
                    args = SubArray(args, 1);
                }

                string act = "";
                if (args.Length > 0)
                {
                    act = args[0].ToLower();
                    if (act.StartsWith("-"))
                    {
                        act = act.Substring(1);
                    }
                }

#if DEBUG
                //System.Threading.Thread.Sleep(8000);
#endif
                switch (act)
                {
                    case "rget":
                        {
                            if (args.Length > 2 && args[2].StartsWith(@"\\"))
                            {
                                string driverdir = Surrogate.NetworkPathForHost(Surrogate.MasterHost);
                                string rdir = driverdir + @"\rget";
                                try
                                {
                                    System.IO.Directory.CreateDirectory(rdir);
                                }
                                catch
                                {
                                }
                                string getname = args[1]; // Name only! not path.
                                string tempfilepath = rdir + @"\rget_" + Guid.NewGuid().ToString() + "_" + getname + ".rget";
                                Exec.Shell("DSpace get \"" + getname + "\" \"" + tempfilepath + "\"");
                                System.IO.File.Copy(tempfilepath, args[2]);
                                System.IO.File.Delete(tempfilepath);
                                Console.WriteLine("Done");
                            }
                            else
                            {
                                Console.Error.WriteLine("Error: DFS file and network path expected");
                            }
                        }
                        return;

                    case "rput":
                        {
                            if (args.Length > 1 && args[1].StartsWith(@"\\"))
                            {
                                System.IO.FileInfo fi = new System.IO.FileInfo(args[1]);
                                string driverdir = Surrogate.NetworkPathForHost(Surrogate.MasterHost);
                                string rdir = driverdir + @"\rput";
                                try
                                {
                                    System.IO.Directory.CreateDirectory(rdir);
                                }
                                catch
                                {
                                }
                                string putname = fi.Name; // Name only! not path.
                                if (args.Length > 2)
                                {
                                    putname = args[2];
                                }
                                string tempfilepath = rdir + @"\rput_" + Guid.NewGuid().ToString() + "_" + putname;

                                System.IO.File.Copy(args[1], tempfilepath);
                                Console.WriteLine(Exec.Shell("DSpace put \"" + tempfilepath + "\" \"" + putname + "\""));
                                System.IO.File.Delete(tempfilepath);
                            }
                            else
                            {
                                Console.Error.WriteLine("Error: network path expected");
                            }
                        }
                        return;
                }

                Additional();

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

                if (sargs.Length > 0 && logToFile)
                {
                    Log(sargs + "\r\n\r\n");
                }

                {
                    //if (args.Length >= 1)
                    {
                        switch (act)
                        {
                            case "exec":
                                if (DebugSwitch)
                                {
                                    goto case "edit";
                                }
                                break; // Normal handling.

                            case "edit":
                            case "editor":
                                Environment.CurrentDirectory = AELight_Dir;

                                int iarg = 1;
                                string ExecOpts = "";
                                List<string> xpaths = null;
                                {
                                    while (iarg < args.Length)
                                    {
                                        switch (args[iarg][0])
                                        {
                                            case '-':
                                                ExecOpts += " " + args[iarg].Substring(1);
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
                                }

                                _FixSciLexDLL();
                                if (args.Length <= iarg)
                                {
                                    Console.Error.WriteLine("Invalid arguments for " + args[0]);
                                    //ShowUsage();
                                    // Show blank usage...
                                    args = new string[0];
                                    break;
                                }
                                else if (0 == string.Compare("*errors*", args[iarg]) || 0 == string.Compare("*error*", args[iarg]))
                                {
                                    if (DebugSwitch)
                                    {
                                        Console.Error.WriteLine("Cannot debug {0}", args[iarg]);
                                        return;
                                    }
                                    string errorscsfp = Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\error.cs";
                                    if (System.IO.File.Exists(errorscsfp))
                                    {
                                        JobsEdit.RunJobsEditor(errorscsfp, "C# Errors", true);
                                    }
                                    else
                                    {
                                        Console.Error.WriteLine("No C# errors found");
                                    }
                                }
                                else
                                {
                                    string ActualFile, PrettyFile;

                                    {
                                        dfs dc = Surrogate.ReadMasterDfsConfig();

#if DEBUG
                                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                                        if (null != dc.DefaultDebugType)
                                        {
                                            hashadd["DefaultDebugType"] = dc.DefaultDebugType;
                                        }

                                        try
                                        {
                                            string backupdir = dc.GetMetaBackupLocation();
                                            if (!string.IsNullOrEmpty(backupdir))
                                            {
                                                hashadd.Add("backupdir", backupdir);
                                            }
                                        }
                                        catch (Exception eb)
                                        {
                                            LogOutputToFile(eb.ToString());
                                            Console.Error.WriteLine(eb.Message);
                                        }

                                        dfs.DfsFile dfjob = dc.Find(args[iarg], DfsFileTypes.JOB);
                                        if (null == dfjob)
                                        {
                                            /*if (!QuietMode)
                                            {
                                                Console.WriteLine("New jobs file");
                                            }*/
                                            string fn = args[iarg];

                                            if (fn.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                            {
                                                fn = fn.Substring(6);
                                            }

                                            string reason = "";
                                            if (dfs.IsBadFilename(fn, out reason))
                                            {
                                                Console.Error.WriteLine("Invalid job file name: " + reason);
                                                return;
                                            }

                                            ActualFile = null; // !
                                            PrettyFile = "dfs://" + fn;
                                        }
                                        else
                                        {
                                            if (dfjob.Nodes.Count < 1)
                                            {
                                                throw new Exception("Error: -exec jobs file not in correct jobs DFS format");
                                            }
                                            ActualFile = Surrogate.NetworkPathForHost(dfjob.Nodes[0].Host.Split(';')[0]) + @"\" + dfjob.Nodes[0].Name;
                                            PrettyFile = "dfs://" + dfjob.Name;
                                        }
                                    }

                                    {
                                        System.Threading.Thread editthread = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(editthreadproc));
                                        editthread.IsBackground = true;
                                        editthread.Start(sargs);
                                    }

                                    {
                                        string realuser = mdousername();
                                        int ix = realuser.IndexOfAny(new char[] { '@', '(' });
                                        if (-1 != ix)
                                        {
                                            realuser = realuser.Substring(0, ix);
                                        }
                                        JobsEdit.RealUserName = realuser;
                                    }

                                    if (DebugSwitch)
                                    {
                                        if (string.IsNullOrEmpty(ActualFile))
                                        {
                                            Console.Error.WriteLine("Cannot debug, jobs file does not exist: {0}", PrettyFile);
                                            return;
                                        }
                                        hashadd["DebugSwitch"] = "y";
                                        if (DebugStepSwitch)
                                        {
                                            hashadd["DebugStepSwitch"] = "y";
                                        }
                                    }

                                    hashadd["ExecArgs"] = SubArray(args, iarg + 1);

                                    if (null != xpaths)
                                    {
                                        hashadd["SourceCodeXPathSets"] = xpaths;
                                    }

                                    JobsEdit.RunJobsEditor(ActualFile, PrettyFile, hashadd);
                                }
                                return;

                            case "execview":
                                if (args.Length < 2)
                                {
                                    Console.Error.WriteLine("Invalid arguments for " + args[0]);
                                    return;
                                }
                                else
                                {
                                    string guid = args[1];
                                    string hostNetpath = Surrogate.NetworkPathForHost(Surrogate.MasterHost);
                                    string src = hostNetpath + @"\history." + guid + ".txt";
                                    string log = hostNetpath + @"\exechistorylog.txt";

                                    if (System.IO.File.Exists(log))
                                    {
                                        string logs = System.IO.File.ReadAllText(log);
                                        int gi = logs.IndexOf(guid);

                                        if (gi > -1)
                                        {
                                            int d1 = logs.IndexOf('*', gi);
                                            int d2 = logs.IndexOf(Environment.NewLine, d1 + 1);
                                            string command = logs.Substring(d1 + 1, d2 - d1 - 1);

                                            if (System.IO.File.Exists(src))
                                            {
                                                Environment.CurrentDirectory = AELight_Dir;
                                                _FixSciLexDLL();
                                                JobsEdit.RunJobsEditor(src, command, true);
                                            }
                                            else
                                            {
                                                Console.Error.WriteLine("No source code is found.");
                                            }
                                        }
                                        else
                                        {
                                            Console.Error.WriteLine("GUID is not found in the log file.");
                                        }
                                    }
                                    else
                                    {
                                        Console.Error.WriteLine("Log file is not found.");
                                    }
                                }
                                return;

                            case "importdir":
                            case "importdirst":
                                {
                                    if (args.Length <= 1)
                                    {
                                        Console.Error.WriteLine("Error: directory name expected");
                                        return;
                                    }
                                    else
                                    {
                                        importdir_recursive(args[1]);
                                    }
                                }
                                return;

                            case "importdirmt":
                                {
                                    if (args.Length <= 1)
                                    {
                                        Console.Error.WriteLine("Error: directory name expected");
                                        return;
                                    }
                                    else
                                    {
                                        importdir_recursive_mt(args[1]);
                                    }
                                }
                                return;

                            case "dfs":
                                if (args.Length > 1 && args[1].IndexOf("format", StringComparison.OrdinalIgnoreCase) != -1)
                                {
                                    goto format_cmd;
                                }
                                break;

                            case "clustercheck":
                                Admin.CheckClusters(SubArray(args, 1));
                                return;

                            case "recoverdfsxml":
                                Admin.Recovery.RecoverDfsXml(SubArray(args, 1));
                                return;

                            case "compspeed":
                                Admin.CompareSpeed(SubArray(args, 1));
                                return;

                            case "kick":
                                {
                                    if (args.Length < 2)
                                    {
                                        Console.Error.WriteLine("kick error: kick <username> expected.");
                                        return;
                                    }
                                    dfs dc = Surrogate.ReadMasterDfsConfig();
                                    Admin.KickUser(args[0], args[1], dc.Slaves.SlaveList);
                                    return;
                                }

                            case "kickall":
                                {
                                    if (args.Length < 3)
                                    {
                                        Console.Error.WriteLine("kickall error: kickall <username> <hosts> expected.");
                                        return;
                                    }
                                    Admin.KickUser(args[0], args[1], args[2]);
                                    return;
                                }

                            case "kickallusers":
                                {
                                    if (args.Length < 2)
                                    {
                                        Console.Error.WriteLine("kickallusers error: kickallusers <hosts> expected.");
                                        return;
                                    }
                                    string thisuser = _mdousername.Split('@')[0];
                                    Admin.KickAllUsers(args[0], new string[] { thisuser, "dataminingdspace" }, args[1]);
                                    return;
                                }

                            case "ps":
                                // Only handling ps here if -d is used, otherwise it's a normal command.
                                if (args.Length > 1 && "-d" == args[1])
                                {
                                    string driverdir = Surrogate.NetworkPathForHost(Surrogate.MasterHost);
                                    string execqfn = driverdir + @"\execq.dat";
                                    for (int rotor = 0; ; rotor++)
                                    {
                                        try
                                        {
                                            string[] qlines = System.IO.File.ReadAllLines(execqfn);
                                            for (int iq = 0; iq < qlines.Length; iq++)
                                            {
                                                string ln = qlines[iq];
                                                int ippp = ln.IndexOf("+++");
                                                if (-1 != ippp)
                                                {
                                                    string[] settings = ln.Substring(0, ippp).Split(' ');
                                                    string psspid = "0";
                                                    if (settings.Length > 1)
                                                    {
                                                        psspid = settings[1];
                                                    }
                                                    ln = ln.Substring(ippp + 4);
                                                    Console.WriteLine("  {0} {1}", psspid, ln.Replace("drule", "SYSTEM"));
                                                }
                                            }
                                            break;
                                        }
                                        catch (System.IO.IOException ioe)
                                        {
                                            if (rotor > 10)
                                            {
                                                Console.Error.WriteLine(ioe.Message);
                                                break;
                                            }
                                            System.Threading.Thread.Sleep(200);
                                            continue;
                                        }
                                    }
                                    return; // Handled!
                                }
                                break; // Not handled, normal command.

                            case "killallst":
                            case "killall":
                            case "killallmt":
#if DEBUG
                                Console.WriteLine(@"User=\\" + System.Environment.UserDomainName + @"\" + System.Environment.UserName);
#endif
                                bool isForce = false;
                                string subact = "";
                                for (int ai = 1; ai < args.Length; ai++)
                                {
                                    if (args[ai].ToLower() == "-f")
                                    {
                                        isForce = true;
                                    }
                                    else
                                    {
                                        subact = args[ai];
                                    }
                                }

                                if (!isForce)
                                {
                                    Console.Error.WriteLine("WARNING: about to terminate all jobs on cluster.");
                                    Console.Error.WriteLine("To continue, use:  {0} {1} -f", "Qizmt", sargs);
                                    return;
                                }
                                else
                                {
                                    MakeInvincible();
                                    if (subact == "")
                                    {
                                        subact = "-f";
                                    }
                                    switch (subact)
                                    {
                                        case "xproxy":
                                            RunProxy("DSpace.exe " + act + " -f");
                                            return;

                                        case "proxy":
                                            RunStreamStdIO("localhost", "DSpace.exe " + act + " -f", true);
                                            return;

                                        case "logon2":
                                        case "logon3":
                                        case "logon4":
                                        case "logon5":
                                        case "logon6":
                                        case "logon7":
                                        case "logon8":
                                        case "logon9":
                                            break; // Keep going...

                                        case "-f":
                                            break;

                                        default:
                                            Console.Error.WriteLine("Unknown killall arguments");
                                            return;
                                    }

                                    try
                                    {
                                        Console.WriteLine(Exec.Shell("DSpace scrapeemptynames"));
                                    }
                                    catch
                                    {
                                    }

                                    {
                                        bool gotmaster = false;
                                        System.Threading.Thread masterthread = new System.Threading.Thread(
                                            new System.Threading.ThreadStart(delegate()
                                            {
                                                try
                                                {
                                                    // Connect to surrogate (and why not cache path).
                                                    Surrogate.NetworkPathForHost(Surrogate.MasterHost);
                                                    gotmaster = true;
                                                }
                                                catch
                                                {
                                                }
                                            }));
                                        masterthread.IsBackground = true;
                                        masterthread.Start();
                                        if (!masterthread.Join(1000 * 4) || !gotmaster) // At most, wait this long.
                                        {
                                            try
                                            {
                                                masterthread.Abort();
                                            }
                                            catch
                                            {
                                            }
                                            Console.Write('.');
                                            Exec.Shell(@"sc \\" + Surrogate.MasterHost + " stop DistributedObjects", true);
                                            System.Threading.Thread.Sleep(1000 * 10);
                                            Console.Write('.');
                                            Exec.Shell(@"sc \\" + Surrogate.MasterHost + " start DistributedObjects");
                                            System.Threading.Thread.Sleep(1000 * 10);
                                            Console.WriteLine('.');
                                        }
                                    }

                                    try
                                    {
                                        MasterRunStreamStdIO("-@log " + sargs);
                                    }
                                    catch
                                    {
                                    }

                                    bool singlethreaded = act == "killallst";

                                    try
                                    {
                                        if (singlethreaded)
                                        {
                                            Exec.Shell("dspace delst *.$*.$*");
                                        }
                                        else
                                        {
                                            Exec.Shell("dspace delmt *.$*.$*");
                                        }
                                    }
                                    catch
                                    {
                                    }

                                    try
                                    {
                                        if (singlethreaded)
                                        {
                                            Exec.Shell("dspace delst *" + dfs.TEMP_FILE_MARKER + "*");
                                        }
                                        else
                                        {
                                            Exec.Shell("dspace delmt *" + dfs.TEMP_FILE_MARKER + "*");
                                        }
                                    }
                                    catch
                                    {
                                    }

                                    dfs dc;
                                    try
                                    {
                                        dc = dfs.ReadDfsConfig_unlocked(Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\" + dfs.DFSXMLNAME);
                                    }
                                    catch (Exception e)
                                    {
                                        LogOutputToFile("killallmt accessing surrogate: " + e.ToString());
                                        Console.Error.WriteLine("Ensure the Windows services are running; {0}", e.Message);
                                        return;
                                    }

                                    List<string> hosts = new List<string>();
                                    hosts.AddRange(dc.Slaves.SlaveList.Split(';'));
                                    {
                                        // Ensure the master is at the beginning and shuts down first! (easier than admin-cmd-lock)
                                        string xmaster = IPAddressUtil.GetNameNoCache(Surrogate.MasterHost);
                                        for (int hi = 0; hi < hosts.Count; hi++)
                                        {
                                            if (0 == string.Compare(xmaster, IPAddressUtil.GetNameNoCache(hosts[hi]), StringComparison.OrdinalIgnoreCase))
                                            {
                                                hosts.RemoveAt(hi);
                                                break; // !
                                            }
                                        }
                                        hosts.Insert(0, Surrogate.MasterHost);
                                    }

                                    int threadcount = singlethreaded ? 1 : hosts.Count;
                                    if (threadcount > 15)
                                    {
                                        threadcount = 15;
                                    }

                                    //Console.WriteLine();
                                    Console.WriteLine("Stopping services");

                                    int nkilled = 0;
                                    //for (int hi = 0; hi < hosts.Count; hi++)
                                    MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                        new Action<string>(delegate(string host)
                                        {
                                            //string host = hosts[hi];

                                            string netpath = null;
                                            try
                                            {
                                                netpath = Surrogate.NetworkPathForHost(host);

                                                {
                                                    string driverdir = Surrogate.NetworkPathForHost(Surrogate.MasterHost);
                                                    {
                                                        string dir = driverdir + @"\rget";
                                                        if (System.IO.Directory.Exists(dir))
                                                        {
                                                            foreach (string fn in System.IO.Directory.GetFiles(dir))
                                                            {
                                                                System.IO.File.Delete(fn);
                                                            }
                                                            System.IO.Directory.Delete(dir);
                                                        }
                                                    }
                                                    {
                                                        string dir = driverdir + @"\rput";
                                                        if (System.IO.Directory.Exists(dir))
                                                        {
                                                            foreach (string fn in System.IO.Directory.GetFiles(dir))
                                                            {
                                                                System.IO.File.Delete(fn);
                                                            }
                                                            System.IO.Directory.Delete(dir);
                                                        }
                                                    }
                                                }
                                            }
                                            catch (Exception e)
                                            {
#if CLIENT_LOG_ALL
                                    LogOutputToFile("CLIENT_LOG_ALL: NetworkPathForHost failed for " + host + ": " + e.ToString());
#endif
                                            }

                                            //Console.WriteLine("{0}: stopping service", host);
                                            string scresult = Exec.Shell(@"sc \\" + host + " stop DistributedObjects", true); // Suppress error.
                                            lock (hosts)
                                            {
                                                Console.Write("{0}: ", host);
                                                Console.WriteLine(scresult);
                                            }
                                            System.Threading.Thread.Sleep(1000 * 2);
                                            if (null == netpath)
                                            {
                                                for (int somewait = 0; ; somewait++)
                                                {
                                                    if (somewait >= 3)
                                                    {
                                                        lock (hosts)
                                                        {
                                                            Console.Write("Network path of host {0} is null", host);
                                                        }
                                                        break;
                                                    }
                                                    System.Threading.Thread.Sleep(1000 * 4);
                                                    lock (hosts)
                                                    {
                                                        Console.Write("Network path of host {0} is null", host);
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                for (int somewait = 0; 0 != System.IO.Directory.GetFiles(netpath, "*.pid").Length; somewait++)
                                                {
                                                    if (somewait >= 10)
                                                    {
                                                        lock (hosts)
                                                        {
                                                            Console.Write('?');
                                                        }
                                                        break;
                                                    }
                                                    System.Threading.Thread.Sleep(1000 * 4);
                                                    lock (hosts)
                                                    {
                                                        Console.Write('.');
                                                    }
                                                }
                                            }
                                        }
                                    ), hosts, threadcount);

                                    //System.Threading.Thread.Sleep(1000 * 2);
                                    Console.WriteLine();
                                    Console.WriteLine("Cleaning intermediate data");

                                    string backupdir = dc.GetMetaBackupLocation();

                                    Dictionary<string, bool> jobnames = null;
                                    if (null != backupdir)
                                    {
                                        jobnames = new Dictionary<string, bool>(100, new Surrogate.CaseInsensitiveEqualityComparer()); // host\jobfilenodename keys.
                                    }
                                    Dictionary<string, bool> goodfiles = new Dictionary<string, bool>(300, new Surrogate.CaseInsensitiveEqualityComparer()); // host\filenodename keys.
                                    List<System.Text.RegularExpressions.Regex> snowballregexes = new List<System.Text.RegularExpressions.Regex>();
                                    List<System.Text.RegularExpressions.Regex> snowballzfoilcacheregexes = new List<System.Text.RegularExpressions.Regex>();
                                    List<string> mappedsamplenames = new List<string>();
                                    foreach (dfs.DfsFile df in dc.Files)
                                    {
                                        if (0 == string.Compare(df.Type, DfsFileTypes.DELTA, StringComparison.OrdinalIgnoreCase))
                                        {
                                            string snowballname = df.Name;
                                            {
                                                string srex = Surrogate.WildcardRegexString(GetSnowballFilesWildcard(snowballname));
                                                System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                                                snowballregexes.Add(rex);
                                            }
                                            {
                                                string srex = Surrogate.WildcardRegexString(GetSnowballFoilSampleFilesWildcard(snowballname));
                                                System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                                                snowballzfoilcacheregexes.Add(rex);
                                            }
                                            string fnms = "zsballsample_" + snowballname + ".zsb";
                                            mappedsamplenames.Add(fnms);
                                        }
                                        else
                                        {
                                            if (null != backupdir)
                                            {
                                                if (0 == string.Compare(DfsFileTypes.JOB, df.Type, StringComparison.OrdinalIgnoreCase))
                                                {
                                                    foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                                                    {
                                                        jobnames[fn.Name] = true;
                                                    }
                                                }
                                            }
                                            foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                                            {
                                                foreach (string fnhost in fn.Host.Split(';'))
                                                {
                                                    goodfiles[IPAddressUtil.GetName(fnhost) + @"\" + fn.Name] = true;
                                                }
                                            }
                                        }
                                    }

                                    // Get leaked metabackups:
                                    {
                                        if (null != backupdir)
                                        {
                                            //foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(backupdir)).GetFiles("zd.*-????-????-????-*.zd"))
                                            MySpace.DataMining.Threading.ThreadTools<System.IO.FileInfo>.Parallel(
                                                new Action<System.IO.FileInfo>(
                                                delegate(System.IO.FileInfo fi)
                                                {
                                                    try
                                                    {
                                                        if (!jobnames.ContainsKey(fi.Name))
                                                        {
                                                            fi.Delete();
                                                            lock (hosts)
                                                            {
                                                                Console.Write('.'); // ?
                                                            }
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        LogOutputToFile("killall 'delete leaked metabackup zd file' exception: " + e.ToString());
                                                    }
                                                }), (new System.IO.DirectoryInfo(backupdir)).GetFiles("zd.*-????-????-????-*.zd"), threadcount);
                                        }
                                    }

#if DEBUG
                                    //System.Threading.Thread.Sleep(1000 * 8);
                                    int ixjxj = 33 + 33;
#endif

                                    MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                        new Action<string>(delegate(string host)
                                        //for (int hi = 0; hi < hosts.Count; hi++)
                                        {

                                            //string host = hosts[hi];
                                            string netpath = null;

                                            try
                                            {
                                                netpath = Surrogate.NetworkPathForHost(host);
                                            }
                                            catch (Exception e)
                                            {
#if CLIENT_LOG_ALL
                                    LogOutputToFile("CLIENT_LOG_ALL: NetworkPathForHost failed for " + host + ": " + e.ToString());
#endif
                                                lock (hosts)
                                                {
                                                    Console.WriteLine("Skipping host, killall may have been aboarted early last run.");
                                                }
                                            }

                                            if (netpath != null)
                                            {
                                                //Console.WriteLine("{0}: cleaning intermediate data", host);
#if CLIENT_LOG_ALL
                                    Console.WriteLine("    CLIENT_LOG_ALL: cleaning leaked zd files...");
                                    int nlzdf = 0;
#endif
                                                foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles("zd.*-????-????-????-*.zd"))
                                                {
                                                    try
                                                    {
                                                        if (!goodfiles.ContainsKey(IPAddressUtil.GetName(host) + @"\" + fi.Name))
                                                        {
                                                            fi.Delete();
                                                            lock (hosts)
                                                            {
                                                                Console.Write('.'); // ?
                                                            }
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        LogOutputToFile("killall 'delete leaked zd file' exception: " + e.ToString());
                                                    }
                                                }
                                                foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles("zd.*-????-????-????-*.zd.zsa"))
                                                {
                                                    try
                                                    {
                                                        if (!goodfiles.ContainsKey(IPAddressUtil.GetName(host) + @"\" + fi.Name.Substring(0, fi.Name.Length - 4)))
                                                        {
                                                            fi.Delete();
                                                            lock (hosts)
                                                            {
                                                                Console.Write('.'); // ?
                                                            }
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        LogOutputToFile("killall 'delete leaked zsa file' exception: " + e.ToString());
                                                    }
                                                }
#if CLIENT_LOG_ALL
                                    Console.WriteLine("    CLIENT_LOG_ALL: cleaned " + nlzdf.ToString() + " zd files");
#endif
                                                {
                                                    // Clean leaked snowballs...
                                                    {
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
                                                                try
                                                                {
                                                                    fi.Delete();
                                                                    Console.Write('.'); // ?
                                                                }
                                                                catch (Exception e)
                                                                {
                                                                    LogOutputToFile("killall 'delete leaked deltaCache file' exception: " + e.ToString());
                                                                    Console.Error.WriteLine("Warning: unable to delete leaked file '{0}'; cache file may be corrupt: {1}", fi.Name, e.Message);
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
                                                                try
                                                                {
                                                                    fi.Delete();
                                                                    Console.Write('.'); // ?
                                                                }
                                                                catch (Exception e)
                                                                {
                                                                    LogOutputToFile("killall 'delete leaked deltaCache file' exception: " + e.ToString());
                                                                    Console.Error.WriteLine("Warning: unable to delete leaked file '{0}'; cache file may be corrupt: {1}", fi.Name, e.Message);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    {
                                                        // zfoil cache!
                                                        int snowballzfoilcacheregexesCount = snowballzfoilcacheregexes.Count;
                                                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles("zfoil_*.zf"))
                                                        {
                                                            bool goodsnowball = false;
                                                            for (int i = 0; i < snowballzfoilcacheregexesCount; i++)
                                                            {
                                                                if (snowballzfoilcacheregexes[i].IsMatch(fi.Name))
                                                                {
                                                                    goodsnowball = true;
                                                                    break;
                                                                }
                                                            }
                                                            if (!goodsnowball)
                                                            {
                                                                try
                                                                {
                                                                    fi.Delete();
                                                                    Console.Write('.'); // ?
                                                                }
                                                                catch (Exception e)
                                                                {
                                                                    LogOutputToFile("killall 'delete leaked deltaCache file' exception: " + e.ToString());
                                                                    Console.Error.WriteLine("Warning: unable to delete leaked file '{0}'; cache file may be corrupt: {1}", fi.Name, e.Message);
                                                                }
                                                            }
                                                        }
                                                    }

                                                }
                                                {
#if DEBUG
                                                    //System.Threading.Thread.Sleep(1000 * 8);
#endif
                                                    DeleteAllMatchingFiles(netpath, "ylib_*_*.*", true); // ylib_*.ylib and ylib_*.pdb
                                                    //DeleteAllMatchingFiles(netpath, "*.xlib", true);
                                                    DeleteOldMatchingFiles(new TimeSpan(10 /* days */ , 0, 0),
                                                        netpath, "*.xlib", true);
                                                    DeleteAllMatchingFiles(netpath, "*.zb", true);
                                                    DeleteAllMatchingFiles(netpath, "zmap_*.zm", true);
                                                    DeleteAllMatchingFiles(netpath, "temp_*-????-????-????-*.pdb", true);
                                                    DeleteAllMatchingFiles(netpath, "temp_*-????-????-????-*.dll", true);
                                                    //DeleteAllMatchingFiles(netpath, "zfoil_*.zf", true); // NO! now part of snowball cleanup
                                                    DeleteAllMatchingFiles(netpath, "dbg_*~*_????????-????-????-????-????????????.*", true);
                                                    DeleteOldMatchingFiles(new TimeSpan(10 /* days */ , 0, 0),
                                                        netpath, "*_????????-????-????-????-????????????*_log.txt", true);
                                                    try
                                                    {
                                                        // Clean ALL of temp dir!
                                                        DeleteAllMatchingFiles(netpath + @"\temp", "*", true);
                                                    }
                                                    catch
                                                    {
                                                    }
                                                    DeleteAllMatchingFiles(netpath, "slaveconfig.j*.xml");
                                                    DeleteOldMatchingFiles(new TimeSpan(1 /* days */ , 0, 0),
                                                        netpath, "stdout.jid*.jso", true);
                                                }
                                                //Console.WriteLine();
                                            }
                                        }
                                    ), hosts, threadcount);

                                    try
                                    {
                                        foreach (string jidfn in System.IO.Directory.GetFiles(
                                            Surrogate.NetworkPathForHost(Surrogate.MasterHost),
                                            "*.jid"))
                                        {
                                            try
                                            {
                                                System.IO.File.Delete(jidfn);
                                            }
                                            catch
                                            {
                                            }
                                        }
                                    }
                                    catch
                                    {
                                    }

                                    Console.WriteLine();
                                    Console.WriteLine();
                                    Console.WriteLine("Starting services");

                                    MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                        new Action<string>(delegate(string host)
                                        //for (int hi = 0; hi < hosts.Count; hi++)
                                        {
                                            //string host = hosts[hi];
                                            //string netpath = NetworkPathForHost(host);
                                            //Console.WriteLine("{0}: starting service", host);
                                            string scresult = Exec.Shell(@"sc \\" + host + " start DistributedObjects", false); // Throws on error.
                                            lock (hosts)
                                            {
                                                Console.Write("{0}: ", host);
                                                ConsoleColor oldc = Console.ForegroundColor;
                                                if (-1 == scresult.IndexOf("STATE") || -1 == scresult.IndexOf("START"))
                                                {
                                                    Console.ForegroundColor = ConsoleColor.Red;
                                                }
                                                Console.WriteLine(scresult);
                                                Console.ForegroundColor = oldc;
                                            }

                                            //Console.WriteLine("{0}: done", host);

                                            //Console.WriteLine("--------");
                                            //System.Threading.Thread.Sleep(1000); // Sleep a sec, give the user a chance to see it.

                                            nkilled++;
                                        }
                                    ), hosts, threadcount);

                                    try
                                    {
                                        System.IO.File.Delete(Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\execq.dat"); // Clear jobs list, since I killed them anyway!
                                    }
                                    catch
                                    {
                                    }

                                    Console.WriteLine("Done; killed and restarted {0} nodes", nkilled);
                                }
                                return;

                            case "metapath":
                                Console.WriteLine(Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\" + dfs.DFSXMLNAME);
                                return;

                            case "stopall":
                            case "servicestopall":
                                {
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
                                        dfs dc = dfs.ReadDfsConfig_unlocked(Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\" + dfs.DFSXMLNAME);
                                        hosts = dc.Slaves.SlaveList.Split(';');
                                        // Always include self host if current cluster.
                                        if (null == IPAddressUtil.FindCurrentHost(hosts))
                                        {
                                            List<string> xhosts = new List<string>(1 + hosts.Length);
                                            xhosts.Add(System.Net.Dns.GetHostName());
                                            xhosts.AddRange(hosts);
                                            hosts = xhosts.ToArray();
                                        }
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
                                            try
                                            {
                                                Console.WriteLine("Stopping service on {0}...", host);
                                                Console.WriteLine(Exec.Shell("sc \\\\" + host + " stop DistributedObjects").Trim());
                                            }
                                            catch (Exception e)
                                            {
                                                Console.WriteLine("Problem stopping service on {0}: {1}", host, e.Message);
                                            }
                                        }
                                        ), hosts, threadcount);
                                    Console.WriteLine("Done");
                                }
                                return;

                            case "startall":
                            case "servicestartall":
                                {
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
                                        dfs dc = dfs.ReadDfsConfig_unlocked(Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\" + dfs.DFSXMLNAME);
                                        hosts = dc.Slaves.SlaveList.Split(';');
                                        // Always include self host if current cluster.
                                        if (null == IPAddressUtil.FindCurrentHost(hosts))
                                        {
                                            List<string> xhosts = new List<string>(1 + hosts.Length);
                                            xhosts.Add(System.Net.Dns.GetHostName());
                                            xhosts.AddRange(hosts);
                                            hosts = xhosts.ToArray();
                                        }
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
                                            try
                                            {
                                                Console.WriteLine("Starting service on {0}...", host);
                                                Console.WriteLine(Exec.Shell("sc \\\\" + host + " start DistributedObjects").Trim());
                                            }
                                            catch (Exception e)
                                            {
                                                Console.WriteLine("Problem starting service on {0}: {1}", host, e.Message);
                                            }
                                        }
                                        ), hosts, threadcount);
                                    Console.WriteLine("Done");
                                }
                                return;

                            case "stopsurrogate":
                                {
                                    string surrogate;
                                    if (args.Length > 1)
                                    {
                                        Console.Error.WriteLine("Parameter not supported to stopsurrogate");
                                        return;
                                    }
                                    else
                                    {
                                        surrogate = Surrogate.MasterHost;
                                    }
                                    try
                                    {
                                        Console.WriteLine("Stopping surrogate on {0}...", surrogate);
                                        Console.WriteLine(Exec.Shell("sc \\\\" + surrogate + " stop DistributedObjects").Trim());
                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine("Problem stopping surrogate on {0}: {1}", surrogate, e.Message);
                                    }

                                    Console.WriteLine("Done");
                                }
                                return;

                            case "startsurrogate":
                                {
                                    string surrogate;
                                    if (args.Length > 1)
                                    {
                                        Console.Error.WriteLine("Parameter not supported to startsurrogate");
                                        return;
                                    }
                                    else
                                    {
                                        surrogate = Surrogate.MasterHost;
                                    }
                                    try
                                    {
                                        Console.WriteLine("Starting surrogate on {0}...", surrogate);
                                        Console.WriteLine(Exec.Shell("sc \\\\" + surrogate + " start DistributedObjects").Trim());
                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine("Problem starting surrogate on {0}: {1}", surrogate, e.Message);
                                    }

                                    Console.WriteLine("Done");
                                }
                                return;

                            case "servicestatussurrogate":
                                {
                                    string surrogate;
                                    if (args.Length > 1)
                                    {
                                        string shost = args[1];
                                        if (-1 != shost.IndexOf(';'))
                                        {
                                            Console.Error.WriteLine("servicestatussurrogate does not support list of hosts");
                                            //SetFailure();
                                            return;
                                        }
                                        if (shost.StartsWith("@"))
                                        {
                                            Console.Error.WriteLine("servicestatussurrogate does not support @hosts.txt");
                                            //SetFailure();
                                            return;
                                        }
                                        else
                                        {
                                            surrogate = Surrogate.LocateMasterHost(Surrogate.NetworkPathForHost(shost));
                                        }
                                    }
                                    else
                                    {
                                        surrogate = Surrogate.MasterHost;
                                    }

                                    string s = Surrogate.GetServiceStatusText(surrogate);
                                    Console.WriteLine(s);
                                }
                                return;

                            case "servicestatus":
                                {
                                    string surrogate;
                                    if (args.Length > 1)
                                    {
                                        string shost = args[1];
                                        if (-1 != shost.IndexOf(';'))
                                        {
                                            Console.Error.WriteLine("servicestatus does not support list of hosts");
                                            //SetFailure();
                                            return;
                                        }
                                        if (shost.StartsWith("@"))
                                        {
                                            Console.Error.WriteLine("servicestatus does not support @hosts.txt");
                                            //SetFailure();
                                            return;
                                        }
                                        else
                                        {
                                            surrogate = shost;
                                        }
                                    }
                                    else
                                    {
                                        surrogate = System.Net.Dns.GetHostName();
                                    }

                                    string s = Surrogate.GetServiceStatusText(surrogate);
                                    Console.WriteLine(s);
                                }
                                return;

                            case "servicestatusall":
                                {
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
                                        dfs dc = dfs.ReadDfsConfig_unlocked(Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\" + dfs.DFSXMLNAME);
                                        hosts = dc.Slaves.SlaveList.Split(';');
                                        // Always include self host if current cluster.
                                        if (null == IPAddressUtil.FindCurrentHost(hosts))
                                        {
                                            List<string> xhosts = new List<string>(1 + hosts.Length);
                                            xhosts.Add(System.Net.Dns.GetHostName());
                                            xhosts.AddRange(hosts);
                                            hosts = xhosts.ToArray();
                                        }
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
                                            Console.WriteLine(Surrogate.GetServiceStatusText(host));
                                        }
                                        ), hosts, threadcount);

                                }
                                return;

                            case "restoresurrogate":
                            case "\u0040format":
                            case "format":
                            format_cmd:
                                // Commands forwarded to aelight...
                                {
                                    try
                                    {
                                        Environment.SetEnvironmentVariable("DSPACE_EXE", "0");
                                    }
                                    catch
                                    {
                                    }
                                    System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(AELight_Dir + @"\AELight.exe", sargs);
                                    psi.UseShellExecute = false;
                                    System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
                                    proc.WaitForExit();
                                    Environment.Exit(proc.ExitCode);
                                    return;
                                }
                            //break;

                        }
                    }
                }

#if NOMASTER

#if DEBUG_cmiller
            {
                string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                if (computer_name == "MAPDCMILLER")
                {
                    System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(AELight_Dir + @"\AELight.exe", sargs);
                    psi.UseShellExecute = false;
                    System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
                    proc.WaitForExit();
                    Environment.Exit(proc.ExitCode); // Not friendly to retrylogmd5
                    return;
                }
            }
#endif

                ex = MasterRunStreamStdIO(sargs);
#if CLIENT_LOG_ALL
                LogOutputToFile("CLIENT_LOG_ALL: returning from generic dspace command, StreamStdIO returned " + ex.ToString());
#endif

#else
            try
            {
                Environment.SetEnvironmentVariable("DSPACE_EXE", "0");
            }
            catch
            {
            }

            System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(AELight_Dir + @"\AELight.exe", sargs);
            psi.UseShellExecute = false;
            System.Diagnostics.Process.Start(psi).WaitForExit();
#endif

#if CLIENT_LOG_ALL
            LogOutputToFile("CLIENT_LOG_ALL: clean return from Main");
#endif

            }
            finally
            {
#if DEBUG
                //System.Threading.Thread.Sleep(1000 * 8);
#endif
                if (retrylogmd5)
                {
                    Log("\r\n[EndExec time=\"" + DateTime.Now + "\"]\r\n");
                    lock (retrylogbuf)
                    {
                        retrylogstop = true;
                    }
                    for (; ; )
                    {
                        if (retrylogthread.Join(1000 * 60))
                        {
                            break;
                        }
                        Console.WriteLine("Waiting for log file synchronization...");
                    }
                }

#if DEBUG
                if (_jobdebug)
                {
                    System.Threading.Mutex _jobdebug_m = new System.Threading.Mutex(false, "distobjlog");
                    try
                    {
                        _jobdebug_m.WaitOne();
                    }
                    catch (System.Threading.AbandonedMutexException)
                    {
                    }
                    try
                    {
                        if (System.IO.File.Exists("jobdebug.txt"))
                        {
                            DateTime _jobdebug_endtime = DateTime.Now;
                            using (System.IO.StreamWriter _jobdebu_sw = new System.IO.StreamWriter("jobdebug.txt", true))
                            {
                                _jobdebu_sw.WriteLine();
                                _jobdebu_sw.WriteLine("--------------------------------------------------------------------------------");
                                _jobdebu_sw.WriteLine();
                                _jobdebu_sw.WriteLine(" - Command:   {0}", _jobdebug_cmdline);
                                _jobdebu_sw.WriteLine(" - Started:   {0}", _jobdebug_starttime);
                                _jobdebu_sw.WriteLine(" - Finished:  {0}", _jobdebug_endtime);
                                _jobdebu_sw.WriteLine(" - Duration:  {0}", _jobdebug_endtime - _jobdebug_starttime);
                                _jobdebu_sw.WriteLine(" - Exit Code: {0}", _jobdebug_exitcode);
                                _jobdebu_sw.WriteLine();
                                foreach (string _jobdebug_ln in _jobdebug_output)
                                {
                                    _jobdebu_sw.Write(_jobdebug_ln);
                                }
                                _jobdebu_sw.WriteLine();
                                _jobdebu_sw.WriteLine("--------------------------------------------------------------------------------");
                                _jobdebu_sw.WriteLine();
                            }
                        }
                    }
                    catch
                    {
                    }
                    finally
                    {
                        _jobdebug_m.ReleaseMutex();
                    }
                }
#endif

            }

            Environment.Exit(ex);

        }


        public static string GetSnowballFilesWildcard(string snowballname)
        {
            return "zsball_" + snowballname + "_*.zsb";
        }

        public static string GetSnowballFoilSampleFilesWildcard(string snowballname)
        {
            return "zfoil_" + snowballname + ".*.zf";
        }


        static void MakeInvincible()
        {
            string fp = AELight_Dir + @"\invincible.dat";
#if DEBUG
            if (System.IO.File.Exists(fp))
            {
                Console.Error.WriteLine("DEBUG:  Warning: invincible processes already exist, overwriting");
            }
#endif
            using (System.IO.StreamWriter sw = System.IO.File.CreateText(fp))
            {
                sw.WriteLine(System.Diagnostics.Process.GetCurrentProcess().Id);
                string EvMDORedir = Environment.GetEnvironmentVariable("MDORedir");
                if (null != EvMDORedir)
                {
                    sw.WriteLine(EvMDORedir);
                }
            }
        }


        static void importdir_recursive(string dir)
        {
            _importdir_recursive_mt(dir, int.MinValue);
        }

        static void importdir_recursive_mt(string dir)
        {
            _importdir_recursive_mt(dir, 0);
        }

        static void _importdir_recursive_mt(string dir, int inc)
        {
            dfs dc = Surrogate.ReadMasterDfsConfig();
            string backupdir = dc.GetMetaBackupLocation();
            _importdir_recursive_mt(dir, inc, backupdir);
        }

        // Note: backupdir can be null for no backup.
        static void _importdir_recursive_mt(string dir, int inc, string backupdir)
        {
            System.IO.DirectoryInfo idir = new System.IO.DirectoryInfo(dir);

            {
                System.IO.FileInfo[] ifiles = idir.GetFiles();
                if (ifiles.Length > 0)
                {
                    int nfilethreads = ifiles.Length;
                    if (nfilethreads > 15)
                    {
                        nfilethreads = 15;
                    }
                    if (inc < 0)
                    {
                        nfilethreads = 1;
                    }
                    //foreach (System.IO.FileInfo fi in ifiles)
                    MySpace.DataMining.Threading.ThreadTools<System.IO.FileInfo>.Parallel(
                        new Action<System.IO.FileInfo>(
                        delegate(System.IO.FileInfo fi)
                        {
                            try
                            {
                                string fn = fi.Name;
                                string reason = "";
                                if (dfs.IsBadFilename(fn, out reason))
                                {
                                    lock (idir)
                                    {
                                        Console.Error.WriteLine("Invalid file name, skipping: {0}. {1}", fn, reason);
                                    }                                    
                                    return;
                                }
                                string newactualfilehost = Surrogate.MasterHost;
                                string newactualfilename = dfs.GenerateZdFileDataNodeName(fn);
                                string myActualFile = Surrogate.NetworkPathForHost(newactualfilehost) + @"\" + newactualfilename;
                                string jobtext = System.IO.File.ReadAllText(fi.FullName);
                                string backupfile = null;
                                if (-1 == jobtext.IndexOf("SourceCode"))
                                {
                                    //Console.Error.WriteLine("File '{0}' is not a valid jobs file, skipping", fi.Name);
                                    throw new Exception("File '" + fi.Name + "' is not a valid jobs file"); // Can't rely on this check, so have user fix it instead.
                                }
                                else
                                {
                                    try
                                    {
                                        //System.IO.File.Copy(fi.FullName, myActualFile);
                                        System.IO.File.WriteAllText(myActualFile, jobtext);
                                        //+++metabackup+++
                                        // Since this doesn't even exist in dfs.xml yet,
                                        // writing to the actual jobs file doesn't need to be transactional.
                                        if (null != backupdir)
                                        {
                                            try
                                            {
                                                backupfile = backupdir + @"\" + newactualfilename;
                                                System.IO.File.WriteAllText(backupfile, jobtext);
                                            }
                                            catch (Exception eb)
                                            {
                                                throw new Exception("Error writing backup: " + eb.Message, eb);
                                            }
                                        }
                                        //---metabackup---
                                        string eout = Exec.Shell(
                                            "DSpace -dfsbind \"" + newactualfilehost + "\" \"" + newactualfilename + "\" \"" + fn + "\" " + DfsFileTypes.JOB
                                            );
                                        lock (idir)
                                        {
                                            Console.Write(eout);
                                            Console.WriteLine("Job '{0}' imported into DFS", fi.Name);
                                        }
                                    }
                                    catch
                                    {
                                        try
                                        {
                                            System.IO.File.Delete(myActualFile);
                                        }
                                        catch
                                        {
                                        }
                                        if (null != backupfile)
                                        {
                                            try
                                            {
                                                System.IO.File.Delete(backupfile);
                                            }
                                            catch
                                            {
                                            }
                                        }
                                        throw;
                                    }
                                }
                            }
                            catch (Exec.ShellException e)
                            {
                                if (e.Message.IndexOf("Output file already exists") != -1)
                                {
                                    lock (idir)
                                    {
                                        Console.Error.WriteLine("File '{0}' already exists in DFS, skipping", fi.Name);
                                    }
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        }), ifiles, nfilethreads);
                }
            }

            {
                System.IO.DirectoryInfo[] idirs = idir.GetDirectories();
                if (idirs.Length > 0)
                {
                    int ndirthreads = 1;
                    if (inc == 0)
                    {
                        ndirthreads = idirs.Length;
                        if (ndirthreads > 4)
                        {
                            ndirthreads = 4;
                        }
                    }
                    //foreach (System.IO.DirectoryInfo di in idirs)
                    MySpace.DataMining.Threading.ThreadTools<System.IO.DirectoryInfo>.Parallel(
                        new Action<System.IO.DirectoryInfo>(
                        delegate(System.IO.DirectoryInfo di)
                        {
                            _importdir_recursive_mt(di.FullName, inc + 1, backupdir);
                        }), idirs, ndirthreads);
                }
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

        static void ProcessUser(string[] args)
        {          
            string thisuser = Environment.UserName;
            string thishost = System.Net.Dns.GetHostName();
            _mdousername = thisuser + "@" + thishost;

            string[] ahosts, ausers, acmds;            
            Surrogate.GetAInfo(out ahosts, out ausers, out acmds);

            /*bool accountOn = false;
            dfs dc = null;
            try
            {
                dc = Surrogate.ReadMasterDfsConfig();
                if (dc.AccountType != null)
                {
                    accountOn = dc.AccountType.On;
                    if (accountOn)
                    {
                        ahosts = dc.AccountType.Hosts.Split(';');
                    }
                }                
            }
            catch
            {
            }*/

            bool isahost = false;
            foreach (string host in ahosts)
            {
                System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(
                    Surrogate.WildcardRegexString(host),
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
                if (rex.IsMatch(thishost))
                {
                    isahost = true;
                    break;
                }
            }

            if (isahost)
            {
                bool isauser = false;
                foreach (string auser in ausers)
                {
                    if (0 == string.Compare(auser, thisuser, StringComparison.OrdinalIgnoreCase))
                    {
                        isauser = true;
                        break;
                    }
                }
              
                /*if (accountOn && !isauser)
                {
                    isauser = Surrogate.InAGroup(thisuser, dc);
                }*/
                if (!isauser)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (string arg in args)
                    {
                        sb.Length = 0;
                        for (int ic = 0; ic < arg.Length; ic++)
                        {
                            if (char.IsLetter(arg[ic]))
                            {
                                sb.Append(arg[ic]);
                            }
                        }
                        string narg = sb.ToString();
                        foreach (string acmd in acmds)
                        {
                            if (0 == string.Compare(acmd, narg, StringComparison.OrdinalIgnoreCase))
                            {
                                Console.Error.WriteLine("Security error");
                                Environment.Exit(88);
                                return;
                            }
                        }
                    }
                }
            }
        }
        /*
        static void ProcessUser(string[] args)
        {
            string thisuser = Environment.UserName;
            string thishost = System.Net.Dns.GetHostName();
            _mdousername = thisuser + "@" + thishost;

            string[] ahosts, ausers, acmds;
            bool isahost = false;
            Surrogate.GetAInfo(out ahosts, out ausers, out acmds);
            foreach (string host in ahosts)
            {
                System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(
                    Surrogate.WildcardRegexString(host),
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
                if (rex.IsMatch(thishost))
                {
                    isahost = true;
                    break;
                }
            }
            if (isahost)
            {
                bool isauser = false;
                foreach (string auser in ausers)
                {
                    if (0 == string.Compare(auser, thisuser, StringComparison.OrdinalIgnoreCase))
                    {
                        isauser = true;
                        break;
                    }
                }
                if (!isauser)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (string arg in args)
                    {
                        sb.Length = 0;
                        for (int ic = 0; ic < arg.Length; ic++)
                        {
                            if (char.IsLetter(arg[ic]))
                            {
                                sb.Append(arg[ic]);
                            }
                        }
                        string narg = sb.ToString();
                        foreach (string acmd in acmds)
                        {
                            if (0 == string.Compare(acmd, narg, StringComparison.OrdinalIgnoreCase))
                            {
                                Console.Error.WriteLine("Security error");
                                Environment.Exit(88);
                                return;
                            }
                        }
                    }
                }
            }
        }
        */
        static int RunStreamStdIO(string onhost, string fullargs, bool async)
        {
            using (System.Net.Sockets.NetworkStream nstm = GetServiceStream(onhost))
            {
                try
                {
                    if (null == nstm)
                    {
#if CLIENT_LOG_ALL
                    LogOutputToFile("CLIENT_LOG_ALL: MasterRun: GetServiceStream returned null");
#endif
                        return 44; // ...
                    }

                    nstm.WriteByte((byte)'$');
                    string opts = "";
                    if (null != Environment.GetEnvironmentVariable("DOSLAVE"))
                    {
                        opts = "\"-DOSLAVE" + Environment.GetEnvironmentVariable("DOSLAVE").Replace("\"", "") + "\" ";
                    }
                    if (async)
                    {
                        opts += "-ASYNC ";
                    }                    
                    XContent.SendXContent(nstm, opts + @"\\" + Environment.UserDomainName + @"\" + mdousername() + ": " + fullargs);
                    if ((int)'+' != nstm.ReadByte())
                    {
                        Console.Error.WriteLine("Error:  service did not report a success; problem executing Qizmt on target (see service log on " + onhost + ")");
                        return 44;
                    }

                    return StreamStdIO(nstm);

                }
                catch (Exception e)
                {
                    if (nstm != null)
                    {
                        Console.Error.WriteLine("Closing connection.  Error: " + e.Message);
                        nstm.Close();
                    }
                    return 44;
                }  
            }
        }

        static int MasterRunStreamStdIO(string sargs)
        {
            return RunStreamStdIO(Surrogate.MasterHost, "AELight.exe " + sargs, false); // Can't call DSpace.exe/Qizmt.exe here or it'll be recursive
        }

        static void RunProxy(string sargs)
        {
            System.Net.Sockets.Socket lsock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, 
                System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            System.Net.IPEndPoint ep = new System.Net.IPEndPoint(System.Net.IPAddress.Any, 55901);

            try
            {
                for (int i = 0; ; i++)
                {
                    try
                    {
                        lsock.Bind(ep);
                        break;
                    }
                    catch
                    {
                        if (i >= 5)
                        {
                            throw;
                        }
                        System.Threading.Thread.Sleep(1000 * 4);
                        continue;
                    }
                }
                lsock.Listen(2);
                {
                    System.Net.Sockets.Socket ssock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                        System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                    System.Net.Sockets.NetworkStream snetstm = null;
                    try
                    {
                        ssock.Connect(System.Net.Dns.GetHostName(), 55900);
                        snetstm = new System.Net.Sockets.NetworkStream(ssock);
                        snetstm.WriteByte((byte)'U');  //start DProcess
                        if (snetstm.ReadByte() != (byte)'+')
                        {
                            throw new Exception("Did not receive a success signal from service.");
                        }
                    }
                    finally
                    {
                        if (snetstm != null)
                        {
                            snetstm.Close();
                            snetstm = null;
                        }
                        ssock.Close();
                        ssock = null;
                    }
                }

                {
                    System.Net.Sockets.Socket csock = lsock.Accept();
                    System.Net.Sockets.NetworkStream cnetstm = null;
                    try
                    {
                        cnetstm = new System.Net.Sockets.NetworkStream(csock);
                        lsock.Close();
                        lsock = null;
                        XContent.SendXContent(cnetstm, sargs);
                        if (cnetstm.ReadByte() != (byte)'+')
                        {
                            throw new Exception("Did not receive a success signal from DProcess.");
                        }
                        StreamStdIO(cnetstm);
                    }
                    finally
                    {
                        if (cnetstm != null)
                        {
                            cnetstm.Close();
                            cnetstm = null;
                        }
                        csock.Close();
                        csock = null;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("RunProxy error: {0}", e.ToString());
            }
            finally
            {
                if (lsock != null)
                {
                    lsock.Close();
                    lsock = null;
                }
            }
        }

        internal static int DeleteAllMatchingFiles(string dir, string pattern, bool showdots)
        {

#if CLIENT_LOG_ALL
            Console.WriteLine("    CLIENT_LOG_ALL: cleaning '" + pattern + "' files");
#endif
            int count = 0;
            try
            {
                foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(dir)).GetFiles(pattern))
                {
                    try
                    {
                        fi.Delete();
                        count++;
                        if (showdots)
                        {
                            Console.Write('.');
                        }
                    }
                    catch (Exception e)
                    {
                        LogOutputToFile("killall DeleteAllMatchingFiles exception: " + e.ToString());
                    }
                }
            }
            catch (Exception e)
            {
                LogOutputToFile("killall DeleteAllMatchingFiles exception: " + e.ToString());
            }

#if CLIENT_LOG_ALL
            Console.WriteLine("    CLIENT_LOG_ALL: cleaned " + count.ToString() + " '" + pattern + "' files");
#endif
            return count;
        }

        internal static int DeleteAllMatchingFiles(string dir, string pattern)
        {
            return DeleteAllMatchingFiles(dir, pattern, false);
        }

        internal static int DeleteOldMatchingFiles(TimeSpan dur, string dir, string pattern, bool showdots)
        {

#if CLIENT_LOG_ALL
            Console.WriteLine("    CLIENT_LOG_ALL: cleaning '" + pattern + "' files");
#endif
            int count = 0;
            try
            {
                DateTime now = DateTime.Now;
                foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(dir)).GetFiles(pattern))
                {
                    try
                    {
                        TimeSpan age = now - fi.LastWriteTime;
                        if (age >= dur)
                        {
                            fi.Delete();
                            count++;
                            if (showdots)
                            {
                                Console.Write('.');
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        LogOutputToFile("killall DeleteOldMatchingFiles(" + dur.ToString() + ") exception: " + e.ToString());
                    }
                }
            }
            catch (Exception e)
            {
                LogOutputToFile("killall DeleteOldMatchingFiles(" + dur.ToString() + ") exception: " + e.ToString());
            }

#if CLIENT_LOG_ALL
            Console.WriteLine("    CLIENT_LOG_ALL: cleaned " + count.ToString() + " '" + pattern + "' files");
#endif
            return count;
        }

        internal static int DeleteOldMatchingFiles(TimeSpan dur, string dir, string pattern)
        {
            return DeleteOldMatchingFiles(dur, dir, pattern, false);
        }


        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            LogOutputToFile("Received Ctrl+C");

            {
                System.Threading.Thread logthread = new System.Threading.Thread(
                    new System.Threading.ThreadStart(delegate()
                    {
                        try
                        {
                            MasterRunStreamStdIO("-@log <Ctrl+C>");
                        }
                        catch
                        {
                        }
                    }));
                logthread.IsBackground = true;
                logthread.Start();
                logthread.Join(1000 * 4); // At most, wait this long.
            }

            Console.Error.WriteLine();
            ConsoleColor oldc = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Error.WriteLine("\r\n    Job aborted prematurely.\r\n    Cluster may be in bad state.\r\n    To restart ENTIRE cluster, issue command:  Qizmt killall");
            Console.ForegroundColor = oldc;
        }


        internal class Exec
        {

            public static string Shell(string line, bool suppresserrors)
            {
                System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo("cmd.exe", @"/C " + line);
                psi.CreateNoWindow = true;
                //psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
                psi.UseShellExecute = false;
                psi.RedirectStandardOutput = true;
                if (!suppresserrors)
                {
                    psi.RedirectStandardError = true;
                }
                string result;
                using (System.Diagnostics.Process process = System.Diagnostics.Process.Start(psi))
                {
                    ShellErrInfo sei = null;
                    System.Threading.Thread errthd = null;
                    if (!suppresserrors)
                    {
                        sei = new ShellErrInfo();
                        sei.reader = process.StandardError;
                        errthd = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(shellerrthreadproc));
                        errthd.Start(sei);
                    }
                    result = process.StandardOutput.ReadToEnd();
                    process.WaitForExit();
                    if (!suppresserrors)
                    {
                        errthd.Join();
                        if (!string.IsNullOrEmpty(sei.err))
                        {
                            sei.err = sei.err.Trim();
                            if (sei.err.Length != 0)
                            {
                                throw new ShellException(line, sei.err);
                            }
                        }
                    }
                }
                return result;
            }

            class ShellErrInfo
            {
                public string err;
                public System.IO.StreamReader reader;
            }

            static void shellerrthreadproc(object obj)
            {
                ShellErrInfo sei = (ShellErrInfo)obj;
                sei.err = sei.reader.ReadToEnd();
            }


            public static string Shell(string line)
            {
                return Shell(line, false);
            }


            public class ShellException : Exception
            {
                public ShellException(string cmd, string msg)
                    : base("Shell(\"" + cmd + "\") error: " + msg)
                {
                }
            }


        }


        static void _FixSciLexDLL()
        {
            if (IntPtr.Size > 4)
            {
                string dll = AELight_Dir + @"\SciLexer.dll";
                string dll64 = AELight_Dir + @"\SciLexer64.dll";
                string dll32 = AELight_Dir + @"\SciLexer32.dll";
                if (System.IO.File.Exists(dll64))
                {
                    if (System.IO.File.Exists(dll32))
                    {
                        try
                        {
                            System.IO.File.Delete(dll);
                        }
                        catch
                        {
                        }
                    }
                    else
                    {
                        System.IO.File.Move(dll, dll32);
                    }
                    System.IO.File.Move(dll64, dll);
                }
            }
        }


        static void editthreadproc(object obj)
        {
            string sargs = (string)obj;
            MasterRunStreamStdIO("-@log " + sargs);
        }


        static string _mdousername;

        static string mdousername()
        {
            return _mdousername;
        }


        public static int StreamStdIO(System.Net.Sockets.NetworkStream nstm)
        {
#if CLIENT_LOG_ALL
            LogOutputToFile("CLIENT_LOG_ALL: StreamStdIO: streaming standard I/O");
#endif

            Console.InputEncoding = Encoding.UTF8;

            byte[] buf = new byte[0x400 * 8];
            int len;
            const int ACTIVEPROC = 0x00000103;
            int result = ACTIVEPROC;
            try
            {
                while (result == ACTIVEPROC)
                {
                    switch (nstm.ReadByte())
                    {
                        case (int)'o': // stdout
                            {
                                string outstr = XContent.ReceiveXString(nstm, buf);
                                ConsoleWriteColorful(outstr);
                                Log(StripColor(outstr));
#if DEBUG
                                if (_jobdebug)
                                {
                                    _jobdebug_output.Add(StripColor(outstr));
                                }
#endif
                            }
                            break;

                        case (int)'e': // stderr
                            string eStr = XContent.ReceiveXString(nstm, buf);
                            Console.Error.Write(eStr);
                            Log(eStr);
#if DEBUG
                            if (_jobdebug)
                            {
                                _jobdebug_output.Add(eStr);
                            }
#endif
                            break;

                        case 'r': // return
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (len < 4)
                            {
                                result = 44;
                            }
                            result = MyBytesToInt(buf);
#if CLIENT_LOG_ALL
                            LogOutputToFile("CLIENT_LOG_ALL: StreamStdIO: received 'r' return value of " + result.ToString());
#endif
                            break;

                        case 'p': // ping
                            nstm.WriteByte((byte)'g'); // pong
                            break;

                        case -1:
                            result = 44;
#if CLIENT_LOG_ALL
                            LogOutputToFile("CLIENT_LOG_ALL: StreamStdIO: ReadByte returned -1");
#endif
                            break;

                        default:
                            //throw new Exception("Unexpected $action from master service: " + Surrogate.MasterHost);
                            throw new Exception("Unexpected $action received");
                    }
                }
            }
            catch (System.IO.IOException ioex)
            {
                if (ioex.InnerException as System.Net.Sockets.SocketException == null)
                {
                    throw;
                }
                LogOutputToFile(ioex.ToString());
                result = 44;
            }
#if DEBUG
            if (_jobdebug)
            {
                _jobdebug_exitcode = result;
            }
#endif
            return result;
        }


        public static void LogOutputToFile(string line)
        {
            try
            {
                lock (typeof(Program))
                {
                    //System.IO.StreamWriter fstm = System.IO.File.AppendText("aelight-errors.txt");
                    System.IO.StreamWriter fstm = System.IO.File.AppendText(AELight_Dir + @"\dspace-errors.txt");
                    string build = "";
                    /*try
                    {
                        build = "(build:" + GetBuildInfo() + ") ";
                    }
                    catch
                    {
                    }*/
                    fstm.WriteLine("[{0}] {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
                    fstm.WriteLine("----------------------------------------------------------------");
                    fstm.Close();
                }
            }
            catch
            {
            }
        }

        private static void _NormalLog(string append)
        {
            using (System.IO.StreamWriter writer = new System.IO.StreamWriter(logFilePath, true))
            {
                writer.Write(append);
                writer.Close();
            }
        }

        private static void Log(string line)
        {
            if (logToFile)
            {
                if (retrylogmd5)
                {
                    lock (retrylogbuf)
                    {
                        retrylogbuf.Append(line);
                    }
                }
                else
                {
                    _NormalLog(line);
                }
            }
        }


        internal static StringBuilder retrylogbuf = null;
        internal static System.Threading.Thread retrylogthread = null;
        internal static bool retrylogstop = false;
        static int _retrylogcharpos = 0;
        const int _maxretrylogsendchars = 0x400 * 8;
        static long _retryloginitpos = -1;

#if DEBUG
        static bool _debugretrylogthrown = false;
#endif

        private static void retrylogthreadproc()
        {

#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
#endif

            for (; ; )
            {
                try
                {
                    using (System.IO.FileStream fs = new System.IO.FileStream(logFilePath,
                        System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                    {
                        _retryloginitpos = fs.Length;
                    }
                }
                catch (System.IO.FileNotFoundException)
                {
                    _retryloginitpos = 0;
                }
                catch
                {
                    System.Threading.Thread.Sleep(1000 * 1);
                    continue;
                }
                break;
            }

            StringBuilder sbbuf = new StringBuilder(0x400);

            for (; ; )
            {

                int retrylogbufLength;
                bool mystop;
                lock (retrylogbuf)
                {
                    retrylogbufLength = retrylogbuf.Length;
                    mystop = retrylogstop;
                }

                if (_retrylogcharpos < retrylogbufLength)
                {
                    try
                    {

                        if (_retrylogcharpos < 0) // Need to rewrite all?
                        {
                            using (System.IO.FileStream fs = new System.IO.FileStream(logFilePath,
                                System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite, System.IO.FileShare.Read))
                            {
                                long fsLength = fs.Length;
                                if (0 == fsLength)
                                {
                                    // File might have been deleted.
                                    _retryloginitpos = 0;
                                }
                                else if (fsLength < _retryloginitpos)
                                {
                                    // File changed some unknown way.
                                    _retryloginitpos = fsLength;
                                }
                                else
                                {
                                    fs.SetLength(_retryloginitpos);
                                }
                            }
                            _retrylogcharpos = 0; // Rewrite all.
                        }

                        sbbuf.Length = 0;
                        string sendstring;
                        lock (retrylogbuf)
                        {
                            int sendchars = retrylogbuf.Length - _retrylogcharpos;
                            if (sendchars > _maxretrylogsendchars)
                            {
                                sendchars = _maxretrylogsendchars;
                            }
                            for (int i = 0; i < sendchars; i++)
                            {
                                sbbuf.Append(retrylogbuf[_retrylogcharpos + i]);
                            }
                            sendstring = sbbuf.ToString();
                        }

#if DEBUG
                        if (!_debugretrylogthrown)
                        {
                            if (-1 != sendstring.IndexOf("kill", StringComparison.OrdinalIgnoreCase))
                            {
                                _debugretrylogthrown = true;
                                System.Threading.Thread.Sleep(1000 * 8);
                                throw new System.IO.IOException("Debug exception for retrylog");
                            }
                        }
#endif
                        _NormalLog(sendstring);
                        _retrylogcharpos += sendstring.Length;
                        continue;

                    }
                    catch
                    {
                        _retrylogcharpos = -1; // Need to rewrite all.
                    }
                }
                else
                {
                    if (mystop)
                    {
                        bool confirmed = false;
                        try
                        {
                            byte[] expectedMD5hash;
                            long len1;
                            lock (retrylogbuf)
                            {
                                {
                                    System.IO.MemoryStream estm = new System.IO.MemoryStream();
                                    System.IO.StreamWriter esw = new System.IO.StreamWriter(estm);
                                    esw.Write(retrylogbuf);
                                    esw.Flush();
                                    estm.Position = 0;
                                    len1 = estm.Length - 0;
                                    System.Security.Cryptography.MD5 md5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                                    expectedMD5hash = md5.ComputeHash(estm);
                                }
                            }
                            byte[] gotMD5hash;
                            long len2;
                            {
                                using (System.IO.FileStream fs = new System.IO.FileStream(logFilePath,
                                    System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                                {
                                    fs.Position = _retryloginitpos;
                                    len2 = fs.Length - _retrylogcharpos;
                                    System.Security.Cryptography.MD5 md5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                                    gotMD5hash = md5.ComputeHash(fs);
                                }
                            }
                            if (expectedMD5hash.Length == gotMD5hash.Length)
                            {
                                for (int i = 0; ; i++)
                                {
                                    if (i == expectedMD5hash.Length)
                                    {
                                        confirmed = true;
                                        break;
                                    }
                                    if (gotMD5hash[i] != expectedMD5hash[i])
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        catch
                        {
                        }
                        if (confirmed)
                        {
                            break;
                        }
                        else
                        {
                            _retrylogcharpos = -1; // Need to rewrite all.
                        }
                    }
                }
                System.Threading.Thread.Sleep(1000 * 1);

            }

        }


        public static void ConsoleWriteColorful(string s)
        {
            ConsoleColor oldc = Console.ForegroundColor;
            int i = 0;
            for (; ; )
            {
                int newi = s.IndexOf('\u0001', i);
                if (-1 == newi)
                {
                    break;
                }
                if (newi != i)
                {
                    Console.Write(s.Substring(i, newi - i));
                }
                i = newi;
                i++;
                switch (s[i])
                {
                    case '0':
                        Console.ForegroundColor = oldc;
                        break;
                    case '1':
                        Console.ForegroundColor = ConsoleColor.Green;
                        break;
                    case '2':
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        break;
                    case '3':
                        Console.ForegroundColor = ConsoleColor.DarkGreen;
                        break;
                    case '4':
                        Console.ForegroundColor = ConsoleColor.Red;
                        break;
                    case '5':
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        break;
                }
                i++;
            }
            if (i != s.Length)
            {
                Console.Write(s.Substring(i));
            }
        }

        private static string StripColor(string s)
        {
            StringBuilder result = new StringBuilder();

            int i = 0;
            for (; ; )
            {
                int newi = s.IndexOf('\u0001', i);
                if (-1 == newi)
                {
                    break;
                }
                if (newi != i)
                {
                    result.Append(s.Substring(i, newi - i));
                }
                i = newi;
                i += 2;
            }
            if (i != s.Length)
            {
                result.Append(s.Substring(i));
            }

            return result.ToString();
        }


        // Big-endian.
        public static Int32 MyBytesToInt(IList<byte> x, int offset)
        {
            return Surrogate.BytesToInt(x, offset);
        }

        public static Int32 MyBytesToInt(IList<byte> x)
        {
            return Surrogate.BytesToInt(x);
        }


        static System.Net.Sockets.NetworkStream GetServiceStream(string servicehost)
        {
            System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            try
            {
                sock.Connect(servicehost, 55900);
            }
            catch
            {
                Console.Error.WriteLine("Error:  unable to connect to service; problem executing Qizmt on target  [Note: ensure the Windows services are running]");
                sock.Close();
                return null;
            }
            return new XNetworkStream(sock, true);
        }

    }


}
