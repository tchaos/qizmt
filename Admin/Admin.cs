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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace MySpace.DataMining.AELight
{
    public class Admin
    {
        enum HostState
        {
            InaccessibleHost,
            PermissionDeniedHost,
            UninstalledHost,
            ParticipatingSurrogate,
            NonParticipatingSurrogate,
            BrokenSurrogate,
            OrphanedWorker,
            OrphanedWorkerNoSurrogateReferred,
            Worker,
            Unknown
        };

        private static string[] ParseHostList(string arg)
        {
            Dictionary<string, int> dtHosts = new Dictionary<string, int>();

            if (arg.StartsWith("@"))
            {
                string filePath = arg.Substring(1);
                try
                {
                    string[] lines = System.IO.File.ReadAllLines(filePath);
                    foreach (string line in lines)
                    {
                        if (!line.StartsWith("#"))
                        {
                            AddKeyValuePair<string, int>(dtHosts, line.Trim().ToUpper(), 1);
                        }
                    }
                }
                catch
                {
                    Console.Error.WriteLine("Error while reading file: {0}", filePath);
                    return null;
                }
            }
            else
            {
                string[] parts = arg.ToUpper().Split(new char[] { ',', ';' });

                foreach (string p in parts)
                {
                    AddKeyValuePair<string, int>(dtHosts, p.Trim().ToUpper(), 1);
                }
            }

            return dtHosts.Keys.ToArray();
        }

        public static void KickUser(string action, string username, string shosts)
        {
            string[] hosts = ParseHostList(shosts);

            if (hosts.Length == 0 || username.Length == 0)
            {
                Console.Error.WriteLine("username or hosts not provided in arguments.");
                return;
            }

            DateTime startTime = DateTime.Now;

            object lk = new object();

            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string host)
            {
                lock (lk)
                {
                    Console.WriteLine("Querying user session: {0}", host);
                }

                string[] rr = null;
                try
                {
                    rr = MySpace.DataMining.DistributedObjects.Exec.Shell(string.Format("query user {0} /server:{1}", username, host)).Split('\n');
                }
                catch
                {              
                    return;
                }

                if (rr.Length > 1)
                {
                    int stateoffset = rr[0].IndexOf("STATE");

                    if (stateoffset > -1)
                    {
                        for (int i = 1; i < rr.Length; i++)
                        {
                            int id = ParseUserSessionID(rr[i], stateoffset);  
                            if (id > -1)
                            {
                                try
                                {
#if DEBUG
                                    host = "DONT-LOGOFF-FOR-DEBUG";
#endif
                                    Console.WriteLine(MySpace.DataMining.DistributedObjects.Exec.Shell(string.Format("logoff {0} /server:{1}", id, host), false));
                                    
                                    lock (lk)
                                    {
                                        Console.WriteLine("Kicked from: host={0}, username={1}", host, username);
                                    }                                    
                                }
                                catch(Exception e)
                                {
                                    lock (lk)
                                    {
                                        Console.WriteLine("Error while logging off: {0}", e.Message);
                                    }                                    
                                }
                            }
                        }
                    }
                }                
            }
            ), hosts, hosts.Length);

            Console.WriteLine();
            Console.WriteLine("{0} request start time: {1}", action, startTime);
            Console.WriteLine("{0} request end time: {1}", action, DateTime.Now);
        }

        public static void KickAllUsers(string action, string[] exceptusers, string shosts)
        {            
            string[] hosts = ParseHostList(shosts);

            if (hosts.Length == 0 || exceptusers.Length == 0)
            {
                Console.Error.WriteLine("exceptUsername or hosts not provided in arguments.");
                return;
            }

            DateTime startTime = DateTime.Now;

            object lk = new object();            
            
            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string host)
            {
                lock (lk)
                {
                    Console.WriteLine("Querying all user sessions: {0}", host);
                }

                string[] rr = null;
                try
                {
                    rr = MySpace.DataMining.DistributedObjects.Exec.Shell(string.Format("query user /server:{0}", host)).Split('\n');
                }
                catch
                {
                    return;
                }

                if (rr.Length > 1)
                {
                    int stateoffset = rr[0].IndexOf("STATE");
                    int sessionnameoffset = rr[0].IndexOf("SESSIONNAME");

                    if (stateoffset > -1 && sessionnameoffset > -1)
                    {
                        for (int i = 1; i < rr.Length; i++)
                        {
                            string username = ParseUsername(rr[i], sessionnameoffset);

                            if (username == null)
                            {
                                continue;
                            }

                            bool kickme = true;
                            foreach (string except in exceptusers)
                            {
                                if (string.Compare(username, except, true) == 0)
                                {
                                    kickme = false;
                                    break;
                                }
                            }

                            if (kickme)
                            {
                                int id = ParseUserSessionID(rr[i], stateoffset);
                                if (id > -1)
                                {
                                    try
                                    {
#if DEBUG
                                        host = "DONT-LOGOFF-FOR-DEBUG";
#endif
                                        Console.WriteLine(MySpace.DataMining.DistributedObjects.Exec.Shell(string.Format("logoff {0} /server:{1}", id, host), false));

                                        lock (lk)
                                        {
                                            Console.WriteLine("Kicked from: host={0}, username={1}", host, username);
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        lock (lk)
                                        {
                                            Console.WriteLine("Error while logging off: {0}", e.Message);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            ), hosts, hosts.Length);
            
            Console.WriteLine();
            Console.WriteLine("{0} request start time: {1}", action, startTime);
            Console.WriteLine("{0} request end time: {1}", action, DateTime.Now);
        }

        private static string ParseUsername(string line, int sessionNameOffset)
        {
            if (sessionNameOffset > -1 && line.Length > sessionNameOffset)
            {
                return line.Substring(0, sessionNameOffset).Trim();
            }
            return null;
        }

        private static int ParseUserSessionID(string line, int stateOffset)
        {
            if (stateOffset > -1 && line.Length > stateOffset)
            {
                string part = line.Substring(0, stateOffset).Trim();
                string sid = "";

                for (int j = part.Length - 1; j >= 0; j--)
                {
                    if (part[j] != ' ')
                    {
                        sid = part[j] + sid;
                    }
                    else
                    {
                        break;
                    }
                }

                try
                {
                    return Int32.Parse(sid);
                }
                catch
                {                   
                }
            }
            return -1;
        }

        public static void CheckClusters(string[] args)
        {
            if (args.Length == 0)
            {
                Console.Error.WriteLine("Host name argument is not provided.");
                return;
            }

            string[] hosts = ParseHostList(args[0]);

            if (hosts == null || hosts.Length == 0)
            {
                Console.Error.WriteLine("Host name is not provided.");
                return;
            }

            string outputformat = "";
            for (int i = 1; i < args.Length; i++)
            {
                string arg = args[i].ToLower();
                switch (arg)
                {
                    case "-c":
                    case "-n":
                        outputformat = arg;
                        break;
                    default:
                        Console.Error.WriteLine("Output format argument is invalid.  Try: -c | -n");
                        return;
                }
            }
            bool verbose = (outputformat.Length == 0);

            DateTime startTime = DateTime.Now;
            Dictionary<string, string> netPaths = new Dictionary<string, string>();
            Dictionary<string, dfs> sConfigs = new Dictionary<string, dfs>();
            Dictionary<string, string> wConfigs = new Dictionary<string, string>();
            Dictionary<string, HostState> hostStates = new Dictionary<string, HostState>();
            Dictionary<HostState, List<string>> ihostStates = new Dictionary<HostState, List<string>>();
            Dictionary<string, List<string>> clusters = new Dictionary<string, List<string>>();
            Dictionary<string, List<string>> orphanedWorkers = new Dictionary<string, List<string>>();
            string dfsName = "\\" + dfs.DFSXMLNAME;
            string slavedatName = "\\slave.dat";

            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string _host)
            {
                string host = _host.Trim().ToUpper();

                if (host.Length == 0)
                {
                    return;
                }

                if (verbose)
                {
                    lock (sConfigs)
                    {
                        Console.WriteLine("Getting info: {0}", host);
                    }
                }

                string netPath = Surrogate.NetworkPathForHost(host);
                string dfsPath = netPath + dfsName;
                string slavedatPath = netPath + slavedatName;
                string guidFileName = Guid.NewGuid().ToString() + ".txt";
                string driveLetter = GetDriveLetter(netPath);
                string rootGuidPath = string.Format(@"\\{0}\{1}$\{2}", host, driveLetter, guidFileName);
                string srvGuildPath = netPath + "\\" + guidFileName;

                lock (sConfigs)
                {
                    netPaths.Add(host, netPath);
                }

                //Check disk
                int phase = 0;
                try
                {
                    System.IO.File.WriteAllText(rootGuidPath, "x");
                    phase = 1;
                    System.IO.File.Delete(rootGuidPath);
                    phase = 2;
                    if (System.IO.Directory.Exists(netPath))
                    {
                        phase = 3;
                        System.IO.File.WriteAllText(srvGuildPath, "x");
                        phase = 4;
                        System.IO.File.Delete(srvGuildPath);
                        phase = 5;
                    }
                }
                catch
                {
                    HostState state = HostState.Unknown;
                    switch (phase)
                    {
                        case 0:
                            state = HostState.InaccessibleHost;
                            break;
                        case 2:
                            state = HostState.UninstalledHost;
                            break;
                        case 1:
                        case 3:
                        case 4:
                            state = HostState.PermissionDeniedHost;
                            break;
                    }
                    lock (sConfigs)
                    {
                        AddKeyValuePairInverse<string, HostState>(hostStates, host, state, ihostStates);
                    }
                    return;
                }

                try
                {
                    if (dfs.DfsConfigExists(dfsPath))
                    {
                        dfs dc = null;

                        using (dfs.LockDfsMutex())
                        {
                            dc = dfs.ReadDfsConfig_unlocked(dfsPath);
                        }

                        lock (sConfigs)
                        {
                            AddKeyValuePair<string, dfs>(sConfigs, host, dc);
                        }
                    }
                    else if (System.IO.File.Exists(slavedatPath))
                    {
                        string[] lines = System.IO.File.ReadAllLines(slavedatPath);
                        string master = "";

                        foreach (string _line in lines)
                        {
                            string line = _line.Trim();
                            int eq = line.IndexOf('=');
                            if (eq > -1)
                            {
                                string key = "";
                                string value = "";
                                key = line.Substring(0, eq);
                                value = line.Substring(eq + 1);

                                if (string.Compare(key, "master", true) == 0)
                                {
                                    master = value.ToUpper();
                                }
                            }
                        }

                        lock (sConfigs)
                        {
                            AddKeyValuePair<string, string>(wConfigs, host, master);
                        }
                    }
                    else
                    {
                        lock (sConfigs)
                        {
                            AddKeyValuePairInverse<string, HostState>(hostStates, host, HostState.UninstalledHost, ihostStates);
                        }
                    }
                }
                catch (Exception e)
                {
                    lock (sConfigs)
                    {
                        Console.Error.WriteLine("Exception: {0}", e.Message);
                        AddKeyValuePairInverse<string, HostState>(hostStates, host, HostState.InaccessibleHost, ihostStates);
                    }
                }

                if (verbose)
                {
                    lock (sConfigs)
                    {
                        Console.WriteLine("Done getting info: {0}", host);
                    }
                }
            }
            ), hosts, hosts.Length);

            //Examine surrogates
            foreach (KeyValuePair<string, dfs> pair in sConfigs)
            {
                bool isParticipatingSurrogate = false;
                bool isBrokenSurrogate = false;
                string surrogate = pair.Key;
                dfs dc = pair.Value;
                string[] workers = dc.Slaves.SlaveList.ToUpper().Split(';');

                foreach (string worker in workers)
                {
                    if (string.Compare(worker, surrogate, true) == 0)
                    {
                        AddKeyValuePairList<string>(clusters, surrogate, surrogate);
                        isParticipatingSurrogate = true;
                        continue;
                    }

                    if (wConfigs.ContainsKey(worker) && string.Compare(surrogate, wConfigs[worker]) == 0)
                    {
                        AddKeyValuePairList<string>(clusters, surrogate, worker);
                        AddKeyValuePairInverse<string, HostState>(hostStates, worker, HostState.Worker, ihostStates);
                    }
                    else
                    {
                        AddKeyValuePairList<string>(orphanedWorkers, surrogate, worker);
                        AddKeyValuePairInverse<string, HostState>(hostStates, worker, HostState.OrphanedWorker, ihostStates);
                        isBrokenSurrogate = true;
                    }
                }

                HostState state = HostState.Unknown;

                if (isBrokenSurrogate)
                {
                    state = HostState.BrokenSurrogate;
                }
                else if (isParticipatingSurrogate)
                {
                    state = HostState.ParticipatingSurrogate;
                }
                else
                {
                    state = HostState.NonParticipatingSurrogate;
                }

                AddKeyValuePairInverse<string, HostState>(hostStates, surrogate, state, ihostStates);
            }

            //Examine workers
            foreach (KeyValuePair<string, string> pair in wConfigs)
            {
                string worker = pair.Key;

                //If we have not come across this worker yet above, yet it has a slave.dat,  but not referred to by any surrogates above.
                if (!hostStates.ContainsKey(worker))
                {
                    AddKeyValuePairInverse<string, HostState>(hostStates, worker, HostState.OrphanedWorkerNoSurrogateReferred, ihostStates);
                }
            }

            //Examine metabackup of participating and non-participating clusters.
            Dictionary<string, List<string>> badMetaBackup = new Dictionary<string, List<string>>();
            {                
                List<string> hs = new List<string>();
                if (ihostStates.ContainsKey(HostState.ParticipatingSurrogate))
                {
                    hs.AddRange(ihostStates[HostState.ParticipatingSurrogate]);
                }
                if (ihostStates.ContainsKey(HostState.NonParticipatingSurrogate))
                {
                    hs.AddRange(ihostStates[HostState.NonParticipatingSurrogate]);
                }
                
                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
               new Action<string>(
               delegate(string host)
               {
                   dfs dc = sConfigs[host];
                   string backupdir = dc.MetaBackup;
                   if (backupdir == null || backupdir.Length == 0)
                   {
                       return;
                   }

                   string backupdfs = backupdir + @"\dfs-" + Surrogate.SafeTextPath(host) + @".xml";
                   if (!dfs.DfsConfigExists(backupdfs))
                   {
                       lock (badMetaBackup)
                       {
                           if (!badMetaBackup.ContainsKey(host))
                           {
                               badMetaBackup.Add(host, new List<string>());
                           }
                           badMetaBackup[host].Add("Missing: " + backupdfs);
                       }
                   }

                   for (int i = 0; i < dc.Files.Count; i++)
                   {
                       dfs.DfsFile df = dc.Files[i];
                       if (string.Compare(df.Type, DfsFileTypes.JOB, true) == 0)
                       {
                           foreach (dfs.DfsFile.FileNode node in df.Nodes)
                           {
                               string backupnode = backupdir + @"\" + node.Name;
                               if (!System.IO.File.Exists(backupnode))
                               {
                                   lock (badMetaBackup)
                                   {
                                       if (!badMetaBackup.ContainsKey(host))
                                       {
                                           badMetaBackup.Add(host, new List<string>());
                                       }
                                       badMetaBackup[host].Add("Missing: " + backupnode);
                                   }
                               }
                           }
                       }
                   }        
               }), hs, hs.Count);
            }            

            //OK clusters
            if (ihostStates.ContainsKey(HostState.ParticipatingSurrogate))
            {
                List<string> hs = ihostStates[HostState.ParticipatingSurrogate];
                hs.Sort();

                foreach (string surrogate in hs)
                {
                    if (badMetaBackup.ContainsKey(surrogate))
                    {
                        continue;
                    }
                    List<string> workers = clusters[surrogate];
                    workers.Sort();

                    if (verbose)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Participating surrogate: {0}", Lookup(netPaths, surrogate));
                        Console.WriteLine("Workers:");
                        foreach (string worker in workers)
                        {
                            Console.WriteLine(Lookup(netPaths, worker));
                        }
                    }
                    else if (outputformat == "-c")
                    {
                        string sworkers = "";
                        for (int i = 0; i < workers.Count; i++)
                        {
                            if (i != 0)
                            {
                                sworkers += ",";
                            }
                            sworkers += workers[i];
                        }
                        Console.WriteLine(sworkers);
                    }
                    else if (outputformat == "-n")
                    {
                        Console.WriteLine("#----------------------");
                        foreach (string worker in workers)
                        {
                            Console.WriteLine(worker);
                        }
                    }
                }
            }
            
            ConsoleColor oldColor = Console.ForegroundColor;

            if (ihostStates.ContainsKey(HostState.NonParticipatingSurrogate))
            {
                List<string> hs = ihostStates[HostState.NonParticipatingSurrogate];

                foreach (string host in hs)
                {
                    if (badMetaBackup.ContainsKey(host))
                    {
                        continue;
                    }
                    List<string> workers = clusters[host];
                    workers.Sort();

                    if (verbose)
                    {
                        Console.WriteLine();
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Non-participating surrogate: {0}", Lookup(netPaths, host));
                        Console.ForegroundColor = oldColor;
                        Console.WriteLine("Workers:");
                        foreach (string worker in workers)
                        {
                            Console.WriteLine(Lookup(netPaths, worker));
                        }
                    }
                    else if (outputformat == "-c")
                    {
                        Console.WriteLine("Non-participating surrogate: {0}", host);
                        string sworkers = "";
                        for (int i = 0; i < workers.Count; i++)
                        {
                            if (i != 0)
                            {
                                sworkers += ",";
                            }
                            sworkers += workers[i];
                        }
                        Console.WriteLine(sworkers);
                    }
                    else if (outputformat == "-n")
                    {
                        Console.WriteLine("#----------------------");
                        Console.WriteLine("Non-participating surrogate: {0}", host);
                        foreach (string worker in workers)
                        {
                            Console.WriteLine(worker);
                        }
                    }
                }
            }

            if (!verbose)
            {
                return;
            }

            Console.ForegroundColor = ConsoleColor.Red;

            if (badMetaBackup.Count > 0)
            {
                List<KeyValuePair<string, List<string>>> sbadmetabackup = new List<KeyValuePair<string, List<string>>>(badMetaBackup);
                sbadmetabackup.Sort(CompareString);

                foreach (KeyValuePair<string, List<string>> pair in sbadmetabackup)
                {
                    string surrogate = pair.Key;
                    List<string> reasons = pair.Value;

                    Console.WriteLine();
                    Console.WriteLine("Bad meta backup surrogate: {0}", Lookup(netPaths, surrogate));
                    Console.WriteLine("Reasons:");

                    foreach (string reason in reasons)
                    {
                        Console.WriteLine(reason);
                    }
                }
            }

            if (ihostStates.ContainsKey(HostState.InaccessibleHost))
            {
                List<string> hs = ihostStates[HostState.InaccessibleHost];
                hs.Sort();

                foreach (string host in hs)
                {
                    Console.WriteLine();
                    Console.WriteLine("Inaccessible host: {0}", Lookup(netPaths, host));
                }
            }

            if (ihostStates.ContainsKey(HostState.PermissionDeniedHost))
            {
                List<string> hs = ihostStates[HostState.PermissionDeniedHost];
                hs.Sort();

                foreach (string host in hs)
                {
                    Console.WriteLine();
                    Console.WriteLine("Permission denied host: {0}", Lookup(netPaths, host));
                }
            }

            if (ihostStates.ContainsKey(HostState.UninstalledHost))
            {
                List<string> hs = ihostStates[HostState.UninstalledHost];
                hs.Sort();

                foreach (string host in hs)
                {
                    Console.WriteLine();
                    Console.WriteLine("Uninstalled host: {0}", Lookup(netPaths, host));
                }
            }

            //Orphaned workers that are referred to by a cluster.
            if (orphanedWorkers.Count > 0)
            {
                List<KeyValuePair<string, List<string>>> sOrphanedWorkers = new List<KeyValuePair<string, List<string>>>(orphanedWorkers);
                sOrphanedWorkers.Sort(CompareString);

                foreach (KeyValuePair<string, List<string>> pair in sOrphanedWorkers)
                {
                    string surrogate = pair.Key;
                    List<string> os = pair.Value;
                    os.Sort();

                    Console.WriteLine();
                    Console.WriteLine("Broken surrogate: {0}", Lookup(netPaths, surrogate));
                    Console.WriteLine("Bad workers:");

                    foreach (string o in os)
                    {
                        Console.WriteLine(Lookup(netPaths, o));
                    }
                }
            }

            if (ihostStates.ContainsKey(HostState.OrphanedWorkerNoSurrogateReferred))
            {
                List<string> hs = ihostStates[HostState.OrphanedWorkerNoSurrogateReferred];
                hs.Sort();

                foreach (string host in hs)
                {
                    Console.WriteLine();
                    Console.WriteLine("Orphaned worker: {0}", Lookup(netPaths, host));
                }
            }

            Console.ForegroundColor = oldColor;

            Console.WriteLine();
            Console.WriteLine("clustercheck request start time: {0}", startTime);
            Console.WriteLine("clustercheck request end time: {0}", DateTime.Now);
        }

        private static string GetDriveLetter(string netPath)
        {
            int dollar = netPath.IndexOf('$');
            string driveLetter = "D";
            if (dollar > -1)
            {
                driveLetter = netPath.Substring(dollar - 1, 1);
            }
            return driveLetter;
        }

        private static string Lookup(Dictionary<string, string> dt, string key)
        {
            if (dt.ContainsKey(key))
            {
                return dt[key];
            }
            else
            {
                string path = Surrogate.NetworkPathForHost(key.ToUpper());
                dt.Add(key.ToUpper(), path);
                return path;
            }
        }

        private static int CompareString(KeyValuePair<string, List<string>> firstPair, KeyValuePair<string, List<string>> nextPair)
        {
            return firstPair.Key.CompareTo(nextPair.Key);
        }

        private static void AddKeyValuePair<K, V>(Dictionary<K, V> dt, K key, V value)
        {
            if (!dt.ContainsKey(key))
            {
                dt.Add(key, value);
            }
        }

        private static void AddKeyValuePairInverse<K, V>(Dictionary<K, V> dt, K key, V value, Dictionary<V, List<K>> idt)
        {
            if (!dt.ContainsKey(key))
            {
                dt.Add(key, value);
            }

            if (!idt.ContainsKey(value))
            {
                idt.Add(value, new List<K>());
            }

            idt[value].Add(key);
        }

        private static void AddKeyValuePairList<K>(Dictionary<K, List<string>> dt, K key, string value)
        {
            if (!dt.ContainsKey(key))
            {
                dt.Add(key, new List<string>());
            }

            dt[key].Add(value);
        }

        private static string GenFilename()
        {
            return "spTest_" + Guid.NewGuid() + ".txt";
        }

        private static byte[] GenBytes()
        {
            byte[] buf = new byte[1024];
            Random rnd = new Random();

            for (int i = 0; i < buf.Length; i++)
            {
                buf[i] = (byte)rnd.Next(1, 256);
            }

            return buf;
        }

        private static void GenFile(string filepath, ulong filesize)
        {
            ulong bytesWritten = 0;
            byte[] buf = GenBytes();

            using (BinaryWriter writer = new BinaryWriter(File.Create(filepath)))
            {
                while (bytesWritten < filesize)
                {
                    writer.Write(buf);
                    bytesWritten += (ulong)buf.Length;
                }

                writer.Close();
            }
        }

        public static void CompareSpeed(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Arguments invalid for compspeed.  Need 2 machine names");
                return;
            }

            string host1 = args[0].ToUpper();
            string host2 = args[1].ToUpper();

            if (host1 == host2)
            {
                Console.Error.WriteLine("Arguments invalid for compspeed.  Machine names are the same.");
                return;
            }

            ulong filesize = 10 * 1024 * 1024;

            if (args.Length == 3)
            {
                filesize = (ulong)AELight.ParseLongCapacity(args[2]);
            }            

            if (filesize < 1024 * 1024)
            {
                Console.Error.WriteLine("Filesize must be bigger than 1 MB");               
                return;
            }

            int iterations = 10;
            double margin = 10;
            string thisHost = System.Net.Dns.GetHostName();
            string filename = GenFilename();
            string driveLetter = GetDriveLetter(Surrogate.NetworkPathForHost(thisHost));
            string filePath = string.Format(@"\\{0}\{1}$\{2}", thisHost, driveLetter, filename);     
            string netPath1 = Surrogate.NetworkPathForHost(host1);
            string netPath2 = Surrogate.NetworkPathForHost(host2);
            string[] files1 = new string[iterations];
            string[] files2 = new string[iterations];

            for (int i = 0; i < files1.Length; i++)
            {
                files1[i] = string.Format(@"\\{0}\{1}$\{2}.{3}", host1, driveLetter, filename, i);
            }

            for (int i = 0; i < files2.Length; i++)
            {
                files2[i] = string.Format(@"\\{0}\{1}$\{2}.{3}", host2, driveLetter, filename, i);
            }

            try
            {
                GenFile(filePath, filesize);

                Console.WriteLine("Testing: {0}", host1);

                DateTime startTime = DateTime.Now;
                for (int i = 0; i < files1.Length; i++)
                {                   
                    System.IO.File.Copy(filePath, files1[i]);
                }
                double span1 = (DateTime.Now - startTime).TotalSeconds / (double)iterations;

                Console.WriteLine("Testing completed: {0}", host1);

                Console.WriteLine("Testing: {0}", host2);

                startTime = DateTime.Now;
                for (int i = 0; i < files2.Length; i++)
                {                   
                    System.IO.File.Copy(filePath, files2[i]);
                }
                double span2 = (DateTime.Now - startTime).TotalSeconds / (double)iterations;

                Console.WriteLine("Testing completed: {0}", host2);

                double speed1 = ((double)filesize / (1024d * 1024d)) / span1;
                double speed2 = ((double)filesize / (1024d * 1024d)) / span2;

                Console.WriteLine();
                Console.WriteLine("Network speed: {0}: {1} MB/s", host1, speed1.ToString("N2"));
                Console.WriteLine("Network speed: {0}: {1} MB/s", host2, speed2.ToString("N2"));

                double max, min;
                string fasterHost;
                if (speed1 > speed2)
                {
                    max = speed1;
                    min = speed2;
                    fasterHost = host1;
                }
                else
                {
                    max = speed2;
                    min = speed1;
                    fasterHost = host2;
                }

                double diff = ((max - min) / min) * 100;

                if (diff > margin)
                {
                    Console.WriteLine("{0} is faster by {1}%", fasterHost, diff.ToString("N2"));
                }
                else
                {
                    Console.WriteLine("Speed diff is less than {0}% margin.", margin);
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Exception: {0}", e.Message);
            }

            //Clean up
             if (System.IO.File.Exists(filePath))
             {
                 try
                 {
                     System.IO.File.Delete(filePath);
                 }
                 catch
                 {
                 }
             }

            for (int i = 0; i < files1.Length; i++)
            {
                if(System.IO.File.Exists(files1[i]))
                {
                    try
                    {
                        System.IO.File.Delete(files1[i]);
                    }
                    catch
                    {
                    }
                }
            }

            for (int i = 0; i < files2.Length; i++)
            {
                if(System.IO.File.Exists(files2[i]))
                {
                    try
                    {
                        System.IO.File.Delete(files2[i]);
                    }
                    catch
                    {
                    }
                }
            }
        }

        public class Recovery
        {
            private static Dictionary<string, dfs.DfsFile> dfsfiles = new Dictionary<string, dfs.DfsFile>();

            public static void RecoverDfsXml(string[] args)
            {
                if (args.Length == 0)
                {
                    Console.Error.WriteLine("Host name argument is not provided.");
                    return;
                }

                string[] hosts = ParseHostList(args[0]);

                if (hosts == null || hosts.Length == 0)
                {
                    Console.Error.WriteLine("Host name argument is not valid.");
                    return;
                }

                List<dfs.DfsFile.FileNode> jobNodes = new List<dfs.DfsFile.FileNode>();
                Dictionary<string, Dictionary<int, xFileNode>> idataNodes = new Dictionary<string, Dictionary<int, xFileNode>>();
                Dictionary<string, xFileNode> dataNodes = new Dictionary<string, xFileNode>();

                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string _host)
                {
                    string host = _host.Trim().ToUpper();
 
                    if (host.Length == 0)
                    {
                        return;
                    }
                    Console.WriteLine();
                    Console.WriteLine("Getting files info from: {0}", host);

                    string netpath = Surrogate.NetworkPathForHost(host);
                    FileInfo[] files = (new DirectoryInfo(netpath)).GetFiles("zd.*.zd");

                    foreach (FileInfo file in files)
                    {
                        try
                        {
                            SourceCode code = SourceCode.Load(file.FullName);
                            dfs.DfsFile.FileNode node = new dfs.DfsFile.FileNode();
                            node.Host = host;
                            node.Name = file.Name;
                            node.Position = 0;
                            node.Length = file.Length;
                            lock (jobNodes)
                            {
                                jobNodes.Add(node);
                            }                            
                        }
                        catch
                        {
                            //Not a job file.
                            //zd.0.xxx.yyy.zd                     
                            Regex rg = new Regex(@"^zd\.[0-9]+\.[^\.]+\.[^\.]+\.zd$");

                            if (rg.IsMatch(file.Name))
                            {
                                string[] parts = file.Name.Split('.');
                                string key = parts[2] + "." + parts[3];

                                lock (jobNodes)
                                {
                                    if (!idataNodes.ContainsKey(key))
                                    {
                                        idataNodes.Add(key, new Dictionary<int, xFileNode>());
                                    }

                                    Dictionary<int, xFileNode> nodes = idataNodes[key];
                                    int index = Int32.Parse(parts[1]);

                                    if (!nodes.ContainsKey(index))
                                    {
                                        xFileNode node = CreateFileNode(file, host);
                                        if (node != null)
                                        {
                                            nodes.Add(index, node);
                                        }
                                    }
                                    else
                                    {
                                        nodes[index].Host += ";" + host;
                                    }
                                }                                
                            }
                            else
                            {
                                //zd.xxx.yyy.zd 
                                rg = new Regex(@"^zd\.[^\.]+\.[^\.]+\.zd$");

                                if (rg.IsMatch(file.Name))
                                {
                                    lock (jobNodes)
                                    {
                                        if (!dataNodes.ContainsKey(file.Name))
                                        {
                                            xFileNode node = CreateFileNode(file, host);
                                            if (node != null)
                                            {
                                                dataNodes.Add(file.Name, node);
                                            }
                                        }
                                        else
                                        {
                                            dataNodes[file.Name].Host += ";" + host;
                                        }
                                    }                                    
                                }
                            }
                        }
                    }
                    Console.WriteLine();
                    Console.WriteLine("Completed getting files info from: {0}", host);
                }
                ), hosts, hosts.Length);

                Console.WriteLine();
                Console.WriteLine("Processing job files...");
                ProcessJobNodes(jobNodes);
                Console.WriteLine();
                Console.WriteLine("Processing zd.#. files...");
                ProcessIDataNodes(idataNodes);
                Console.WriteLine();
                Console.WriteLine("Processing zd files...");
                ProcessDataNodes(dataNodes);

                {
                    dfs dc = new dfs();
                    dc.InitNew();

                    string sslavelist = "";
                    {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < hosts.Length; i++)
                        {
                            if (sb.Length != 0)
                            {
                                sb.Append(';');
                            }
                            sb.Append(hosts[i].Trim());
                        }
                        sslavelist = sb.ToString();
                    }
                    dc.Slaves.SlaveList = sslavelist;

                    dc.Blocks = new dfs.ConfigBlocks();                   
                    dc.Blocks.TotalCount = AELight.NearestPrimeGE(hosts.Length * Surrogate.NumberOfProcessors);
                    dc.Blocks.SortedTotalCount = hosts.Length * Surrogate.NumberOfProcessors;
                    dc.slave.CompressDfsChunks = 0;                
                    dc.slave.CompressZMapBlocks = 0;
                    dc.Files = new List<dfs.DfsFile>(dfsfiles.Values);

                    System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(dfs));
                    string xmlpath = Surrogate.NetworkPathForHost(System.Net.Dns.GetHostName()) + @"\dfs_" + Guid.NewGuid().ToString() + ".xml";
                    using (System.IO.StreamWriter sw = System.IO.File.CreateText(xmlpath))
                    {
                        xs.Serialize(sw, dc);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Recovered DFS xml at: {0}", xmlpath);
                }
            }

            private static void ProcessJobNodes(List<dfs.DfsFile.FileNode> jobNodes)
            {
                foreach (dfs.DfsFile.FileNode node in jobNodes)
                {                    
                    dfs.DfsFile dfsfile = new dfs.DfsFile();
                    dfsfile.Nodes = new List<dfs.DfsFile.FileNode>() { node };
                    dfsfile.Size = node.Length;
                    dfsfile.Type = DfsFileTypes.JOB;
                    string[] parts = node.Name.Split('.');
                    dfsfile.Name = parts[1];
                    AddDfsFile(dfsfile);
                }
            }

            private static void AddDfsFile(dfs.DfsFile dfsfile)
            {
                if (dfsfiles.ContainsKey(dfsfile.Name))
                {
                    dfsfile.Name += "_" + Guid.NewGuid().ToString();
                }
                dfsfiles.Add(dfsfile.Name, dfsfile);
            }

            private static void ProcessDataNodes(Dictionary<string, xFileNode> dataNodes)
            {
                List<xFileNode> list = new List<xFileNode>(dataNodes.Values);
                list.Sort(delegate(xFileNode x, xFileNode y)
                {
                    return -(x.Position.CompareTo(y.Position));
                });

                Dictionary<long, List<xFileNode>> candidates = new Dictionary<long, List<xFileNode>>();

                foreach (xFileNode n in list)
                {
                    //End position
                    long key = n.Position + n.Length; 

                    if (!candidates.ContainsKey(key))
                    {
                        candidates.Add(key, new List<xFileNode>(){n});
                    }
                    else
                    {
                        candidates[key].Add(n);
                    }
                }

                Dictionary<string, int> done = new Dictionary<string, int>();

                foreach (xFileNode n in list)
                {
                    if (done.ContainsKey(n.Name))
                    {
                        continue;
                    }                                       

                    if (n.Position == 0)
                    {
                        done.Add(n.Name, 0);
                        dfs.DfsFile dfsfile = new dfs.DfsFile();
                        dfsfile.Nodes = new List<dfs.DfsFile.FileNode>() { n.ToDfsFileNode() };
                        dfsfile.Size = n.Length;
                        string[] parts = n.Name.Split('.');
                        dfsfile.Name = parts[1];
                        AddDfsFile(dfsfile);
                    }
                    else
                    {
                        List<xFileNode> prevNodes = new List<xFileNode>();
                        prevNodes.Add(n);
                        xFileNode prev = n;

                        for (; ; )
                        {
                            prev = GetPrevFileNode(prev, candidates);

                            if (prev == null)
                            {
                                break;
                            }

                            prevNodes.Add(prev);

                            if (prev.Position == 0)
                            {
                                break;
                            }
                        }

                        dfs.DfsFile dfsfile = new dfs.DfsFile();
                        string[] parts = n.Name.Split('.');
                        dfsfile.Name = parts[1];                    
                        dfsfile.Nodes = new List<dfs.DfsFile.FileNode>();
                        for (int i = prevNodes.Count - 1; i >= 0; i--)
                        {
                            done.Add(prevNodes[i].Name, 0);
                            dfsfile.Nodes.Add(prevNodes[i].ToDfsFileNode());
                            dfsfile.Size += prevNodes[i].Length;
                        }
                        AddDfsFile(dfsfile);
                    }                    
                }
            }

            private static xFileNode GetPrevFileNode(xFileNode node, Dictionary<long, List<xFileNode>> candidates)
            {
                long key = node.Position;

                if (!candidates.ContainsKey(key))
                {
                    return null;
                }

                List<xFileNode> nodes = candidates[key];

                if (nodes.Count == 0)
                {
                    return null;
                }
                else if (nodes.Count == 1)
                {
                    return nodes[0];
                }
                else
                {
                    int index = 0;
                    double span = 0;
                    double min = 0;

                    for (int i = 0; i < nodes.Count; i++)
                    {
                        xFileNode n = nodes[i];
                        span = (node.LastMod - n.LastMod).TotalMilliseconds;

                        if (i == 0)
                        {
                            min = span;
                        }
                        else
                        {
                            if (span < min)
                            {
                                index = i;
                                min = span;
                            }                        
                        }
                    }
                    xFileNode prev = nodes[index];
                    nodes.RemoveAt(index);
                    return prev;
                }
            }

            private static void ProcessIDataNodes(Dictionary<string, Dictionary<int, xFileNode>> idataNodes)
            {
                List<FileNodeSeq> seqs = new List<FileNodeSeq>();
                Dictionary<string, List<FileNodeSeq>> candidates = new Dictionary<string, List<FileNodeSeq>>();

                foreach (Dictionary<int, xFileNode> ls in idataNodes.Values)
                {
                    FileNodeSeq seq = new FileNodeSeq(ls);
                    seqs.Add(seq);

                    string key = string.Format("{0}.{1}", seq.Name, seq.EndPos);
                    if (!candidates.ContainsKey(key))
                    {
                        candidates.Add(key, new List<FileNodeSeq>() { seq });
                    }
                    else
                    {
                        candidates[key].Add(seq);
                    }
                }

                seqs.Sort(delegate(FileNodeSeq x, FileNodeSeq y)
                {
                    return -(x.StartPos.CompareTo(y.StartPos));
                });

                Dictionary<string, int> done = new Dictionary<string, int>();

                foreach (FileNodeSeq seq in seqs)
                {
                    if (done.ContainsKey(seq.ID))
                    {
                        continue;
                    }                                        

                    if (seq.StartPos == 0)
                    {
                        done.Add(seq.ID, 0);
                        dfs.DfsFile dfsfile = new dfs.DfsFile();
                        dfsfile.Name = seq.Name;
                        dfsfile.Size = seq.EndPos;
                        dfsfile.Nodes = new List<dfs.DfsFile.FileNode>();
                        foreach (xFileNode n in seq.Nodes)
                        {
                            dfsfile.Nodes.Add(n.ToDfsFileNode());
                        }
                        AddDfsFile(dfsfile);
                    }
                    else
                    {
                        List<FileNodeSeq> prevSeqs = new List<FileNodeSeq>();
                        prevSeqs.Add(seq);
                        FileNodeSeq prev = seq;

                        for (; ; )
                        {
                            prev = GetPrevSeq(prev, candidates);
                            if (prev == null)
                            {
                                break;
                            }
                            prevSeqs.Add(prev);

                            if (prev.StartPos == 0)
                            {
                                break;
                            }
                        }
                       
                        dfs.DfsFile dfsfile = new dfs.DfsFile();
                        dfsfile.Name = seq.Name;
                        dfsfile.Nodes = new List<dfs.DfsFile.FileNode>();
                        for (int i = prevSeqs.Count - 1; i >= 0; i--)
                        {
                            done.Add(prevSeqs[i].ID, 0);
                            foreach (xFileNode n in prevSeqs[i].Nodes)
                            {
                                dfsfile.Nodes.Add(n.ToDfsFileNode());                                
                                dfsfile.Size += n.Length;
                            }
                        }
                        AddDfsFile(dfsfile);
                    }
                }
            }

            private static FileNodeSeq GetPrevSeq(FileNodeSeq seq, Dictionary<string, List<FileNodeSeq>> candidates)
            {
                string key = string.Format("{0}.{1}", seq.Name, seq.StartPos);

                if (!candidates.ContainsKey(key))
                {
                    return null;
                }

                List<FileNodeSeq> seqs = candidates[key];

                if (seqs.Count == 0)
                {
                    return null;
                }
                else if (seqs.Count == 1)
                {
                    return seqs[0];
                }
                else
                {
                    double ts = 0;
                    double min = 0;
                    int index = 0;
                    for (int i = 0; i < seqs.Count; i++)
                    {
                        FileNodeSeq p = seqs[i];
                        ts = (seq.LastMod - p.LastMod).TotalMilliseconds;

                        if (i == 0)
                        {
                            min = ts;
                        }
                        else
                        {
                            if (ts < min)
                            {
                                min = ts;
                                index = i;
                            }
                        }
                    }
                    FileNodeSeq prev =  seqs[index];
                    seqs.RemoveAt(index);
                    return prev;
                }
            }

            private static xFileNode CreateFileNode(FileInfo fileinfo, string host)
            {
                if (fileinfo.Length <= 12)
                {
                    return null;
                }

                System.IO.FileStream fs = null;
                try
                {
                    fs = new FileStream(fileinfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read);
                    byte[] buf = new byte[12];
                    StreamReadExact(fs, buf, 12);
                    long position = MySpace.DataMining.DistributedObjects.Entry.BytesToLong(buf, 4);

                    xFileNode node = new xFileNode();
                    node.Host = host;
                    node.Length = fileinfo.Length - 12;
                    node.Position = position;
                    node.Name = fileinfo.Name;
                    node.LastMod = fileinfo.LastWriteTimeUtc;
                    return node;
                }
                catch
                {
                    return null;
                }
                finally
                {
                    if (fs != null)
                    {
                        fs.Close();
                    }
                    fs = null;
                }
            }

            private static int StreamReadLoop(System.IO.Stream stm, byte[] buf, int len)
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

            private static void StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
            {
                if (len != StreamReadLoop(stm, buf, len))
                {
                    throw new System.IO.IOException("Unable to read from stream");
                }
            }

            private class xFileNode : dfs.DfsFile.FileNode
            {
                public DateTime LastMod;

                public dfs.DfsFile.FileNode ToDfsFileNode()
                {
                    dfs.DfsFile.FileNode node = new dfs.DfsFile.FileNode();
                    node.Host = this.Host;
                    node.Length = this.Length;
                    node.Name = this.Name;
                    node.Position = this.Position;
                    return node;
                }
            }

            private class FileNodeSeq
            {
                public string ID;
                public long StartPos;
                public long EndPos;
                public string Name;
                public DateTime LastMod;
                public xFileNode[] Nodes;

                public FileNodeSeq(Dictionary<int, xFileNode> dt)
                {
                    IEnumerable<KeyValuePair<int, xFileNode>> odt = dt.OrderBy(pair => pair.Key);
                    Nodes = new xFileNode[odt.Count()];

                    int i = 0;
                    foreach (KeyValuePair<int, xFileNode> pair in odt)
                    {
                        Nodes[i++] = pair.Value;
                    }

                    xFileNode lastnode = Nodes[Nodes.Length - 1];
                    StartPos = Nodes[0].Position;
                    EndPos = lastnode.Position + lastnode.Length;
                    string[] parts = lastnode.Name.Split('.');
                    ID = parts[3];
                    Name = parts[2];
                    LastMod = lastnode.LastMod;
                }
            }
        }
    }
}
