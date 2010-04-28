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

#if DEBUG
#define DEBUG_REPL
//#define DEBUGprintreplication
//#define DEBUGonemachinereplication
#else
#define DEBUG_REPL
#endif

// Replication mutex not needed anymore due to file name passed in,
// and if null file name, it should be in admin cmd lock.
//#define REPLICATION_MUTEX


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {

        static bool ReplicationDebugVerbose = false;


        struct ReplFileNode
        {
            internal dfs.DfsFile.FileNode node;
            internal dfs.DfsFile ownerfile; // File the node belongs to.
        }

        struct ReplInfo
        {
            internal string host;
            internal List<ReplFileNode> pullfiles;
        }


        class RAQueue<T>
        {
            List<T> list;


            public RAQueue()
            {
                list = new List<T>();
            }

            public RAQueue(int capacity)
            {
                list = new List<T>(capacity);
            }


            public T[] ToArray()
            {
                return list.ToArray();
            }


            public int Count
            {
                get
                {
                    return list.Count;
                }
            }


            public T this[int index]
            {
                get
                {
                    return list[index];
                }

                set
                {
                    list[index] = value;
                }
            }


            public void Enqueue(T item)
            {
                list.Add(item);
            }


            public T DequeueAt(int index)
            {
                T item = list[index];
                list.RemoveAt(index);
                return item;
            }

            public T Dequeue()
            {
                return DequeueAt(0);
            }

        }


        // file is a dfs file name without dfs://
        public static bool ReplicationPhase(bool verbose, int NumberOfCores, IList<string> TheseServersOnly, IList<string> files)
        {

#if DEBUG_REPL
            if (null != files)
            {
                foreach (string file in files)
                {
                    if (string.IsNullOrEmpty(file))
                    {
                        throw new Exception("DEBUG:  ReplicationPhase: DFS file is null or empty");
                    }
                    if (file.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        throw new Exception("DEBUG:  ReplicationPhase: DFS file '" + file + "' starts with dfs://");
                    }
                }
            }
#endif
            if (ReplicationDebugVerbose)
            {
                Console.WriteLine("[rv]");
            }
            bool anywork = false; // Did it replicate anything?
            Random rnd = new Random();
            int numwarnings = 0;
#if REPLICATION_MUTEX
            System.Threading.Mutex repmutex = new System.Threading.Mutex(false, "DOreplication");
            try
            {
                repmutex.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
#endif
            {
                dfs dc = LoadDfsConfig();
                if (dc.Replication < 2)
                {
                    return false;
                }
                int needsayreplicating = verbose ? 1 : 0;
                int replication = dc.Replication;
                string[] slaves = null;
                if (null == TheseServersOnly)
                {
                    slaves = dc.Slaves.SlaveList.Split(';');
                }
                else
                {
                    slaves = TheseServersOnly.ToArray();
                }
                if (NumberOfCores < 1)
                {
                    NumberOfCores = Surrogate.NumberOfProcessors * slaves.Length;
                }
                int NumberOfCoresPerMachine = NumberOfCores / slaves.Length;
                int NumberOfCoresRemainder = NumberOfCores % slaves.Length;
                const char CORE_COMPLETED_CHAR = 'x';
                string MachineCompletedString = new string(CORE_COMPLETED_CHAR, NumberOfCoresPerMachine);
                if (replication > slaves.Length)
                {
                    replication = slaves.Length;
                    /*if (!QuietMode)
                    {
                        ConsoleFlush();
                        Console.Error.WriteLine("Warning: Replication is higher than number of machines");
                        Console.Error.Flush();
                    }*/
                }

                RAQueue<ReplInfo> infos = new RAQueue<ReplInfo>(slaves.Length);
                for (int i = 0; i < slaves.Length; i++)
                {
                    ReplInfo ri;
                    ri.host = slaves[i];
                    ri.pullfiles = new List<ReplFileNode>();
                    infos.Enqueue(ri);
                }
                {
                    // Randomize machine list for random starting point, etc.
                    Random infosrnd = new Random(unchecked(infos.Count + DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));
                    for (int ii = 0; ii < infos.Count; ii++)
                    {
                        int nii = infosrnd.Next(0, infos.Count);
                        ReplInfo ritemp = infos[nii];
                        infos[nii] = infos[ii];
                        infos[ii] = ritemp;
                    }
                }
#if DEBUG_REPL
                if (infos.Count != slaves.Length)
                {
                    throw new Exception("DEBUG: (infos.Count != slaves.Length)");
                }
#endif

                System.Threading.Thread timethread = null;
                try
                {
                    // Build lists of which hosts pull which chunks...
                    foreach (dfs.DfsFile df in dc.Files)
                    {
                        if (null != files)
                        {
                            bool includefile = false;
                            for (int fi = 0; fi < files.Count; fi++)
                            {
                                if (0 == string.Compare(files[fi], df.Name, StringComparison.OrdinalIgnoreCase))
                                {
                                    includefile = true;
                                    break;
                                }
                            }
                            if (!includefile)
                            {
                                continue;
                            }
                        }
                        if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                            || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                        {
                            bool printedthisfile = false; // verbose only
                            foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                            {
                                try
                                {
                                    string[] chosts = fn.Host.Split(';');
                                    if (chosts.Length < 1 || string.IsNullOrEmpty(chosts[0]))
                                    {
                                        Console.Error.WriteLine("Invalid Host tag for: dfs://{0}", df.Name);
                                        continue;
                                    }

                                    if (chosts.Length >= replication)
                                    {
                                        continue; // Enough replications; go to next node.
                                    }
                                    {
                                        anywork = true;
                                        if (needsayreplicating == 1)
                                        {
                                            needsayreplicating = 2;
                                            //Console.WriteLine("Replicating...");
                                        }
                                        if (verbose)
                                        {
                                            if (null == timethread)
                                            {
                                                timethread = new System.Threading.Thread(new System.Threading.ThreadStart(timethreadproc_repl));
                                            }
                                        }
                                        if (verbose && !printedthisfile)
                                        {
                                            printedthisfile = true;
                                            string dfn = df.Name;
                                            if (dfn.StartsWith(".$"))
                                            {
                                                dfn = dfn.Substring(2);
                                                int idd = dfn.IndexOf(".$");
                                                if (-1 != idd)
                                                {
                                                    dfn = dfn.Substring(0, idd);
                                                }
                                            }
                                            //Console.WriteLine("    Replicating dfs://{0}", dfn);
                                            Console.WriteLine("    [{0}]        Replicating dfs://{1}", DateTime.Now.ToString(), dfn);
                                        }

                                        // Find host(s) for the chunk, ensuring it's not already there...
                                        {
                                            int neednhosts = replication - chosts.Length;
#if DEBUG_REPL
                                            if (neednhosts <= 0)
                                            {
                                                throw new Exception("DEBUG: (neednhosts <= 0)");
                                            }
#endif
                                            {
                                                int ii = 0;
                                                int infosStop = infos.Count;
                                                for (; ; )
                                                {
                                                    if (ii >= infosStop)
                                                    {
                                                        throw new ReplicationException("Cannot find host for replication");
                                                    }
                                                    string htarget = infos[ii].host;
                                                    bool onhost = false;
                                                    for (int ci = 0; ci < chosts.Length; ci++)
                                                    {
                                                        if (0 == string.Compare(IPAddressUtil.GetName(chosts[ci]), IPAddressUtil.GetName(htarget), StringComparison.OrdinalIgnoreCase))
                                                        {
                                                            onhost = true;
                                                            break;
                                                        }
                                                    }
                                                    if (!onhost)
                                                    {
                                                        // It's not on this host (htarget/infos[ii]), so replicate to it.
                                                        ReplInfo info = infos[ii];
                                                        ReplFileNode rfn;
                                                        rfn.node = fn;
                                                        rfn.ownerfile = df;
                                                        info.pullfiles.Add(rfn);
#if DEBUG_REPL
                                                        int DEBUG_CHECK_infosCount = infos.Count;
#endif
                                                        infos.DequeueAt(ii);
                                                        infos.Enqueue(info);
#if DEBUG_REPL
                                                        if (infos.Count != DEBUG_CHECK_infosCount)
                                                        {
                                                            throw new Exception("DEBUG: (infos.Count != DEBUG_CHECK_infosCount)");
                                                        }
#endif
                                                        if (0 == --neednhosts)
                                                        {
                                                            break;
                                                        }
                                                        infosStop--; // Don't consider this host that was removed and added to the end.
                                                        //ii = 0; // Pointless, because...
                                                        // Since the current ii was removed, and ones before it were skipped,
                                                        // it's fine to stay at the same index, which has the next potential host.
                                                        continue;
                                                    }
                                                    ii++;
                                                }
                                            }

                                        }

                                    }
                                }
                                catch (ReplicationException e)
                                {
                                    throw;
                                }
                                catch (Exception e)
                                {
                                    LogOutputToFile(e.ToString());
                                    if (verbose)
                                    {
                                        if (numwarnings == 0)
                                        {
                                            numwarnings++;
                                            ConsoleFlush();
                                            Console.Error.WriteLine("Replication error: {0}", e.Message);
                                            Console.Error.Flush();
                                        }
                                    }
                                    throw new ReplicationException(string.Format("Replication error: {0}", e.Message), e);
                                }
                            }
                        }
                        else if (0 == string.Compare(df.Type, DfsFileTypes.DELTA, StringComparison.OrdinalIgnoreCase)
                            || 0 == string.Compare(df.Type, DfsFileTypes.JOB, StringComparison.OrdinalIgnoreCase))
                        {
                            // Known not to be replicated...
                        }
                        else
                        {
                            if (verbose)
                            {
                                Console.Error.WriteLine("Skipping replication for file '{0}': replication not known for file of type '{1}'", df.Name, df.Type);
                            }
                        }
                    }

                    Dictionary<string, string> pullerrors = new Dictionary<string, string>(slaves.Length);

                    // Send these file-pull requests!
                    Random pfrnd = new Random(unchecked(DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));
                    MySpace.DataMining.Threading.ThreadTools<ReplInfo>.Parallel(
                        new Action<ReplInfo>(delegate(ReplInfo ri)
                        {
                            if (ri.pullfiles.Count > 0)
                            {

                                {
                                    // Randomize pull files list...
                                    for (int pfir = 0; pfir < ri.pullfiles.Count; pfir++)
                                    {
                                        int ni;
                                        lock (pfrnd)
                                        {
                                            ni = pfrnd.Next(0, ri.pullfiles.Count);
                                        }
                                        ReplFileNode trfn = ri.pullfiles[ni];
                                        ri.pullfiles[ni] = ri.pullfiles[pfir];
                                        ri.pullfiles[pfir] = trfn;
                                    }
                                }

                                System.Net.Sockets.NetworkStream nstm = null;
                                try
                                {
                                    nstm = Surrogate.ConnectService(ri.host);

                                    string spullpaths;
                                    {
                                        StringBuilder sbpullpaths = new StringBuilder(ri.pullfiles.Count * 100);
                                        for (int pfi = 0; pfi < ri.pullfiles.Count; pfi++)
                                        {
                                            if (0 != sbpullpaths.Length)
                                            {
                                                sbpullpaths.Append('\u0001');
                                            }
                                            ReplFileNode pf = ri.pullfiles[pfi];
                                            sbpullpaths.Append(Surrogate.NetworkPathForHost(pf.node.Host.Split(';')[0]) + @"\" + pf.node.Name);
                                            if (pf.ownerfile.HasZsa)
                                            {
                                                sbpullpaths.Append('\u0001');
                                                sbpullpaths.Append(Surrogate.NetworkPathForHost(pf.node.Host.Split(';')[0]) + @"\" + pf.node.Name + ".zsa");
                                            }

                                            // Since the replication phase aborts on error and doesn't update metadata in this case,
                                            // can update the Host with this host immediately, for when/if replication succeeds.
                                            {
                                                if (string.IsNullOrEmpty(pf.node.Host))
                                                {
                                                    pf.node.Host = ri.host;
                                                }
                                                else
                                                {
                                                    pf.node.Host += ";" + ri.host;
                                                }
                                            }

                                        }
                                        spullpaths = sbpullpaths.ToString();
                                    }

                                    if (ReplicationDebugVerbose)
                                    {
                                        lock (infos)
                                        {
                                            Console.WriteLine("[{0} pulling files: {1}]", ri.host, spullpaths.Replace("\u0001", ";").Replace("\u0002", "->"));
                                        }
                                    }

                                    byte[] opts = new byte[1 + 4 + 4];
                                    opts[0] = 0; // Disabled: per-file download feedback.
                                    MySpace.DataMining.DistributedObjects.Entry.ToBytes(dc.slave.CookTimeout, opts, 1);
                                    MySpace.DataMining.DistributedObjects.Entry.ToBytes(dc.slave.CookRetries, opts, 1 + 4);

                                    nstm.WriteByte((byte)'Y'); // Batch send.
                                    XContent.SendXContent(nstm, opts);
                                    XContent.SendXContent(nstm, spullpaths);

                                    int ib = nstm.ReadByte();
                                    if (ib == '-')
                                    {
                                        throw new ReplicationException("Host " + ri.host + " did not report a success for bulk file transfer");
                                    }
                                    if (ib == 'e')
                                    {
                                        string failedfiles = XContent.ReceiveXString(nstm, opts);
                                        lock (pullerrors)
                                        {
                                            pullerrors[ri.host] = failedfiles;
                                        }                                        
                                    }
                                }
                                catch (ReplicationException e)
                                {
                                    throw;
                                }
                                catch (Exception e)
                                {
                                    LogOutputToFile(e.ToString());
                                    if (verbose)
                                    {
                                        if (numwarnings == 0)
                                        {
                                            numwarnings++;
                                            ConsoleFlush();
                                            Console.Error.WriteLine("Replication error: {0}", e.Message);
                                            Console.Error.Flush();
                                        }
                                    }
                                    throw new ReplicationException(string.Format("Replication error: {0}", e.Message), e);
                                }

                                if (null != nstm)
                                {
                                    nstm.Close();
                                    nstm = null;
                                }

                            }

                            if (verbose)
                            {
                                lock (infos)
                                {
                                    Console.Write(MachineCompletedString);
                                    ConsoleFlush();
                                }
                            }

                        }), infos.ToArray(), infos.Count);

                    if (anywork)
                    {
                        if (pullerrors.Count > 0)
                        {
                            Console.WriteLine("Replication error:");

                            //get all unique repl file nodes by node name.
                            Dictionary<string, ReplFileNode> allreplfilenodes = new Dictionary<string, ReplFileNode>();
                            for(int ri = 0; ri < infos.Count; ri++)
                            {
                                foreach (ReplFileNode fn in infos[ri].pullfiles)
                                {
                                    string nodename = fn.node.Name.ToLower();
                                    if (!allreplfilenodes.ContainsKey(nodename))
                                    {
                                        allreplfilenodes.Add(nodename, fn);
                                    }
                                }
                            }

                            //removed failed replicated host from filenode.
                            foreach (KeyValuePair<string, string> pair in pullerrors)
                            {
                                string failedhost = pair.Key;
                                string[] nodenames = pair.Value.Split(';');
                                foreach (string nodename in nodenames)
                                {
                                    int del = nodename.LastIndexOf(@"\");
                                    string _nodename = nodename.Substring(del + 1).ToLower();
                                    if (allreplfilenodes.ContainsKey(_nodename))
                                    {
                                        ReplFileNode fn = allreplfilenodes[_nodename];
                                        string[] chosts = fn.node.Host.Split(';');
                                        string validhosts = "";
                                        foreach (string chost in chosts)
                                        {
                                            if (string.Compare(chost, failedhost, StringComparison.OrdinalIgnoreCase) != 0)
                                            {
                                                if (validhosts.Length > 0)
                                                {
                                                    validhosts += ';';
                                                }
                                                validhosts += chost;
                                            }
                                        }
                                        fn.node.Host = validhosts;

                                        Console.WriteLine("    File: {0}; Part: {1}; Failed Host: {2}", fn.ownerfile, fn.node.Name, failedhost);
                                    }
                                }
                            }
                        }

                        // Perform DFS merge!
                        if (ReplicationDebugVerbose)
                        {
                            Console.WriteLine("[Entering lock for DFS merge]");
                        }
                        using (LockDfsMutex())
                        {
                            if (ReplicationDebugVerbose)
                            {
                                Console.WriteLine("[Lock acquired, loading DFS.xml]");
                            }
                            // Note: "new*" means new changes to dfs.xml outside of the replication.
                            dfs newdc = LoadDfsConfig();
                            if (ReplicationDebugVerbose)
                            {
                                Console.WriteLine("[DFS.xml loaded, performing merge]");
                            }
                            replication = newdc.Replication;
                            Dictionary<string, dfs.DfsFile> newdfsfiles = new Dictionary<string, dfs.DfsFile>(newdc.Files.Count, new Surrogate.CaseInsensitiveEqualityComparer());
                            Dictionary<string, dfs.DfsFile.FileNode> newdfsfilenodes = new Dictionary<string, dfs.DfsFile.FileNode>(200);
                            foreach (dfs.DfsFile df in newdc.Files)
                            {
                                newdfsfiles[df.Name] = df;
                                foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                                {
                                    newdfsfilenodes[fn.Name] = fn;
                                }
                            }
                            foreach (dfs.DfsFile df in dc.Files)
                            {
                                if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                                    || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                                {
                                    dfs.DfsFile newdf = null;
                                    //newdf = newdc.Find(df.Name);
                                    if (newdfsfiles.ContainsKey(df.Name))
                                    {
                                        newdf = newdfsfiles[df.Name];
                                    }
                                    if (null != newdf)
                                    {
                                        foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                                        {
                                            dfs.DfsFile.FileNode newfn = null;
                                            //newfn = newdf.FindNode(fn.Name);
                                            if (newdfsfilenodes.ContainsKey(fn.Name))
                                            {
                                                newfn = newdfsfilenodes[fn.Name];
                                            }
                                            if (null != newfn)
                                            {
                                                if (fn.Host != newfn.Host)
                                                {
                                                    Dictionary<string, bool> allchosts = new Dictionary<string, bool>(new Surrogate.CaseInsensitiveEqualityComparer());
                                                    foreach (string chost in fn.Host.Split(';'))
                                                    {
                                                        allchosts[chost] = true;
                                                    }
                                                    foreach (string newchost in newfn.Host.Split(';'))
                                                    {
                                                        allchosts[newchost] = true;
                                                    }
                                                    {
                                                        StringBuilder sbchosts = new StringBuilder();
                                                        int narep = 0;
                                                        foreach (KeyValuePair<string, bool> kv in allchosts)
                                                        {
                                                            if (0 != sbchosts.Length)
                                                            {
                                                                sbchosts.Append(';');
                                                            }
                                                            sbchosts.Append(kv.Key);
                                                            if (++narep >= replication)
                                                            {
                                                                break;
                                                            }
                                                        }
                                                        newfn.Host = sbchosts.ToString();
                                                    }
                                                }

                                            }
                                        }
                                    }
                                }
                            }
                            if (ReplicationDebugVerbose)
                            {
                                Console.WriteLine("[Merge completed; updating DFS.xml]");
                            }
                            UpdateDfsXml(newdc); // !
                            if (ReplicationDebugVerbose)
                            {
                                Console.WriteLine("[DFS.xml updated; leaving DFS lock]");
                            }
                        }
                        if (ReplicationDebugVerbose)
                        {
                            Console.WriteLine("[Unlocked DFS, closing sockets]");
                        }
                    }

                }
                finally
                {
                    if (null != timethread)
                    {
                        timethread.Abort();
                        timethread = null;
                    }
                }

                if (verbose && NumberOfCoresRemainder > 0)
                {
                    Console.Write(new string(CORE_COMPLETED_CHAR, NumberOfCoresRemainder));
                    ConsoleFlush();
                }

                if (needsayreplicating == 2)
                {
                    Console.WriteLine("  Done");
                }
            }
#if REPLICATION_MUTEX
            finally
            {
                repmutex.ReleaseMutex();
            }
#endif
            return anywork;
        }

        public static bool ReplicationPhase(string file, bool verbose, int NumberOfCores, IList<string> TheseServersOnly)
        {
            string[] files;
            if (null == file)
            {
                files = null;
            }
            else
            {
                files = new string[] { file };
            }
            return ReplicationPhase(verbose, NumberOfCores, TheseServersOnly, files);
        }


        // Call this after lowering the replication count.
        public static void LowerReplicationCount(bool verbose)
        {
#if REPLICATION_MUTEX
            System.Threading.Mutex repmutex = new System.Threading.Mutex(false, "DOreplication");
            try
            {
                repmutex.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
#endif
            {
                int numwarnings = 0;
                // First we remove the extra hosts from dfs.xml and keep track of removed replicates.
                List<RemoteFile> removefiles = new List<RemoteFile>();
                RemoteFile rf; // = new RemoteFile();
                string[] machines;
                using (LockDfsMutex())
                {
                    dfs dc = LoadDfsConfig();
                    machines = dc.Slaves.SlaveList.Split(';');
                    int replication = dc.Replication;
                    if (replication > machines.Length)
                    {
                        replication = machines.Length;
                    }
                    foreach (dfs.DfsFile df in dc.Files)
                    {
                        foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                        {
                            string[] chosts = fn.Host.Split(';');
                            if (chosts.Length > replication)
                            {
                                StringBuilder sb = new StringBuilder();
                                for (int ci = 0; ci < chosts.Length; ci++)
                                {
                                    if (ci < replication)
                                    {
                                        if (0 != sb.Length)
                                        {
                                            sb.Append(';');
                                        }
                                        sb.Append(chosts[ci]);
                                    }
                                    else
                                    {
                                        rf.host = chosts[ci];
                                        rf.name = fn.Name;
                                        removefiles.Add(rf);
                                    }
                                }
                                fn.Host = sb.ToString();
                            }
                        }
                    }
                    if (removefiles.Count > 0)
                    {
                        UpdateDfsXml(dc);
                    }
                }

                // Now DFS.xml is updated, kill the actual files on disk.
                if (removefiles.Count > 0)
                {
                    if (verbose)
                    {
                        //Console.Write("Deleting extra replicates");
                        Console.WriteLine("Deleting extra replicates");
                        ConsoleFlush();
                    }
                    System.Threading.Thread timethread = null;
                    if (verbose)
                    {
                        timethread = new System.Threading.Thread(new System.Threading.ThreadStart(timethreadproc_repl));
                    }
                    try
                    {
                        //foreach (RemoteFile krf in removefiles)
                        MySpace.DataMining.Threading.ThreadTools<RemoteFile>.Parallel(
                            new Action<RemoteFile>(
                            delegate(RemoteFile krf)
                            {
                                try
                                {
                                    System.IO.File.Delete(Surrogate.NetworkPathForHost(krf.host) + @"\" + krf.name);
                                    /*
                                    if (verbose)
                                    {
                                        lock (removefiles)
                                        {
                                            Console.Write('.');
                                            ConsoleFlush();
                                        }
                                    }
                                     * */
                                }
                                catch (Exception e)
                                {
                                    if (numwarnings == 0)
                                    {
                                        lock (removefiles)
                                        {
                                            numwarnings++;
                                            ConsoleFlush();
                                            Console.Error.WriteLine("Replication warning: {0}", e.Message);
                                            Console.Error.Flush();
                                        }
                                    }
                                }
                            }), removefiles, machines.Length + machines.Length / 2);
                    }
                    finally
                    {
                        if (null != timethread)
                        {
                            timethread.Abort();
                            timethread = null;
                        }
                    }
                    if (verbose)
                    {
                        Console.WriteLine("Done");
                    }
                }

            }
#if REPLICATION_MUTEX
            finally
            {
                repmutex.ReleaseMutex();
            }
#endif
        }


        struct RemoteFile
        {
            internal string host;
            internal string name;
        }


        public class ReplicationException : Exception
        {
            public ReplicationException(string msg)
                : base(msg)
            {
            }

            public ReplicationException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }
        }


        static void timethreadproc_repl()
        {
            try
            {
                for (; ; )
                {
                    System.Threading.Thread.Sleep(1000 * 10);

                    /*ConsoleColor oldfc = Console.ForegroundColor;
                    Console.ForegroundColor = ConsoleColor.DarkGray;*/
                    Console.Write('.');
                    ConsoleFlush();
                    //Console.ForegroundColor = oldfc;
                }
            }
            catch
            {
            }
        }


        // Used by ReplicationPhase and RemoveMachine
        class RThread
        {
            internal int nexttarget;


            internal delegate int NewInit();

            internal static RThread[] NewArray(int count, NewInit init)
            {
                RThread[] result = new RThread[count];
                for (int i = 0; i < count; i++)
                {
                    result[i] = new RThread();
                    result[i].nexttarget = init();
                }
                return result;
            }

        }



        class RMachine
        {
            internal string host;
            internal System.Net.Sockets.NetworkStream sock;
        }


        public static void MetaRemoveMachine(string oldhost, bool DontTouchOldHost, bool force)
        {
#if DEBUG_REPL
            if (!IsAdminCmd)
            {
                throw new Exception("EnterAdminCmd required");
            }
#endif
            //using (LockDfsMutex())
            {
                if (0 == string.Compare(IPAddressUtil.GetName(oldhost), IPAddressUtil.GetName(System.Net.Dns.GetHostName()), StringComparison.OrdinalIgnoreCase))
                {
                    Console.Error.WriteLine("Cannot remove meta for surrogate");
                    SetFailure();
                    return;
                }

                List<string> oldies = new List<string>();
                oldies.Add(oldhost);

                dfs dc = LoadDfsConfig();
                {
                    dc.Slaves.SlaveList = RemoveFromHosts(oldies, dc.Slaves.SlaveList);
                }

                bool needforce = false;
                foreach (dfs.DfsFile df in dc.Files)
                {
                    int numbadparts = 0;
                    long newsize = df.Size;
                    List<dfs.DfsFile.FileNode> newnodes = new List<dfs.DfsFile.FileNode>(df.Nodes.Count);
                    //long curposition = 0;
                    foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                    {
                        fn.Host = RemoveFromHosts(oldies, fn.Host);
                        if (fn.Host.Length == 0)
                        {
                            numbadparts++;
                            needforce = true;
                            newsize -= fn.Length;
                            break;
                        }
                        else
                        {
                            //fn.Position = curposition;
                            //curposition += fn.Length;
                            newnodes.Add(fn);
                        }
                    }
                    if (numbadparts != 0)
                    {
                        Console.WriteLine("{4}  DFS file '{0}' will have {1} parts lost, resulting in {2} data loss from {3} {5}",
                            df.Name,
                            df.Nodes.Count - numbadparts,
                            GetFriendlyByteSize(df.Size - newsize),
                            GetFriendlyByteSize(df.Size),
                            isdspace ? "\u00014" : "",
                            isdspace ? "\u00010" : ""
                            );
                        df.Nodes = newnodes;
                        df.Size = newsize;
                    }
                }
                if (needforce && !force)
                {
                    Console.Error.WriteLine("Not performing machine removal; use -f to override");
                    SetFailure();
                    return;
                }

                int newblockbase = dc.Slaves.SlaveList.Split(';').Length * Surrogate.NumberOfProcessors;
                int newblockcount = NearestPrimeGE(newblockbase);
                int newsortedblockcount = newblockbase;

                dc.Blocks.TotalCount = newblockcount;
                dc.Blocks.SortedTotalCount = newsortedblockcount;
                UpdateDfsXml(dc);

            }

            if (!DontTouchOldHost)
            {
                System.Threading.Thread sdthread = new System.Threading.Thread(
                    new System.Threading.ThreadStart(
                    delegate
                    {
                        try
                        {
                            string ohnetpath = Surrogate.NetworkPathForHost(oldhost);
                            System.IO.File.Delete(ohnetpath + @"\slave.dat");
                        }
                        catch
                        {
                        }
                    }));
                sdthread.Start();
                if (!sdthread.Join(1000 * 10))
                {
                    Console.WriteLine("Warning: timed out while accessing removed machine");
                }
            }

            Console.WriteLine("Done");

        }


        public static void RemoveMachine(string oldhost)
        {
#if DEBUG_REPL
            if (!IsAdminCmd)
            {
                throw new Exception("EnterAdminCmd required");
            }
#endif
            bool needrepl = false;
            //using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();
                string[] slaves = dc.Slaves.SlaveList.Split(';');

                string newmaster = null; // non-null only if moving the surrogate.

                bool oldhostfound = false;
                string newslavesstring = "";
                {
                    int oldindex = -1;
                    for (int si = 0; si < slaves.Length; si++)
                    {
                        //if (0 == string.Compare(oldhost, slaves[si], StringComparison.OrdinalIgnoreCase))
                        if (0 == string.Compare(IPAddressUtil.GetName(oldhost), IPAddressUtil.GetName(slaves[si]), StringComparison.OrdinalIgnoreCase))
                        {
                            oldindex = si;
                            oldhostfound = true;
                        }
                    }
                    if (-1 != oldindex)
                    {
                        {
                            StringBuilder sb = new StringBuilder(100);
                            for (int si = 0; si < slaves.Length; si++)
                            {
                                if (si != oldindex)
                                {
                                    if (0 != sb.Length)
                                    {
                                        sb.Append(';');
                                    }
                                    sb.Append(slaves[si]);
                                }
                            }
                            newslavesstring = sb.ToString();
                        }
                        slaves = newslavesstring.Split(';'); // Excluding the one being removed!

                        if (dc.Replication > slaves.Length)
                        {
                            Console.Error.WriteLine("Replication factor of {0} prevents machine from being removed (must lower replication or add another machine first)", dc.Replication);
                            SetFailure();
                            return;
                        }

                    }
                }
                int newblockbase = slaves.Length * Surrogate.NumberOfProcessors;
                int newblockcount = NearestPrimeGE(newblockbase);
                int newsortedblockcount = newblockbase;

                {
                    string netdir = null;
                    try
                    {
                        netdir = Surrogate.NetworkPathForHost(oldhost);
                    }
                    catch
                    {
                    }
                    if (null != netdir)
                    {
                        string olddfsxmlpath = netdir + @"\" + dfs.DFSXMLNAME;
                        if (dfs.DfsConfigExists(olddfsxmlpath))
                        {
                            // dfs.xml exists, so see if it's this cluster's surrogate...
                            if(0 == string.Compare(IPAddressUtil.GetName(oldhost), IPAddressUtil.GetName(Surrogate.MasterHost), StringComparison.OrdinalIgnoreCase))
                            {
                                // Removing this cluster's surrogate (this machine)...
                                if (0 == slaves.Length || string.IsNullOrEmpty(slaves[0]))
                                {
                                    //throw new Exception("Surrogate cannot be moved because there are no more machines in the cluster");
                                    // Falls through to "cannot delete last host"
                                }
                                else
                                {

                                    //oldmaster = oldhost; // (due to above check).
                                    newmaster = slaves[0];

                                    if (!oldhostfound)
                                    {
                                        _MoveSurrogate(oldhost, newmaster, slaves);
                                        // Nothing more to do because I removed the surrogate
                                        // which wasn't a part of the cluster's processing..
                                        // Note: in this case, 'Removing machine' isn't printed;
                                        // that's fine because it actually wasn't part of the cluster,
                                        // it was just the surrogate.
                                        Console.Out.WriteLine("Done");
                                        return;
                                    }
                                }
                            }
                            else
                            {
                                // Removing another cluster's surrogate? doesn't make sense..
                                // falls through to "old host not found"
                                if (oldhostfound)
                                {
                                    throw new Exception("Assertion failure; accessing another cluster");
                                }
                            }
                        }
                    }
                }

                if (0 == slaves.Length || string.IsNullOrEmpty(slaves[0]))
                {
                    //Console.Error.WriteLine("removemachine error: cannot delete last host; division by zero");
                    //SetFailure();
                    //return;
                    // Note: even if no files in DFS this still happens because there's still nowhere to put DFS configuration (e.g. empty file system still takes up space)
                    throw new Exception("Out of DFS disk space: there is not enough free space in DFS of the remaining cluster to redistribute the data residing on the machine being removed (cannot delete last host; division by zero)");
                }

                if (!oldhostfound)
                {
                    Console.Error.WriteLine("removemachine error: old host not found in SlaveList");
                    SetFailure();
                    return;
                }

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
                            string state = "initial";
                            System.IO.FileInfo fizd = null;
                            try
                            {
                                //string slave = slaves[si];
                                state = "Surrogate.NetworkPathForHost";
                                string snetdir = Surrogate.NetworkPathForHost(slave);
                                state = "GetDiskFreeBytes";
                                long x = (long)GetDiskFreeBytes(snetdir);
                                lock (slaves)
                                {
                                    if (x < freemin)
                                    {
                                        freemin = x;
                                    }
                                }
                                state = "DirectoryInfo";
                                System.IO.DirectoryInfo netdi = new System.IO.DirectoryInfo(snetdir);
                                {
                                    state = "GetFiles zd*.zd";
                                    System.IO.FileInfo[] fis = (netdi).GetFiles("zd*.zd");
                                    state = " Succeeded GetFiles zd*.zd";
                                   
                                    lock (slaves)
                                    {
                                        foreach (System.IO.FileInfo fi in fis)
                                        {
                                            fizd = fi;
                                            newspacezdcount++;
                                            newspacezdsizes += fi.Length;
                                        }
                                    }
                                }
                                {
                                    state = "GetFiles  zd*.zd.zsa";
                                    System.IO.FileInfo[] fis = (netdi).GetFiles("zd*.zd.zsa");
                                    lock (slaves)
                                    {
                                        foreach (System.IO.FileInfo fi in fis)
                                        {
                                            newspacezsasizes += fi.Length;
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("Removemachine error at host: " + slave + 
                                    "  Error: " + ex.ToString() + " State: " + state + " newspacezdcount: " 
                                    + newspacezdcount.ToString() + " newspacezdsizes: " + newspacezdsizes.ToString()
                                    + " newspacezsasizes: " + newspacezsasizes.ToString() + " fizd: " + (fizd == null  ? "null" : fizd.FullName));
                            }
                            
                        }), slaves);
                    // Note: RemoveMachine: slaves already has the removed machine removed from the array at this point.
                    long freemintotal = freemin * slaves.Length;
                    long newspacetotal = newspacezdsizes + newspacezsasizes;
                    if (newspacetotal > 0)
                    {
                        // Add a little padding (another average size data-node-chunk [64MB?])
                        long newspacepaddingtotal = (newspacezdsizes / newspacezdcount) * slaves.Length;
                        if (newspacetotal + newspacepaddingtotal >= freemintotal)
                        {
                            throw new Exception("Out of DFS disk space: there is not enough free space in DFS of the remaining cluster to redistribute the data residing on the machine being removed");
                        }
                    }
                }

                List<RMachine> rms;
                {
                    rms = new List<RMachine>(slaves.Length);
                    for (int si = 0; si < slaves.Length; si++)
                    {
                        rms.Add(new RMachine());
                        rms[si].host = slaves[si];
                    }
                }

                Console.Write("  Removing machine {0}", oldhost);
                ConsoleFlush();

                {

                    try
                    {
                        string olddir = Surrogate.NetworkPathForHost(oldhost);
                        try
                        {
                            System.IO.File.Delete(olddir + @"\slave.dat");
                        }
                        catch
                        {
                        }
                    }
                    catch
                    {
                    }

                    List<string> enddelete = new List<string>();
                    List<dfs.DfsFile> caches = new List<dfs.DfsFile>(dc.Files.Count);
                    List<dfs.DfsFile> keeps = new List<dfs.DfsFile>(dc.Files.Count);

                    Random rnd = new Random();
                    int slavetarget = rnd.Next() % slaves.Length;
                    {
                        foreach (dfs.DfsFile df in dc.Files)
                        {
                            if (0 == string.Compare(df.Type, DfsFileTypes.DELTA, StringComparison.OrdinalIgnoreCase))
                            {
                                caches.Add(df);
                            }
                            else
                            {
                                keeps.Add(df);

                                if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                                    || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                                {
                                    MySpace.DataMining.Threading.ThreadTools<dfs.DfsFile.FileNode>.Parallel(
                                        new Action<dfs.DfsFile.FileNode>(
                                        delegate(dfs.DfsFile.FileNode fn)
                                        {
                                            List<string> xchosts = new List<string>(fn.Host.Split(';'));
                                            bool cfound = false;
                                            for (int chimax = 0; chimax < 20; chimax++)
                                            {
                                                int chi = ListContainsHostIndexOf(xchosts, oldhost);
                                                if (-1 == chi)
                                                {
                                                    break;
                                                }
                                                cfound = true;
                                                xchosts.RemoveAt(chi);
                                            }
                                            if (cfound)
                                            {
                                                string newslave = null;
                                                lock (slaves)
                                                {
                                                    for (int xfind = 0; ; xfind++)
                                                    {
                                                        if (xfind > slaves.Length)
                                                        {
                                                            Console.Error.WriteLine("Unable to find host for chunk '{0}' for DFS file '{1}'", fn.Name, df.Name);
                                                            cfound = false; // No destination available.
                                                            break;
                                                        }
                                                        slavetarget++;
                                                        if (slavetarget >= slaves.Length)
                                                        {
                                                            slavetarget = 0;
                                                        }
                                                        if (-1 == ListContainsHostIndexOf(xchosts, slaves[slavetarget]))
                                                        {
                                                            newslave = slaves[slavetarget];
                                                            break;
                                                        }
                                                    }
                                                }
                                                if (cfound) // Can be set back to false if no destination is available.
                                                {
                                                    try
                                                    {
                                                        string srcfn = Surrogate.NetworkPathForHost(oldhost) + @"\" + fn.Name;
                                                        string destfn = Surrogate.NetworkPathForHost(newslave) + @"\" + fn.Name;
                                                        string srcfnzsa = srcfn + ".zsa";
                                                        string destfnzsa = destfn + ".zsa";
                                                        {
                                                            // Copy and then delete at end to ensure transaction.
                                                            System.IO.File.Copy(srcfn, destfn, true);
                                                            if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase))
                                                            {
                                                                System.IO.File.Copy(srcfnzsa, destfnzsa, true);
                                                            }
                                                            // ---
                                                            /*try
                                                            {
                                                                System.IO.File.Delete(srcfn);
                                                                System.IO.File.Delete(srcfnzsa);
                                                            }
                                                            catch
                                                            {
                                                            }*/
                                                            // Delete them at the very end to ensure full removemachine transaction.
                                                            lock (enddelete)
                                                            {
                                                                enddelete.Add(srcfn);
                                                                enddelete.Add(srcfnzsa);
                                                            }
                                                        }
                                                        Console.Write('.');
                                                        ConsoleFlush();
                                                        xchosts.Add(newslave);
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        if (dc.Replication > 1)
                                                        {
                                                            if (xchosts.Count > 0)
                                                            {
                                                                needrepl = true;
                                                            }
                                                            else
                                                            {
                                                                throw new Exception("Failed to copy data-node-chunk and no data replicates for this one", e);
                                                            }
                                                        }
                                                        else
                                                        {
                                                            throw new Exception("Failed to copy data-node-chunk and no data replication enabled", e);
                                                        }
                                                    }
                                                }
                                                fn.Host = string.Join(";", xchosts.ToArray());
                                            }
                                        }), df.Nodes);
                                }
                                else if (0 == string.Compare(df.Type, DfsFileTypes.JOB, StringComparison.OrdinalIgnoreCase))
                                {
                                    try
                                    {
                                        if (0 != string.Compare(df.Nodes[0].Host, Surrogate.MasterHost, StringComparison.OrdinalIgnoreCase))
                                        {
                                            System.IO.File.Move(Surrogate.NetworkPathForHost(df.Nodes[0].Host.Split(';')[0]) + @"\" + df.Nodes[0].Name, Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\" + df.Nodes[0].Name);
                                            df.Nodes[0].Host = Surrogate.MasterHost;
                                        }
                                    }
                                    catch
                                    {
                                    }
                                }
                                else
                                {
                                    if (!QuietMode)
                                    {
                                        Console.Error.WriteLine("Warning: cannot handle DFS file of type {0}", df.Type);
                                    }
                                }
                            }                            
                        }

                        // Remove cac dlls
                        {
                            try
                            {
                                string netpath = NetworkPathForHost(oldhost);
                                string cacpath = netpath + @"\" + dfs.DLL_DIR_NAME;
                                foreach (string dllfn in System.IO.Directory.GetFiles(cacpath, "*.dll")) // Throws if no cac dir.
                                {
                                    try
                                    {
                                        System.IO.File.Delete(dllfn);
                                    }
                                    catch
                                    {
                                    }
                                }
                                System.IO.Directory.Delete(cacpath); // Throws if non-dll files or directories were in the cac dir.
                            }
                            catch
                            {
                            }
                        }

                        //Delete cache files
                        {
                            _KillSnowballFileChunks_unlocked_mt(caches, false);
                            dc.Files = keeps;
                        }
                    }

                    {
                        // Update DFS!..
                        dc.Slaves.SlaveList = newslavesstring;
                        dc.Blocks.TotalCount = newblockcount;
                        dc.Blocks.SortedTotalCount = newsortedblockcount;
                        UpdateDfsXml(dc);
                    }

                    if (null != newmaster)
                    {
                        _MoveSurrogate(oldhost, newmaster, slaves);
                    }

                    // Complete the entire removemachine transaction by deleting the old files on the removed machine...
                    //lock (enddelete)
                    {
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string delfn)
                            {
                                try
                                {
                                    System.IO.File.Delete(delfn);
                                }
                                catch
                                {
                                }
                            }), enddelete, slaves.Length * 2);
                    }

                    if (needrepl)
                    {
                        ReplicationPhase(null, false, 0, slaves); // We're locked in admin cmd.
                    }

                    Console.Out.WriteLine(Environment.NewLine + "Done");

                }
            }
        }


        static void _MoveSurrogate(string oldhost, string newmaster, IList<string> slaves)
        {
            // Update slave.dat on the other machines (set new master)...
            for (int si = 1; si < slaves.Count; si++)
            {
                WriteSlaveDat(slaves[si], newmaster);
            }

            // Update slave.dat on the new master (delete it)...
            try
            {
                System.IO.File.Delete(Surrogate.NetworkPathForHost(newmaster) + @"\slave.dat");
            }
            catch
            {
            }

            // Copy dfs.xml over to the new surrogate..
            System.IO.File.Copy(Surrogate.NetworkPathForHost(oldhost) + @"\" + dfs.DFSXMLNAME, Surrogate.NetworkPathForHost(newmaster) + @"\" + dfs.DFSXMLNAME, false); // Don't overwrite; let it fail.try
            try
            {
                // Rename old surrogate's dfs.xml to a backup..
                System.IO.File.Move(Surrogate.NetworkPathForHost(oldhost) + @"\" + dfs.DFSXMLNAME, Surrogate.NetworkPathForHost(oldhost) + @"\" + dfs.DFSXMLNAME + "." + DateTime.Now.ToString("yyyyMMddHHmmss") + ".bak");
            }
            catch
            {
            }

            Surrogate.SetNewMasterHost(newmaster); // Note: doesn't update default dfsxmlpath.
        }


        public static void AddMachine(string newhost)
        {
#if DEBUG_REPL
            if (!IsAdminCmd)
            {
                throw new Exception("EnterAdminCmd required");
            }
#endif
            if (!VerifyHostPermissions(new string[] { newhost }))
            {
                Console.Error.WriteLine("Unable to add machine: ensure the Windows service is installed and running on '{0}'", newhost);
                SetFailure();
                return;
            }
            //using (LockDfsMutex()) // Locked by adminlock.
            {
                dfs dc = LoadDfsConfig();
                string[] slaves = dc.Slaves.SlaveList.Split(';');
                int newblockbase = (slaves.Length + 1) * Surrogate.NumberOfProcessors;
                int newblockcount = NearestPrimeGE(newblockbase);
                int newsortedblockcount = newblockbase;
                for (int si = 0; si < slaves.Length; si++)
                {
                    //if (0 == string.Compare(newhost, slaves[si], StringComparison.OrdinalIgnoreCase))
                    if (0 == string.Compare(IPAddressUtil.GetName(newhost), IPAddressUtil.GetName(slaves[si]), StringComparison.OrdinalIgnoreCase))
                    {
                        Console.Error.WriteLine("addmachine error: the host '{0}' is already a machine of this cluster", newhost);
                        SetFailure();
                        return;
                    }
                }

                string newhostnetpath;
                try
                {
                    newhostnetpath = Surrogate.NetworkPathForHost(newhost);
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("Unable to communicate with machine '{0}': {1}  [ensure the Windows service is running on the new machine]", newhost, e.Message);
                    SetFailure();
                    return;
                }

                bool writeslavedat = true;
                {
                    string netdir = null;
                    string newhostmaster = null;
                    try
                    {
                        netdir = Surrogate.NetworkPathForHost(newhost);
                        newhostmaster = Surrogate.LocateMasterHost(netdir);
                    }
                    catch
                    {
                    }
                    if (null != newhostmaster)
                    {
                        if (0 != string.Compare(IPAddressUtil.GetName(newhostmaster), IPAddressUtil.GetName(Surrogate.MasterHost), StringComparison.OrdinalIgnoreCase))
                        {
                            if (dfs.DfsConfigExists(Surrogate.NetworkPathForHost(newhostmaster) + @"\" + dfs.DFSXMLNAME))
                            {
                                // Need to remove the machine from its current/old cluster..
                                Console.WriteLine("  Removing machine {0} from old cluster...", newhost);
                                Shell("DSpace @=" + newhostmaster + " -@log [user:`" + douser + "` cluster:`" + Surrogate.MasterHost + "` addmachine:`" + newhost + "`]");
                                Shell("DSpace @=" + newhostmaster + " removemachine " + newhost); // Important: let this failure bail out of addmachine!
                            }
                        }
                        else
                        {
                            // It's already configured to be part of this cluster, but technically it isn't;
                            // e.g. it's this cluster's surrogate but not part of the cluster yet, etc.
                            // Just go forward and add it...
                            writeslavedat = false;
                        }
                    }
                }

                // Above block should take care of removing dfs.xml
                if (writeslavedat)
                {
                    WriteSlaveDat(newhost);
                }

                // All file chunks on the slaves; hnodes[n] are data-node-chunks on slaves[n].
                List<List<dfs.DfsFile.FileNode>> hnodes = new List<List<dfs.DfsFile.FileNode>>(slaves.Length);
                List<dfs.DfsFile> keeps = new List<dfs.DfsFile>(dc.Files.Count);
                List<dfs.DfsFile> caches = new List<dfs.DfsFile>(dc.Files.Count);
                int newavgchunks = 0;
                if (slaves.Length > 0)
                {
                    for (int si = 0; si < slaves.Length; si++)
                    {
                        hnodes.Add(new List<dfs.DfsFile.FileNode>(200));
                    }
                    Dictionary<string, int> hosttoslaveindex = new Dictionary<string, int>(new Surrogate.CaseInsensitiveEqualityComparer());
                    long totalchunks = 0;
                    for (int si = 0; si < slaves.Length; si++)
                    {
                        hosttoslaveindex[IPAddressUtil.GetName(slaves[si])] = si;
                    }
                    for (int fi = 0; fi < dc.Files.Count; fi++)
                    {
                        dfs.DfsFile df = dc.Files[fi];

                        if (0 == string.Compare(DfsFileTypes.DELTA, df.Type, true))
                        {
                            caches.Add(df);
                        }
                        else
                        {
                            keeps.Add(df);
                        }

                        for (int ni = 0; ni < df.Nodes.Count; ni++)
                        {
                            dfs.DfsFile.FileNode fn = df.Nodes[ni];
                            //string hn = IPAddressUtil.GetName(fn.Host.Split(';')[0]);
                            foreach (string _hn in fn.Host.Split(';'))
                            {
                                string hn = IPAddressUtil.GetName(_hn);
                                if (hosttoslaveindex.ContainsKey(hn))
                                {
                                    totalchunks++;
                                    int si = hosttoslaveindex[hn];
                                    hnodes[si].Add(fn);
                                }
                            }
                        }
                    }
                    newavgchunks = (int)(totalchunks / (slaves.Length + 1)); // Average after adding this one.
                }


                Console.Write("  Adding machine {0}", newhost);
                ConsoleFlush();
                int nmoved = 0;
                bool errors = false;
                int nthreads = dc.AddMachineMinThreads;
                if (slaves.Length > nthreads)
                {
                    nthreads = slaves.Length;
                }
                if (slaves.Length > 0) // If no slaves, nothing to copy!
                {
                    string newhostfield = newhost;
                    Random rnd = new Random(DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                    int nextslave = rnd.Next(0, slaves.Length);
                    DataMining.Threading.ThreadTools.Parallel(
                        new Action<int>(
                        delegate(int _dummy)
                        {
                            {
                                dfs.DfsFile.FileNode node;
                                lock (hnodes) // !
                                {
                                    int si;
                                    int numzero = 0;
                                    do
                                    {
                                        si = nextslave++;
                                        if (nextslave >= slaves.Length)
                                        {
                                            nextslave = 0;
                                        }
                                        if (numzero++ > slaves.Length)
                                        {
                                            return; // ...
                                        }
                                    }
                                    while (hnodes[si].Count < 1);
                                    int ni = rnd.Next(0, hnodes[si].Count);
                                    node = hnodes[si][ni];
                                    {
                                        // Exclude from all machines to avoid getting another replicate.
                                        for (int ihn = 0; ihn < hnodes.Count; ihn++)
                                        {
                                            hnodes[ihn].Remove(node);
                                        }
                                    }
                                }
                                {
                                    try
                                    {
                                        string[] chosts = node.Host.Split(';');
                                        int fromhostindex = nmoved % chosts.Length;
                                        string fromhost = chosts[fromhostindex];
                                        {
                                            string srcfn = Surrogate.NetworkPathForHost(fromhost) + @"\" + node.Name;
                                            string destfn = Surrogate.NetworkPathForHost(newhostfield) + @"\" + node.Name;
                                            string srcfnzsa = srcfn + ".zsa";
                                            string destfnzsa = destfn + ".zsa";
                                            // Copy and then delete at end to ensure transaction.
                                            System.IO.File.Copy(srcfn, destfn, true);
                                            //if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase))
                                            {
                                                System.IO.File.Copy(srcfnzsa, destfnzsa, true);
                                            }
                                            // ---
                                            try
                                            {
                                                System.IO.File.Delete(srcfn);
                                                System.IO.File.Delete(srcfnzsa);
                                            }
                                            catch
                                            {
                                            }
                                        }
                                        //node.Host = newhostfield;
                                        {
                                            List<string> xchosts = new List<string>(chosts);
                                            xchosts.RemoveAt(fromhostindex);
                                            xchosts.Add(newhostfield);
                                            node.Host = string.Join(";", xchosts.ToArray());
                                        }
                                        System.Threading.Interlocked.Increment(ref nmoved);
                                        Console.Write('.');
                                        ConsoleFlush();
                                    }
                                    catch (System.IO.IOException e)
                                    {
                                        /*
                                        LogOutputToFile(e.ToString());
                                        errors = true;
                                        Console.Error.WriteLine();
                                        Console.Error.WriteLine("    Unable to move data-node chunk {0} from {1}", node.Name, node.Host);
                                        Console.Error.Flush();
                                         * */
                                    }
                                }
                            }
                        }), newavgchunks, nthreads); // avgchunks == number of files to copy to the new machine!

                    // Copy cac dlls from another slave.
                    {
                        string slavenetpath = NetworkPathForHost(slaves[0]);
                        string slavecacpath = slavenetpath + @"\" + dfs.DLL_DIR_NAME;
                        if (System.IO.Directory.Exists(slavecacpath))
                        {
                            string newnetpath = NetworkPathForHost(newhost);
                            string newcacpath = newnetpath + @"\" + dfs.DLL_DIR_NAME;
                            try
                            {
                                System.IO.Directory.CreateDirectory(newcacpath);
                            }
                            catch
                            {
                            }
                            MySpace.DataMining.Threading.ThreadTools<System.IO.FileInfo>.Parallel(
                                new Action<System.IO.FileInfo>(
                                delegate(System.IO.FileInfo dllfi)
                                {
                                    System.IO.File.Copy(dllfi.FullName, newcacpath + @"\" + dllfi.Name, true);
                                }), (new System.IO.DirectoryInfo(slavecacpath)).GetFiles("*.dll"), nthreads);
                        }
                    }

                    //Delete cache files
                    {
                        _KillSnowballFileChunks_unlocked_mt(caches, false);
                        dc.Files = keeps;
                    }
                }
                if (0 == nmoved && errors)
                {
                    Console.Out.WriteLine();
                    Console.Error.WriteLine("Errors were encountered and unable to distribute; machine not added");
                }
                else
                {
                    if (slaves.Length > 0)
                    {
                        dc.Slaves.SlaveList += ";" + newhost;
                    }
                    else
                    {
                        dc.Slaves.SlaveList = newhost;
                    }
                    dc.Blocks.TotalCount = newblockcount;
                    dc.Blocks.SortedTotalCount = newsortedblockcount;
                    UpdateDfsXml(dc);

                    Console.Out.WriteLine(Environment.NewLine + "Done");
                }

            }
        }


    }

}

