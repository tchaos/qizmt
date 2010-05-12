using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{
    public class FileDaemon
    {

        public static void RunScanner()
        {
            string thishost = System.Net.Dns.GetHostName();
            Random rnd = new Random(unchecked(DateTime.Now.Millisecond
                + System.Diagnostics.Process.GetCurrentProcess().Id
                + 7179259));

            // Random sleep to hit surrogate\dfs.xml at different times.
            System.Threading.Thread.Sleep(rnd.Next(5000, 30000));

            MySpace.DataMining.AELight.dfs dc
                = MySpace.DataMining.AELight.dfs.ReadDfsConfig_unlocked(
                    MySpace.DataMining.AELight.Surrogate.NetworkPathForHost(MySpace.DataMining.AELight.Surrogate.MasterHost)
                        + @"\" + MySpace.DataMining.AELight.dfs.DFSXMLNAME);

            if (null == dc.FileDaemon || !dc.FileDaemon.Enabled)
            {
                dc = null;
                _ThreadDisabled();
                return;
            }

#if optional
            try
            {
                System.IO.FileInfo fslog = new System.IO.FileInfo("filescan.log");
                if (!fslog.Exists || fslog.Length < 0x400 * 0x400)
                {
                    System.IO.File.AppendAllText("filescan.log",
                        "[" + DateTime.Now + "] " + thishost + " <enabled>"
                        + Environment.NewLine);
                }
            }
            catch
            {
            }
#endif

            for (; ; System.Threading.Thread.Sleep(1000 * 60 * 1))
            {
                string[] chunks = System.IO.Directory.GetFiles(".", "zd.*.zd");
                for (int ic = 0; ic < chunks.Length; ic++)
                {
                    int ri = rnd.Next(0, chunks.Length);
                    string s = chunks[ic];
                    chunks[ic] = chunks[ri];
                    chunks[ri] = s;
                }

#if optional
                try
                {
                    System.IO.FileInfo fslog = new System.IO.FileInfo("filescan.log");
                    if (!fslog.Exists || fslog.Length < 0x400 * 0x400)
                    {
                        System.IO.File.AppendAllText("filescan.log",
                            "[" + DateTime.Now + "] " + thishost + " <start> scanning " + chunks.Length + " chunks"
                            + Environment.NewLine);
                    }
                }
                catch
                {
                }
#endif

                for (int ic = 0; ic < chunks.Length; ic++)
                {
                    _ScanChunkSleep(dc.FileDaemon.ScanChunkSleep);

                    string chunk = chunks[ic];
                    int pos = -1;
                    try
                    {
                        using (System.IO.FileStream fs = new System.IO.FileStream(chunk,
                            System.IO.FileMode.Open, System.IO.FileAccess.Read,
                            System.IO.FileShare.ReadWrite | System.IO.FileShare.Delete))
                        {
                            pos++;
                            for (; ; )
                            {
                                int ib = fs.ReadByte();
                                if (-1 == ib)
                                {
                                    break;
                                }
                                pos++;
                            }
                        }
                    }
                    catch (System.IO.FileNotFoundException)
                    {
                        // Valid for file to be deleted.
                    }
                    catch
                    {
                        try
                        {
                            string cn = chunk;
                            {
                                int ils = Math.Max(cn.LastIndexOf('/'), cn.LastIndexOf('\\'));
                                if (-1 != ils)
                                {
                                    cn = cn.Substring(ils + 1);
                                }
                            }
                            System.IO.FileInfo fslog = new System.IO.FileInfo("filescan.log");
                            if (!fslog.Exists || fslog.Length < 0x400 * 0x400)
                            {
                                System.IO.File.AppendAllText("filescan.log",
                                    "[" + DateTime.Now + "] " + thishost + " " + pos + " \"" + cn + "\""
                                    + Environment.NewLine);
                            }
                        }
                        catch
                        {
                        }
                    }

                }

                try
                {
                    System.IO.FileInfo fslog = new System.IO.FileInfo("filescan.log");
                    if (!fslog.Exists || fslog.Length < 0x400 * 0x400)
                    {
                        System.IO.File.AppendAllText("filescan.log",
                            "[" + DateTime.Now + "] " + thishost + " <done> scanned " + chunks.Length + " chunks"
                            + Environment.NewLine);
                    }
                }
                catch
                {
                }

            }

        }


        public static void RunRepairer()
        {
            string thishost = System.Net.Dns.GetHostName();
            Random rnd = new Random(unchecked(DateTime.Now.Millisecond
                + System.Diagnostics.Process.GetCurrentProcess().Id
                + 5757981));

            System.Threading.Thread.Sleep(7000);

            if (!MySpace.DataMining.AELight.dfs.DfsConfigExists(
                MySpace.DataMining.AELight.dfs.DFSXMLNAME))
            {
                _ThreadDisabled();
                return;
            }

            MySpace.DataMining.AELight.dfs dc
                = MySpace.DataMining.AELight.dfs.ReadDfsConfig_unlocked(
                    MySpace.DataMining.AELight.dfs.DFSXMLNAME);

            if (null == dc.FileDaemon || !dc.FileDaemon.Enabled)
            {
                dc = null;
                _ThreadDisabled();
                return;
            }

            for (; ; System.Threading.Thread.Sleep(dc.FileDaemon.RepairSleep))
            {
                List<RepairItem> repairs = new List<RepairItem>(dc.FileDaemon.MaxRepairs);
                {
                    List<RepairItem> allrepairs = new List<RepairItem>(200);
                    foreach (string host in dc.Slaves.SlaveList.Split(';'))
                    {
                        try
                        {
                            string netpath = MySpace.DataMining.AELight.Surrogate.NetworkPathForHost(host);
                            string fp = netpath + @"\filescan.log";
                            string[] repairfiles = System.IO.File.ReadAllLines(fp);
                            try
                            {
                                System.IO.File.Delete(fp);
                            }
                            catch
                            {
                            }
                            foreach (string rf in repairfiles)
                            {
                                int i = rf.IndexOf("] ");
                                if (-1 != i)
                                {
                                    string s = rf.Substring(i + 2);
                                    i = s.IndexOf(' ');
                                    string rfhost = s.Substring(0, i);
                                    s = s.Substring(i + 1);
                                    i = s.IndexOf(' ');
                                    string spos = s.Substring(0, i);
                                    s = s.Substring(i + 1);
                                    string chunkname = s.Trim('"');
                                    RepairItem ri = new RepairItem();
                                    ri.chunkname = chunkname;
                                    ri.host = rfhost;
                                    try
                                    {
                                        ri.filepos = int.Parse(spos); // spos might be "<done>"; if so, don't add it.
                                        allrepairs.Add(ri);
                                    }
                                    catch
                                    {
                                    }
                                }
                            }
                        }
                        catch
                        {
                        }
                    }

                    allrepairs.Sort(new Comparison<RepairItem>(
                        delegate(RepairItem ri1, RepairItem ri2)
                        {
                            return string.Compare(ri1.chunkname, ri2.chunkname, true);
                        }));

                    for (int i = 1; i < allrepairs.Count; i++)
                    {
                        if (0 == string.Compare(allrepairs[i - 1].chunkname, allrepairs[i].chunkname, true))
                        {
                            allrepairs[i].hits += allrepairs[i - 1].hits;
                            allrepairs[i - 1].hits = 0;
                        }
                    }

                    allrepairs.Sort(new Comparison<RepairItem>(
                        delegate(RepairItem ri1, RepairItem ri2)
                        {
                            return ri2.hits - ri1.hits;
                        }));

                    for (int i = 0; i < 10 && i < allrepairs.Count && allrepairs[i].hits > 0; i++)
                    {
                        repairs.Add(allrepairs[i]);
                    }

                }

                foreach (RepairItem ri in repairs)
                {
                    bool found = false;
                    foreach (MySpace.DataMining.AELight.dfs.DfsFile df in dc.Files)
                    {
                        foreach (MySpace.DataMining.AELight.dfs.DfsFile.FileNode fn in df.Nodes)
                        {
                            if (0 == string.Compare(fn.Name, ri.chunkname, true))
                            {
                                found = true;
                                string state = "no attempt";
                                string newhost = "N/A";
                                string[] ahosts = fn.Host.Split(';');
                                if (ahosts.Length < 2
                                    || ahosts.Length < dc.Replication)
                                {
                                    //state = "no attempt: file replicate count is " + ahosts.Length
                                    //    + "; cluster replication factor is " + dc.Replication;
                                    break;
                                }
                                else
                                {
                                    foreach (string ahost in ahosts)
                                    {
                                        if (0 != string.Compare(ahost, ri.host, true))
                                        {
                                            try
                                            {
                                                string newchunkname = "zd.fdrepair." + Guid.NewGuid() + ".zd";
                                                string newchunkpath = MySpace.DataMining.AELight.Surrogate.NetworkPathForHost(ri.host)
                                                    + @"\" + newchunkname;
                                                System.IO.File.Copy(
                                                    MySpace.DataMining.AELight.Surrogate.NetworkPathForHost(ahost) + @"\" + ri.chunkname,
                                                    newchunkpath
                                                    );
                                                string chunkpath = ri.chunkpath;
                                                System.IO.File.Delete(chunkpath);
                                                System.IO.File.Move(newchunkpath, chunkpath);
                                                state = "success";
                                                newhost = ahost;
                                                break;
                                            }
                                            catch (Exception e)
                                            {
                                                state = "failure: " + e.ToString();
                                            }
                                        }
                                    }
                                }
                                for (int itries = 0; itries < 5; itries++, System.Threading.Thread.Sleep(1000))
                                {
                                    try
                                    {
                                        System.IO.File.AppendAllText("filerepairlog.txt",
                                            "[" + DateTime.Now + "] Replacing " + ri.chunkpath
                                            + " with chunk from " + newhost
                                            + ": " + state
                                            + Environment.NewLine);
                                        break;
                                    }
                                    catch
                                    {
                                    }
                                }
                                break;
                            }
                        }
                        if (found)
                        {
                            break;
                        }
                    }
                }

            }

        }


        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        internal static void _ScanChunkSleep(int ms)
        {
            System.Threading.Thread.Sleep(ms);
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        internal static void _ThreadDisabled()
        {
            System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
        }


        internal class RepairItem
        {
            internal string chunkname;
            internal string host;
            internal int filepos = -2;
            internal int hits = 1;

            internal string chunkpath
            {
                get
                {
                    return MySpace.DataMining.AELight.Surrogate.NetworkPathForHost(host) + @"\" + chunkname;
                }
            }
        }


    }
}
