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
using System.Linq;
using System.Text;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {
        class RemoteBlockInfo
        {
            internal string logname;
            internal SourceCode.Job cfgj;
            internal string DFSWriter; // Note: can be blank for no desired output.
            internal List<string> DFSWriters;

            internal System.Threading.Thread thread;
            internal List<string> dfsinputpaths;
            internal List<string> dfsinputfilenames;
            internal List<int> dfsinputnodesoffsets;
            internal bool verbose;
            internal MySpace.DataMining.DistributedObjects5.Remote rem;
            internal long baseoutputfilesize = -1;
            internal int blockcount = -1;
            internal List<string> outputdfsnodes;
            internal List<List<string>> outputdfsnodeses;
            internal IList<string> outputdfsdirs;
            internal IList<string> slaves;
            internal bool explicithost;
            internal List<long> outputsizes;
            internal List<List<long>> outputsizeses;
            internal long sampledist;
            internal bool blockfail = true;

            internal int BlockID;
            internal string SlaveHost;
            internal string SlaveIP;
            internal string[] ExecArgs;
            internal string Meta;


            internal void threadproc()
            {
                try
                {
                    string reason;
                    if (!Surrogate.IsHealthySlaveMachine(SlaveHost, out reason))
                    {
                        if (explicithost)
                        {
                            throw new Exception("Remote cannot connect to explicit host '" + SlaveHost + "': " + reason);
                        }
                        /*if (!failoverenabled)
                        {
                            throw new Exception("Remote cannot connect to host '" + SlaveHost + "': " + reason);
                        }
                        else*/
                        {
                            SlaveHost = null;
                            int startsi = (new Random(DateTime.Now.Millisecond / (BlockID + 2))).Next() % slaves.Count;
                            for (int i = startsi; i < slaves.Count; i++)
                            {
                                if (Surrogate.IsHealthySlaveMachine(slaves[i], out reason))
                                {
                                    SlaveHost = slaves[i];
                                    rem.OutputStartingPoint = i;
                                    break;
                                }
                            }
                            if (null == SlaveHost)
                            {
                                for (int i = 0; i < startsi; i++)
                                {
                                    if (Surrogate.IsHealthySlaveMachine(slaves[i], out reason))
                                    {
                                        SlaveHost = slaves[i];
                                        rem.OutputStartingPoint = i;
                                        break;
                                    }
                                }
                                if (null == SlaveHost)
                                {
                                    throw new Exception("Remote cannot connect to any hosts; last reason: " + reason);
                                }
                            }
                        }
                    }
                    SlaveIP = IPAddressUtil.GetIPv4Address(SlaveHost); // !

                    rem.SetJID(jid);
                    rem.AddBlock(SlaveIP + @"|" + logname + @"|slaveid=0");

                    string codectx = (@"
    public const int DSpace_BlockID = " + BlockID.ToString() + @";
    public const int DSpace_ProcessID = DSpace_BlockID;
    public const int Qizmt_ProcessID = DSpace_ProcessID;

    public const int DSpace_BlocksTotalCount = " + blockcount.ToString() + @";
    public const int DSpace_ProcessCount = DSpace_BlocksTotalCount;
    public const int Qizmt_ProcessCount = DSpace_ProcessCount;

    public const string DSpace_SlaveHost = `" + SlaveHost + @"`;
    public const string DSpace_MachineHost = DSpace_SlaveHost;
    public const string Qizmt_MachineHost = DSpace_MachineHost;

    public const string DSpace_SlaveIP = `" + SlaveIP + @"`;
    public const string DSpace_MachineIP = DSpace_SlaveIP;
    public const string Qizmt_MachineIP = DSpace_MachineIP;

    public static readonly string[] DSpace_ExecArgs = new string[] { " + ExecArgsCode(ExecArgs) + @" };
    public static readonly string[] Qizmt_ExecArgs = DSpace_ExecArgs;
    
    public const string DSpace_OutputFilePath = `" + DFSWriter + @"`; // Includes `dfs://` if in DFS.
    public const string Qizmt_OutputFilePath = DSpace_OutputFilePath;

    public static int DSpace_InputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength; } }
    public static int Qizmt_InputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength; } }

    public static int DSpace_OutputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength; } }
    public static int Qizmt_OutputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength; } }

    public const string Qizmt_Meta = @`" + Meta + @"`;

const string _userlogname = `" + logname + @"`;
static System.Threading.Mutex _logmutex = new System.Threading.Mutex(false, `distobjlog`);

    static string Shell(string line, bool suppresserrors)
    {
        return MySpace.DataMining.DistributedObjects.Exec.Shell(line, suppresserrors);
    }


    static string Shell(string line)
    {
        return MySpace.DataMining.DistributedObjects.Exec.Shell(line, false);
    }

private static int userlogsremain = " + AELight.maxuserlogs.ToString() + @";
public static void Qizmt_Log(string line) { DSpace_Log(line); }
public static void DSpace_Log(string line)
{
    if(--userlogsremain < 0)
    {
        return;
    }
    try
    {
        _logmutex.WaitOne();
    }
    catch (System.Threading.AbandonedMutexException)
    {
    }
    try
    {
        using (System.IO.StreamWriter fstm = System.IO.File.AppendText(_userlogname))
        {
            fstm.WriteLine(`{0}`, line);
        }
    }
    finally
    {
        _logmutex.ReleaseMutex();
    }
}

public static void Qizmt_LogResult(string line, bool passed) { DSpace_LogResult(line, passed); }
public static void DSpace_LogResult(string name, bool passed)
{
    if(passed)
    {
        DSpace_Log(`[\u00012PASSED\u00010] - ` + name);
    }
    else
    {
        DSpace_Log(`[\u00014FAILED\u00010] - ` + name);
    }
}

").Replace('`', '"') + CommonDynamicCsCode;


                    List<string> outputbasenames = new List<string>(DFSWriters.Count);                        
                    foreach (string writer in DFSWriters)
                    {
                        string dfsname = writer;
                        if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                        {
                            dfsname = dfsname.Substring(6);
                        }
                        outputbasenames.Add(GenerateZdFileDataNodeBaseName(dfsname));
                    }
                    
                    rem.Open();

                    outputsizes = new List<long>();
                    outputsizeses = new List<List<long>>();
                    outputdfsnodeses = new List<List<string>>();
                    rem.RemoteExec(dfsinputpaths, outputdfsdirs, outputbasenames, baseoutputfilesize, codectx + cfgj.Remote, cfgj.Usings, outputsizeses, outputdfsnodeses, dfsinputfilenames, dfsinputnodesoffsets);

                    blockfail = false; // !

                    if (verbose)
                    {
                        Console.Write('*');
                        ConsoleFlush();
                    }
                }
                catch (Exception e)
                {
                    LogOutput("RemoteBlockInfo.threadproc exception: " + e.ToString());
                }
            }

        }


        public static void ExecOneRemote(SourceCode.Job cfgj, string[] ExecArgs, bool verbose)
        {
            ExecOneRemote(cfgj, ExecArgs, verbose, verbose);
        }

        public static void ExecOneRemote(SourceCode.Job cfgj, string[] ExecArgs, bool verbose, bool verbosereplication)
        {
            if (verbose)
            {
                Console.WriteLine("[{0}]        [Remote: {2}]", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, cfgj.NarrativeName);
            }

            string logname = Surrogate.SafeTextPath(cfgj.NarrativeName) + "_" + Guid.NewGuid().ToString() + ".j" + sjid + "_log.txt";

            //System.Threading.Thread.Sleep(8000);
            /*if (cfgj.IOSettings.DFS_IOs == null || cfgj.IOSettings.DFS_IOs.Length == 0)
            {
                Console.Error.WriteLine("One or more IOSettings/DFS_IO needed in configuration for 'remote'");
                return;
            }*/

            // Could provide BlockID here, which is just the n-th DFS_IO entry.
            //cfgj.Remote

            dfs dc = LoadDfsConfig();

            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
            if (dc.Slaves.SlaveList.Length == 0 || slaves.Length < 1)
            {
                throw new Exception("SlaveList expected in " + dfs.DFSXMLNAME);
            }
            if(dc.Replication > 1)
            {
                string[] slavesbefore = slaves;
                slaves = ExcludeUnhealthySlaveMachines(slaves, true).ToArray();
                if (slavesbefore.Length - slaves.Length >= dc.Replication)
                {
                    throw new Exception("Not enough healthy machines to run job (hit replication count)");
                }
            }

            if (cfgj.IOSettings.DFS_IO_Multis != null)
            {
                cfgj.ExpandDFSIOMultis(slaves.Length, MySpace.DataMining.DistributedObjects.MemoryUtils.NumberOfProcessors);                
            }

            Dictionary<string, int> slaveIDs = new Dictionary<string, int>();
            for (int si = 0; si < slaves.Length; si++)
            {
                slaveIDs.Add(slaves[si].ToUpper(), si);
            }

            try
            {
                List<RemoteBlockInfo> blocks = new List<RemoteBlockInfo>(cfgj.IOSettings.DFS_IOs.Length);
                if (verbose)
                {
                    Console.WriteLine("{0} processes on {1} machines:", cfgj.IOSettings.DFS_IOs.Length, slaves.Length);
                }

                List<string> outputdfsdirs = new List<string>(slaves.Length);
                {
                    for (int i = 0; i < slaves.Length; i++)
                    {
                        try
                        {
                            outputdfsdirs.Add(NetworkPathForHost(slaves[i]));
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine("    {0}", e.Message);
                        }
                    }
                }

                string slaveconfigxml = "";
                {
                    System.Xml.XmlDocument pdoc = new System.Xml.XmlDocument();
                    {
                        System.IO.MemoryStream ms = new System.IO.MemoryStream();
                        System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(dfs));
                        xs.Serialize(ms, dc);
                        ms.Seek(0, System.IO.SeekOrigin.Begin);
                        pdoc.Load(ms);
                    }
                    string xml = pdoc.DocumentElement.SelectSingleNode("./slave").OuterXml;
                    //System.Threading.Thread.Sleep(8000);
                    slaveconfigxml = xml;
                }
                {
                    // Temporary:
                    for (int si = 0; si < slaves.Length; si++)
                    {
                        System.Threading.Mutex m = new System.Threading.Mutex(false, "AEL_SC_" + slaves[si]);
                        try
                        {
                            m.WaitOne();
                        }
                        catch (System.Threading.AbandonedMutexException)
                        {
                        }
                        try
                        {
                            System.IO.File.WriteAllText(NetworkPathForHost(slaves[si]) + @"\slaveconfig.j" + sjid + ".xml", slaveconfigxml);
                        }
                        catch
                        {
                        }
                        finally
                        {
                            m.ReleaseMutex();
                            m.Close();
                        }
                    }
                }

                int nextslave = (new Random(DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2)).Next() % slaves.Length;
                int hosttypes = 0;
                List<int> outputrecordlengths = new List<int>();
                List<int> inputrecordlengths = new List<int>();
                for (int BlockID = 0; BlockID < cfgj.IOSettings.DFS_IOs.Length; BlockID++)
                {
                    int slaveHostID = 0;
                    RemoteBlockInfo bi = new RemoteBlockInfo();
                    bi.sampledist = dc.DataNodeBaseSize / dc.DataNodeSamples;
                    bi.BlockID = BlockID;
                    bi.blockcount = cfgj.IOSettings.DFS_IOs.Length;
                    if (string.IsNullOrEmpty(cfgj.IOSettings.DFS_IOs[BlockID].Host))
                    {
                        if (0 != hosttypes && 1 != hosttypes)
                        {
                            throw new Exception("DFS_IO/Host tag must be specified for all or none");
                        }
                        hosttypes = 1;
                        bi.SlaveHost = slaves[nextslave];
                        slaveHostID = nextslave;
                        bi.explicithost = false;
                    }
                    else
                    {
                        if (0 != hosttypes && 2 != hosttypes)
                        {
                            throw new Exception("DFS_IO/Host tag must be specified for all or none");
                        }
                        hosttypes = 2;
                        bi.SlaveHost = cfgj.IOSettings.DFS_IOs[BlockID].Host;
                        slaveHostID = slaveIDs[bi.SlaveHost.ToUpper()];
                        bi.explicithost = true;
                    }
                    bi.ExecArgs = ExecArgs;
                    if (++nextslave >= slaves.Length)
                    {
                        nextslave = 0;
                    }

                    bi.logname = logname;
                    bi.outputdfsdirs = outputdfsdirs;
                    bi.slaves = slaves;
                    bi.baseoutputfilesize = dc.DataNodeBaseSize;
                    bi.cfgj = cfgj;
                    bi.DFSWriter = cfgj.IOSettings.DFS_IOs[BlockID].DFSWriter.Trim();
                    bi.Meta = cfgj.IOSettings.DFS_IOs[BlockID].Meta;

                    List<string> dfswriters = new List<string>();                    
                    if (bi.DFSWriter.Length > 0)
                    {
                        string[] writers = bi.DFSWriter.Split(';');
                        for (int wi = 0; wi < writers.Length; wi++)
                        {
                            string thiswriter = writers[wi].Trim();
                            if (thiswriter.Length == 0)
                            {
                                continue;
                            }
                            int ic = thiswriter.IndexOf('@');
                            int reclen = -1;
                            if (-1 != ic)
                            {
                                try
                                {
                                    reclen = Surrogate.GetRecordSize(thiswriter.Substring(ic + 1));
                                    thiswriter = thiswriter.Substring(0, ic);
                                }
                                catch (FormatException e)
                                {
                                    Console.Error.WriteLine("Error: remote output record length error: {0} ({1})", thiswriter, e.Message);
                                    SetFailure();
                                    return;
                                }
                                catch (OverflowException e)
                                {
                                    Console.Error.WriteLine("Error: remote output record length error: {0} ({1})", thiswriter, e.Message);
                                    SetFailure();
                                    return;
                                }
                            }
                            string outfn = thiswriter;
                            if (outfn.StartsWith(@"dfs://", StringComparison.OrdinalIgnoreCase))
                            {
                                outfn = outfn.Substring(6);
                            }
                            string reason = "";
                            if (dfs.IsBadFilename(outfn, out reason))
                            {
                                Console.Error.WriteLine("Invalid output file: {0}", reason);
                                return;
                            }
                            if (null != DfsFindAny(dc, outfn))
                            {
                                Console.Error.WriteLine("Error:  output file already exists in DFS: {0}", outfn);
                                return;
                            }
                            dfswriters.Add(thiswriter);
                            outputrecordlengths.Add(reclen);
                        }
                    }
                    else
                    {
                        dfswriters.Add("");
                        outputrecordlengths.Add(-1);
                    }
                    bi.DFSWriters = dfswriters;                   
                    bi.verbose = verbose;
                    bi.rem = new MySpace.DataMining.DistributedObjects5.Remote(cfgj.NarrativeName + "_remote");
                    bi.rem.CookRetries = dc.slave.CookRetries;
                    bi.rem.CookTimeout = dc.slave.CookTimeout;
                    bi.rem.DfsSampleDistance = bi.sampledist;
                    bi.rem.CompressFileOutput = dc.slave.CompressDfsChunks;
                    bi.rem.LocalCompile = true;
                    bi.rem.OutputStartingPoint = slaveHostID;
                    bi.rem.CompilerOptions = cfgj.IOSettings.CompilerOptions;
                    bi.rem.CompilerVersion = cfgj.IOSettings.CompilerVersion;
                    if (cfgj.AssemblyReferencesCount > 0)
                    {
                        cfgj.AddAssemblyReferences(bi.rem.CompilerAssemblyReferences, Surrogate.NetworkPathForHost(dc.Slaves.GetFirstSlave()));
                    }
                    if (cfgj.OpenCVExtension != null)
                    {
                        bi.rem.AddOpenCVExtension();
                    }
                    if (cfgj.Unsafe != null)
                    {
                        bi.rem.AddUnsafe();
                    }                    
                    {
                        List<dfs.DfsFile.FileNode> nodes = new List<dfs.DfsFile.FileNode>();                        
                        List<string> mapfileswithnodes = null;
                        List<int> nodesoffsets = null;
                        IList<string> mapfiles = SplitInputPaths(dc, cfgj.IOSettings.DFS_IOs[BlockID].DFSReader);
                        if (mapfiles.Count > 0)
                        {
                            mapfileswithnodes = new List<string>(mapfiles.Count);
                            nodesoffsets = new List<int>(mapfiles.Count);
                        }
                        for (int i = 0; i < mapfiles.Count; i++)
                        {
                            string dp = mapfiles[i].Trim();
                            int inreclen = -1;
                            if (0 != dp.Length) // Allow empty entry where input isn't wanted.
                            {
                                if (dp.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                {
                                    dp = dp.Substring(6);
                                }

                                {
                                    int ic = dp.IndexOf('@');
                                    if (-1 != ic)
                                    {
                                        try
                                        {
                                            inreclen = Surrogate.GetRecordSize(dp.Substring(ic + 1));
                                            dp = dp.Substring(0, ic);
                                        }
                                        catch (FormatException e)
                                        {
                                            Console.Error.WriteLine("Error: remote input record length error: {0} ({1})", dp, e.Message);
                                            SetFailure();
                                            return;
                                        }
                                        catch (OverflowException e)
                                        {
                                            Console.Error.WriteLine("Error: remote input record length error: {0} ({1})", dp, e.Message);
                                            SetFailure();
                                            return;
                                        }
                                    }
                                }
                                dfs.DfsFile df;
                                if (inreclen > 0)
                                {
                                    df = DfsFind(dc, dp, DfsFileTypes.BINARY_RECT);
                                    if (null != df && inreclen != df.RecordLength)
                                    {
                                        Console.Error.WriteLine("Error: remote input file does not have expected record length of {0}: {1}@{2}", inreclen, dp, df.RecordLength);
                                        SetFailure();
                                        return;
                                    }
                                }
                                else
                                {
                                    df = DfsFind(dc, dp);
                                }
                                if (null == df)
                                {
                                    //throw new Exception("Remote input file not found in DFS: " + dp);
                                    Console.Error.WriteLine("Remote input file not found in DFS: {0}", dp);
                                    return;
                                }
                                if (df.Nodes.Count > 0)
                                {
                                    mapfileswithnodes.Add(dp);
                                    nodesoffsets.Add(nodes.Count);
                                    inputrecordlengths.Add(inreclen);
                                    nodes.AddRange(df.Nodes);
                                }                                
                                
                            }
                        }
                        bi.dfsinputpaths = new List<string>(nodes.Count);
                        //MapNodesToNetworkPaths(nodes, bi.dfsinputpaths);
                        dfs.MapNodesToNetworkStarPaths(nodes, bi.dfsinputpaths);
                        bi.dfsinputfilenames = mapfileswithnodes;
                        bi.dfsinputnodesoffsets = nodesoffsets;
                    }

                    blocks.Add(bi);

                    bi.thread = new System.Threading.Thread(new System.Threading.ThreadStart(bi.threadproc));
                    bi.thread.Name = "RemoteJobBlock" + bi.BlockID;
                }
                MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength = inputrecordlengths.Count > 0 ? inputrecordlengths[0] : -1;
                MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength = outputrecordlengths.Count > 0 ? outputrecordlengths[0] : -1;
                // Need to start threads separately due to StaticGlobals being updated.
                for (int BlockID = 0; BlockID < cfgj.IOSettings.DFS_IOs.Length; BlockID++)
                {
                    RemoteBlockInfo bi = blocks[BlockID];
                    bi.rem.InputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                    bi.rem.InputRecordLengths = inputrecordlengths;
                    bi.rem.OutputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength;
                    bi.rem.OutputRecordLengths = outputrecordlengths;
                    AELight_StartTraceThread(bi.thread);
                }

                for (int BlockID = 0; BlockID < blocks.Count; BlockID++)
                {
                    AELight_JoinTraceThread(blocks[BlockID].thread);
                    blocks[BlockID].rem.Close();

                    if (blocks[BlockID].blockfail)
                    {
                        Console.Error.WriteLine("BlockID {0} on host '{1}' did not complete successfully", BlockID, (blocks[BlockID].SlaveHost != null) ? blocks[BlockID].SlaveHost : "<null>");
                        continue;
                    }
                }

                List<string> dfsnames = new List<string>();
                List<string> dfsnamesreplicating = new List<string>();
                // Reload DFS config to make sure changes since starting get rolled in, and make sure the output file wasn't created in that time...
                using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                {
                    dc = LoadDfsConfig();
                    for (int BlockID = 0; BlockID < blocks.Count; BlockID++)
                    {
                        if (blocks[BlockID].blockfail)
                        {
                            continue;
                        }
                        {
                            bool anyoutput = false;
                            bool nonemptyoutputpath = false;
                            for (int oi = 0; oi < blocks[BlockID].DFSWriters.Count; oi++)
                            {
                                string dfswriter = blocks[BlockID].DFSWriters[oi];

                                if (string.IsNullOrEmpty(dfswriter))
                                {
                                    if (blocks[BlockID].outputdfsnodeses[oi].Count > 0)
                                    {
                                        Console.Error.WriteLine("Output data detected with no DFSWriter specified");
                                    }
                                }
                                else
                                {
                                    {
                                        if (null != DfsFind(dc, dfswriter))
                                        {
                                            Console.Error.WriteLine("Error:  output file was created during job: {0}", dfswriter);
                                            continue;
                                        }

                                        string dfspath = dfswriter;

                                        {
                                            nonemptyoutputpath = true;
                                            dfs.DfsFile df = new dfs.DfsFile();
                                            if (blocks[BlockID].rem.OutputRecordLengths[oi] > 0)
                                            {
                                                df.XFileType = DfsFileTypes.BINARY_RECT + "@" + blocks[BlockID].rem.OutputRecordLengths[oi].ToString();
                                            }
                                            df.Nodes = new List<dfs.DfsFile.FileNode>();
                                            df.Size = -1; // Preset
                                            if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                            {
                                                dfspath = dfspath.Substring(6);
                                            }
                                            string dfspathreplicating = ".$" + dfspath + ".$replicating-" + Guid.NewGuid().ToString();
                                            if (null != dc.FindAny(dfspathreplicating))
                                            {
                                                Console.Error.WriteLine("Error: file exists: file put into DFS from another location during job: " + dfspathreplicating);
                                                SetFailure();
                                                return;
                                            }
                                            dfsnames.Add(dfspath);
                                            dfsnamesreplicating.Add(dfspathreplicating);
                                            df.Name = dfspathreplicating;
                                            bool anybad = false;
                                            long totalsize = 0;
                                            {
                                                int i = BlockID;
                                                for (int j = 0; j < blocks[i].outputdfsnodeses[oi].Count; j++)
                                                {
                                                    dfs.DfsFile.FileNode fn = new dfs.DfsFile.FileNode();
                                                    fn.Host = blocks[i].slaves[(blocks[i].rem.OutputStartingPoint + j) % blocks[i].slaves.Count];
                                                    fn.Name = blocks[i].outputdfsnodeses[oi][j];
                                                    df.Nodes.Add(fn);
                                                    fn.Length = -1; // Preset
                                                    fn.Position = -1; // Preset
                                                    if (anybad)
                                                    {
                                                        continue;
                                                    }
                                                    fn.Length = blocks[i].outputsizeses[oi][j];
                                                    fn.Position = totalsize; // Position must be set before totalsize updated!
                                                    if (blocks[i].outputdfsnodeses[oi].Count != blocks[i].outputsizeses[oi].Count)
                                                    {
                                                        anybad = true;
                                                        continue;
                                                    }
                                                    totalsize += blocks[i].outputsizeses[oi][j];
                                                }
                                            }
                                            if (!anybad)
                                            {
                                                df.Size = totalsize;
                                            }
                                            if (totalsize != 0)
                                            {
                                                anyoutput = true;
                                            }
                                            // Always add the file to DFS, even if blank!
                                            dc.Files.Add(df);
                                        }
                                    }
                                }
                            }
                            if (!anyoutput && verbose && nonemptyoutputpath)
                            {
                                Console.Write(" (no DFS output) ");
                                ConsoleFlush();
                            }
                        }
                    }

                    UpdateDfsXml(dc);

                }

                ReplicationPhase(verbosereplication, blocks.Count, slaves, dfsnamesreplicating);

                using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                {
                    dc = LoadDfsConfig(); // Reload in case of change or user modifications.
                    for (int nfile = 0; nfile < dfsnames.Count; nfile++)
                    {
                        string dfspath = dfsnames[nfile];
                        string dfspathreplicating = dfsnamesreplicating[nfile];
                        {
                            dfs.DfsFile dfu = dc.FindAny(dfspathreplicating);
                            if (null != dfu)
                            {
                                if (null != DfsFindAny(dc, dfspath))
                                {
                                    Console.Error.WriteLine("Error: file exists: file put into DFS from another location during job");
                                    SetFailure();
                                    continue;
                                }
                                dfu.Name = dfspath;
                            }
                        }
                    }
                    UpdateDfsXml(dc);
                }

                if (verbose)
                {
                    Console.WriteLine(); // Line after output chars.
                }

            }
            finally
            {
                {
                    for (int si = 0; si < slaves.Length; si++)
                    {
                        System.Threading.Mutex m = new System.Threading.Mutex(false, "AEL_SC_" + slaves[si]);
                        try
                        {
                            m.WaitOne();
                        }
                        catch (System.Threading.AbandonedMutexException)
                        {
                        }
                        try
                        {
                            System.IO.File.Delete(NetworkPathForHost(slaves[si]) + @"\slaveconfig.j" + sjid + ".xml");
                        }
                        catch
                        {
                        }
                        finally
                        {
                            m.ReleaseMutex();
                            m.Close();
                        }
                    }
                }

                CheckUserLogs(slaves, logname);
            }

            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine("[{0}]        Done", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
                for (int i = 0; i < cfgj.IOSettings.DFS_IOs.Length; i++)
                {
                    Console.WriteLine("Output:   {0}", cfgj.IOSettings.DFS_IOs[i].DFSWriter);
                }
            }

        }
    }
}
