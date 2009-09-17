﻿/**************************************************************************************
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

            internal System.Threading.Thread thread;
            internal List<string> dfsinputpaths;
            internal bool verbose;
            internal MySpace.DataMining.DistributedObjects5.Remote rem;
            internal long baseoutputfilesize = -1;
            internal int blockcount = -1;
            internal List<string> outputdfsnodes;
            internal IList<string> outputdfsdirs;
            internal IList<string> slaves;
            internal bool explicithost;
            internal List<long> outputsizes;
            internal long sampledist;
            internal bool blockfail = true;

            internal int BlockID;
            internal string SlaveHost;
            internal string SlaveIP;
            internal string[] ExecArgs;


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

    public const int DSpace_InputRecordLength = " + MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength.ToString() + @";
    public const int Qizmt_InputRecordLength = DSpace_InputRecordLength;

    public const int DSpace_OutputRecordLength = " + MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength.ToString() + @";
    public const int Qizmt_OutputRecordLength = DSpace_OutputRecordLength;

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
    _logmutex.WaitOne();
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


                    string dfsname = DFSWriter;
                    if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        dfsname = dfsname.Substring(6);
                    }
                    string outputbasename = GenerateZdFileDataNodeBaseName(dfsname);

                    rem.Open();

                    outputsizes = new List<long>();
                    int noutfiles = rem.RemoteExec(dfsinputpaths, outputdfsdirs, outputbasename, baseoutputfilesize, codectx + cfgj.Remote, cfgj.Usings, outputsizes);

                    outputdfsnodes = new List<string>(noutfiles);
                    for (int ne = 0; ne < noutfiles; ne++)
                    {
                        outputdfsnodes.Add(outputbasename.Replace("%n", ne.ToString()));
                    }

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

            string logname = Surrogate.SafeTextPath(cfgj.NarrativeName) + "_" + Guid.NewGuid().ToString() + "_log.txt";

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
                        m.WaitOne();
                        try
                        {
                            System.IO.File.WriteAllText(NetworkPathForHost(slaves[si]) + @"\slaveconfig.xml", slaveconfigxml);
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
                int InputRecordLength = int.MinValue;
                int OutputRecordLength = int.MinValue;
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
                    {
                        int ic = bi.DFSWriter.IndexOf('@');
                        if (-1 != ic)
                        {
                            try
                            {
                                int reclen = Surrogate.GetRecordSize(bi.DFSWriter.Substring(ic + 1));
                                if (OutputRecordLength != int.MinValue && OutputRecordLength == reclen)
                                {
                                    Console.Error.WriteLine("Error: all remote outputs must have the same record length: {0}", bi.DFSWriter);
                                    SetFailure();
                                    return;
                                }
                                OutputRecordLength = reclen;
                                bi.DFSWriter = bi.DFSWriter.Substring(0, ic);
                            }
                            catch (FormatException e)
                            {
                                Console.Error.WriteLine("Error: remote output record length error: {0} ({1})", bi.DFSWriter, e.Message);
                                SetFailure();
                                return;
                            }
                            catch (OverflowException e)
                            {
                                Console.Error.WriteLine("Error: remote output record length error: {0} ({1})", bi.DFSWriter, e.Message);
                                SetFailure();
                                return;
                            }
                        }
                        else if(OutputRecordLength == int.MinValue)
                        {
                            OutputRecordLength = -1;
                        }
                    }
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
                        IList<string> mapfiles = SplitInputPaths(dc, cfgj.IOSettings.DFS_IOs[BlockID].DFSReader);
                        for (int i = 0; i < mapfiles.Count; i++)
                        {
                            string dp = mapfiles[i].Trim();
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
                                            int reclen = Surrogate.GetRecordSize(dp.Substring(ic + 1));
                                            if (InputRecordLength != int.MinValue && InputRecordLength == reclen)
                                            {
                                                Console.Error.WriteLine("Error: all remote inputs must have the same record length: {0}", dp);
                                                SetFailure();
                                                return;
                                            }
                                            InputRecordLength = reclen;
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
                                    else if(InputRecordLength == int.MinValue)
                                    {
                                        InputRecordLength = -1;
                                    }
                                }
                                dfs.DfsFile df;
                                if (InputRecordLength > 0)
                                {
                                    df = DfsFind(dc, dp, DfsFileTypes.BINARY_RECT);
                                    if (null != df && InputRecordLength != df.RecordLength)
                                    {
                                        Console.Error.WriteLine("Error: remote input file does not have expected record length of {0}: {1}@{2}", InputRecordLength, dp, df.RecordLength);
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
                                nodes.AddRange(df.Nodes);
                            }
                        }
                        bi.dfsinputpaths = new List<string>(nodes.Count);
                        //MapNodesToNetworkPaths(nodes, bi.dfsinputpaths);
                        dfs.MapNodesToNetworkStarPaths(nodes, bi.dfsinputpaths);

                        //if (0 != cfgj.IOSettings.DFS_IOs[BlockID].DFSWriter.Trim().Length) // Allow empty entry where output isn't wanted.
                        if (0 != bi.DFSWriter.Length) // Allow empty entry where output isn't wanted.
                        {
                            string outfn = bi.DFSWriter;
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
                            if (null != DfsFindAny(dc, cfgj.IOSettings.DFS_IOs[BlockID].DFSWriter))
                            {
                                Console.Error.WriteLine("Error:  output file already exists in DFS: {0}", cfgj.IOSettings.DFS_IOs[BlockID].DFSWriter);
                                return;
                            }
                        }
                    }

                    blocks.Add(bi);

                    bi.thread = new System.Threading.Thread(new System.Threading.ThreadStart(bi.threadproc));
                }
                MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength = InputRecordLength;
                MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength = OutputRecordLength;
                // Need to start threads separately due to StaticGlobals being updated.
                for (int BlockID = 0; BlockID < cfgj.IOSettings.DFS_IOs.Length; BlockID++)
                {
                    RemoteBlockInfo bi = blocks[BlockID];
                    bi.rem.InputRecordLength = InputRecordLength;
                    bi.rem.OutputRecordLength = OutputRecordLength;
                    bi.thread.Start();
                }

                for (int BlockID = 0; BlockID < blocks.Count; BlockID++)
                {
                    blocks[BlockID].thread.Join();
                    blocks[BlockID].rem.Close();

                    if (blocks[BlockID].blockfail)
                    {
                        Console.Error.WriteLine("BlockID {0} on host '{1}' did not complete successfully", BlockID, (blocks[BlockID].SlaveHost != null) ? blocks[BlockID].SlaveHost : "<null>");
                        continue;
                    }

                    {
                        string dfspath;
                        string dfspathreplicating;
                        // Reload DFS config to make sure changes since starting get rolled in, and make sure the output file wasn't created in that time...
                        using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                        {
                            dc = LoadDfsConfig();
                            if (null != DfsFind(dc, blocks[BlockID].DFSWriter))
                            {
                                Console.Error.WriteLine("Error:  output file was created during job");
                                return;
                            }
                            dfspath = blocks[BlockID].DFSWriter; // Init.
                            bool anyoutput = false;
                            if (string.IsNullOrEmpty(dfspath))
                            {
                                dfspathreplicating = null;
                                if (blocks[BlockID].outputdfsnodes.Count > 0)
                                {
                                    Console.Error.WriteLine("Output data detected with no DFSWriter specified");
                                }
                            }
                            else
                            {
                                dfs.DfsFile df = new dfs.DfsFile();
                                if (MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength > 0)
                                {
                                    df.XFileType = DfsFileTypes.BINARY_RECT + "@" + MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength.ToString();
                                }
                                df.Nodes = new List<dfs.DfsFile.FileNode>();
                                df.Size = -1; // Preset
                                if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                {
                                    dfspath = dfspath.Substring(6);
                                }
                                dfspathreplicating = ".$" + dfspath + ".$replicating-" + Guid.NewGuid().ToString();
                                if (null != dc.FindAny(dfspathreplicating))
                                {
                                    Console.Error.WriteLine("Error: file exists: file put into DFS from another location during job: " + dfspathreplicating);
                                    SetFailure();
                                    return;
                                }
                                df.Name = dfspathreplicating;
                                bool anybad = false;
                                long totalsize = 0;
                                {
                                    int i = BlockID;
                                    for (int j = 0; j < blocks[i].outputdfsnodes.Count; j++)
                                    {
                                        dfs.DfsFile.FileNode fn = new dfs.DfsFile.FileNode();
                                        fn.Host = blocks[i].slaves[(blocks[i].rem.OutputStartingPoint + j) % blocks[i].slaves.Count];
                                        fn.Name = blocks[i].outputdfsnodes[j];
                                        df.Nodes.Add(fn);
                                        fn.Length = -1; // Preset
                                        fn.Position = -1; // Preset
                                        if (anybad)
                                        {
                                            continue;
                                        }
                                        fn.Length = blocks[i].outputsizes[j];
                                        fn.Position = totalsize; // Position must be set before totalsize updated!
                                        if (blocks[i].outputdfsnodes.Count != blocks[i].outputsizes.Count)
                                        {
                                            anybad = true;
                                            continue;
                                        }
                                        totalsize += blocks[i].outputsizes[j];
                                    }
                                }
                                if (!anybad)
                                {
                                    df.Size = totalsize;
                                }
                                anyoutput = totalsize != 0;
                                // Always add the file to DFS, even if blank!
                                {
                                    dc.Files.Add(df);
                                    UpdateDfsXml(dc); // !
                                }
                            }
                            if (!anyoutput && verbose && (dfspath.Length != 0))
                            {
                                Console.Write(" (no DFS output) ");
                                ConsoleFlush();
                            }
                        }
                        if (verbose)
                        {
                            Console.WriteLine(); // Line after output chars.
                        }
                        if (null != dfspathreplicating) // If there was an output file specified...
                        {
                            ReplicationPhase(dfspathreplicating, verbosereplication, blocks.Count, slaves);
                            using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                            {
                                dc = LoadDfsConfig();
                                dfs.DfsFile dfu = dc.FindAny(dfspathreplicating);
                                if (null != dfu)
                                {
                                    if (null != DfsFindAny(dc, dfspath))
                                    {
                                        Console.Error.WriteLine("Error: file exists: file put into DFS from another location during job");
                                        SetFailure();
                                        return;
                                    }
                                    dfu.Name = dfspath;
                                    UpdateDfsXml(dc);
                                }
                            }
                        }

                    }
                }

            }
            finally
            {
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