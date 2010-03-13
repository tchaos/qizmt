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

#define CONN_BACKLOG_IPSINGLE

#define MR_EXCHANGE_TIME_PRINT
#define MR_REPLICATION_TIME_PRINT

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {
        internal class FailoverInfo
        {
            internal Dictionary<string, List<MapReduceBlockInfo>> hostToBlocks = null;
            internal MapReduceBlockInfo[] allBlocks = null;
            internal Dictionary<string, int> goodHosts = null;
            internal Dictionary<string, int> badHosts = null;
            internal Dictionary<int,int> blockStatus = null;
            internal List<string> newBadHosts = null;
            internal MySpace.DataMining.DistributedObjects.DiskCheck diskcheck = null;
            internal Random rnd = null;
            internal int sleepCnt = 0;
            internal string inputOrder = null;
            internal string[] healthpluginpaths = null;

            internal FailoverInfo(dfs dc, string inputorder)
            {
                healthpluginpaths = GetHealthPluginPaths(dc);
                diskcheck = new MySpace.DataMining.DistributedObjects.DiskCheck(healthpluginpaths);
                rnd = new Random(unchecked(DateTime.Now.Millisecond + System.Diagnostics.Process.GetCurrentProcess().Id));
                inputOrder = inputorder;
            }

            internal void CreateBlocks(string[] hosts, List<MapReduceBlockInfo> _blocks, int blockscount, int initialstatus, JobInfo jobinfo)
            {
                int allblockscount = blockscount * jobinfo.dc.Replication;
                hostToBlocks = new Dictionary<string, List<MapReduceBlockInfo>>(hosts.Length);
                allBlocks = new MapReduceBlockInfo[allblockscount];
                goodHosts = new Dictionary<string, int>(hosts.Length);
                blockStatus = new Dictionary<int, int>(allblockscount);
                badHosts = new Dictionary<string, int>(hosts.Length);
                newBadHosts = new List<string>(hosts.Length);

                foreach (string host in hosts)
                {
                    goodHosts.Add(host.ToLower(), 0);
                }

                int blocksperhost = allblockscount / hosts.Length;
                if ((blocksperhost * hosts.Length) != allblockscount)
                {
                    blocksperhost++;
                }
                        
                for (int ri = 0; ri < jobinfo.dc.Replication; ri++)
                {
                    for (int bi = 0; bi < blockscount; bi++)
                    {
                        MapReduceBlockInfo block = new MapReduceBlockInfo();
                        block.BlockID = bi;
                        block.BlockCID = ri * blockscount + block.BlockID;
                        block.failover = this;
                        allBlocks[block.BlockCID] = block;
                        blockStatus.Add(block.BlockCID, initialstatus);
                    }
                }
                
                MapReduceBlockInfo[] firstset = new MapReduceBlockInfo[blockscount];
                {
                    Dictionary<string, Dictionary<int, MapReduceBlockInfo>> hostToBlockIDs = new Dictionary<string, Dictionary<int, MapReduceBlockInfo>>(hosts.Length);
                    int nexthost = 0;
                    List<MapReduceBlockInfo> collisions = new List<MapReduceBlockInfo>(blockscount);
                    Dictionary<string, Dictionary<int, MapReduceBlockInfo>> hostToBlockIDsPerRep = new Dictionary<string, Dictionary<int, MapReduceBlockInfo>>(hosts.Length);

                    for (int ri = 0; ri < jobinfo.dc.Replication; ri++)
                    {
#if FAILOVER_DEBUG
                        Log("Assigning hosts to blocks in replication index = " + ri.ToString());
#endif

                        hostToBlockIDsPerRep.Clear();
                       
                        MapReduceBlockInfo[] scrambled = new MapReduceBlockInfo[blockscount]; //!
                        for (int bi = 0; bi < scrambled.Length; bi++)
                        {
                            scrambled[bi] = allBlocks[bi + ri * blockscount];
                        }
                        for (int bi = 0; bi < scrambled.Length; bi++)
                        {
                            int rndindex = rnd.Next() % scrambled.Length;
                            MapReduceBlockInfo oldvalue = scrambled[bi];
                            scrambled[bi] = scrambled[rndindex];
                            scrambled[rndindex] = oldvalue;
                        }
                       
                        if (ri == 0)
                        {
                            for (int bi = 0; bi < scrambled.Length; bi++)
                            {
                                firstset[bi] = scrambled[bi];
                            }
                        }
#if FAILOVER_DEBUG
                        {
                            string debugtxt = "firstset:" + Environment.NewLine;
                            foreach (MapReduceBlockInfo bl in firstset)
                            {
                                debugtxt += bl.BlockID.ToString() + ":" + bl.BlockCID.ToString() + ":" + (bl.SlaveHost == null ? "null" : bl.SlaveHost) + Environment.NewLine;
                            }
                            Log(debugtxt);
                        }
#endif
                        
                        int tryremains = blockscount;
                        for (; ; )
                        {
                            AssignBlocksToHosts(scrambled, hosts, ref nexthost, hostToBlockIDs, collisions, hostToBlockIDsPerRep, blocksperhost, blockscount);
                            if (collisions.Count == 0)
                            {
                                break;
                            }
                            if (--tryremains <= 0)
                            {
                                throw new Exception("Cannot resolve collisions.  Reached maximum number of tries.");
                            }
                            scrambled = collisions.ToArray();
                            collisions.Clear();
                        }
                    }

                    foreach (MapReduceBlockInfo block in allBlocks)
                    {
                        string host = block.SlaveHost.ToLower();
                        if (!hostToBlocks.ContainsKey(host))
                        {
                            hostToBlocks.Add(host, new List<MapReduceBlockInfo>(blocksperhost));
                        }
                        hostToBlocks[host].Add(block);
                    }
                }
                
                foreach (MapReduceBlockInfo block in allBlocks)
                {
                    block.jobshared = jobinfo.jobshared;
                    block.allinputsamples = jobinfo.allinputsamples;
                    block.extraverbose = jobinfo.extraverbose;
                    block.AddCacheOnly = false;
                    block.outputfiles = jobinfo.outputfiles;
                    block.outputfile = jobinfo.outputfile;
                    block.basefilesize = jobinfo.dc.DataNodeBaseSize;
                    block.cfgj = jobinfo.cfgj;
                    block.SlaveIP = IPAddressUtil.GetIPv4Address(block.SlaveHost);
                    block.ExecArgs = jobinfo.execargs;
                    block.logname = jobinfo.logname;
                    block.acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList(jobinfo.cfgj.NarrativeName + "_BlockID" + block.BlockID.ToString(), jobinfo.cfgj.IOSettings.KeyLength);
                    block.acl.SetJID(jid);
                    block.acl.HealthPluginPaths = healthpluginpaths;
                    int IntermediateDataAddressing = jobinfo.cfgj.IntermediateDataAddressing;
                    if (0 == IntermediateDataAddressing)
                    {
                        IntermediateDataAddressing = jobinfo.dc.IntermediateDataAddressing;
                    }
                    block.acl.ValueOffsetSize = IntermediateDataAddressing / 8;
                    if (block.acl.ValueOffsetSize <= 0)
                    {
                        throw new InvalidOperationException("Invalid value for IntermediateDataAddressing: " + IntermediateDataAddressing.ToString());
                    }
                    block.acl.InputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                    block.acl.OutputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength;
                    block.acl.OutputRecordLengths = jobinfo.outputrecordlengths;
                    block.acl.InputRecordLengths = new List<int>();
                    block.acl.CookRetries = jobinfo.dc.slave.CookRetries;
                    block.acl.CookTimeout = jobinfo.dc.slave.CookTimeout;
                    block.acl.LocalCompile = (0 == block.BlockID);
                    block.acl.BTreeCapSize = jobinfo.dc.BTreeCapSize;
                    MySpace.DataMining.DistributedObjects5.DistObject.FILE_BUFFER_SIZE = FILE_BUFFER_SIZE;
                    block.acl.atype = atype;
                    block.acl.DfsSampleDistance = jobinfo.dc.DataNodeBaseSize / jobinfo.dc.DataNodeSamples;
                    block.slaveconfigxml = jobinfo.slaveconfigxml;
                    block.acl.CompressFileOutput = jobinfo.dc.slave.CompressDfsChunks;
                    block.acl.ZMapBlockCount = blockscount;
                    block.verbose = jobinfo.verbose;
                    block.acl.CompilerOptions = jobinfo.cfgj.IOSettings.CompilerOptions;
                    block.acl.CompilerVersion = jobinfo.cfgj.IOSettings.CompilerVersion;
                    if (jobinfo.cfgj.AssemblyReferencesCount > 0)
                    {
                        jobinfo.cfgj.AddAssemblyReferences(block.acl.CompilerAssemblyReferences, Surrogate.NetworkPathForHost(block.SlaveHost));
                    }
                    if (jobinfo.cfgj.OpenCVExtension != null)
                    {
                        block.acl.AddOpenCVExtension();
                    }
                    if (jobinfo.cfgj.Unsafe != null)
                    {
                        block.acl.AddUnsafe();
                    }
                    block.gencodectx();
                    block.acl.AddBlock("1", "1", block.SlaveHost + @"|" + block.logname + @"|slaveid=0");
                    block.ownedzmapblockIDs.Add(block.BlockID);

                    if (block.BlockCID < blockscount)
                    {
                        _blocks.Add(block);
                    }
                }
                jobinfo.timethread.Start();

#if FAILOVER_DEBUG
                {
                    Log("jobinfo.mapinputchunks");
                    string debugtxt = "";
                    foreach (dfs.DfsFile.FileNode xx in jobinfo.mapinputchunks)
                    {
                        debugtxt += xx.Name + Environment.NewLine;
                    }
                    Log(debugtxt);  
                }
                {
                    Log("jobinfo.inputnodesoffsets");
                    string debugtxt = "";
                    foreach (int xx in jobinfo.inputnodesoffsets)
                    {
                        debugtxt += xx.ToString() + Environment.NewLine;
                    }
                    Log(debugtxt);
                }
                {
                    Log("jobinfo.mapfileswithnodes");
                    string debugtxt = "";
                    foreach (string xx in jobinfo.mapfileswithnodes)
                    {
                        debugtxt += xx + Environment.NewLine;
                    }
                    Log(debugtxt);
                }                
                {
                    Log("jobinfo.inputrecordlengths");
                    string debugtxt = "";
                    foreach (int xx in jobinfo.inputrecordlengths)
                    {
                        debugtxt += xx.ToString() + Environment.NewLine;
                    }
                    Log(debugtxt);
                }
#endif
                               
                {
                    List<dfs.DfsFile.FileNode> _mapinputchunks = null;
                    List<string> _mapfileswithnodes = null;
                    List<int> _inputnodesoffsets = null;
                    List<int> _inputrecordlengths = null;

                    if (string.Compare("next", inputOrder, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        _mapinputchunks = jobinfo.mapinputchunks;
                        _mapfileswithnodes = jobinfo.mapfileswithnodes;
                        _inputnodesoffsets = jobinfo.inputnodesoffsets;
                        _inputrecordlengths = jobinfo.inputrecordlengths;
                    }
                    else if (string.Compare("shuffle", inputOrder, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        _mapinputchunks = new List<dfs.DfsFile.FileNode>(jobinfo.mapinputchunks.Count);
                        _mapfileswithnodes = new List<string>(jobinfo.mapinputchunks.Count);
                        _inputnodesoffsets = new List<int>(jobinfo.mapinputchunks.Count);
                        _inputrecordlengths = new List<int>(jobinfo.mapinputchunks.Count);

                        for (int ci = 0; ci < jobinfo.mapinputchunks.Count; ci++)
                        {
                            _mapinputchunks.Add(jobinfo.mapinputchunks[ci]);
                            _inputnodesoffsets.Add(ci);
                        }

                        for (int oi = 0; oi < jobinfo.inputnodesoffsets.Count; oi++)
                        {
                            string fname = jobinfo.mapfileswithnodes[oi];
                            int reclen = jobinfo.inputrecordlengths[oi];
                            int expand = (oi == jobinfo.inputnodesoffsets.Count - 1 ? jobinfo.mapinputchunks.Count : jobinfo.inputnodesoffsets[oi + 1]);
                            expand = expand - _mapfileswithnodes.Count;

                            for (int ei = 0; ei < expand; ei++)
                            {
                                _mapfileswithnodes.Add(fname);
                                _inputrecordlengths.Add(reclen);
                            }
                        }
                        
                        for (int ci = 0; ci < _mapinputchunks.Count; ci++)
                        {
                            int rndindex = rnd.Next() % _mapinputchunks.Count;
                            dfs.DfsFile.FileNode oldchunk = _mapinputchunks[ci];
                            _mapinputchunks[ci] = _mapinputchunks[rndindex];
                            _mapinputchunks[rndindex] = oldchunk;

                            string oldfname = _mapfileswithnodes[ci];
                            _mapfileswithnodes[ci] = _mapfileswithnodes[rndindex];
                            _mapfileswithnodes[rndindex] = oldfname;

                            int oldreclen = _inputrecordlengths[ci];
                            _inputrecordlengths[ci] = _inputrecordlengths[rndindex];
                            _inputrecordlengths[rndindex] = oldreclen;
                        }
                    }
                    else
                    {
                        throw new Exception("Computing InputOrder is not valid");
                    }

#if FAILOVER_DEBUG
                    Log("Done shuffling; InputOrder=" + inputOrder);
                    {
                        Log("jobinfo.mapinputchunks");
                        string debugtxt = "";
                        foreach (dfs.DfsFile.FileNode xx in jobinfo.mapinputchunks)
                        {
                            debugtxt += xx.Name + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    {
                        Log("jobinfo.inputnodesoffsets");
                        string debugtxt = "";
                        foreach (int xx in jobinfo.inputnodesoffsets)
                        {
                            debugtxt += xx.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    {
                        Log("jobinfo.mapfileswithnodes");
                        string debugtxt = "";
                        foreach (string xx in jobinfo.mapfileswithnodes)
                        {
                            debugtxt += xx + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    {
                        Log("jobinfo.inputrecordlengths");
                        string debugtxt = "";
                        foreach (int xx in jobinfo.inputrecordlengths)
                        {
                            debugtxt += xx.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    {
                        Log("_mapinputchunks");
                        string debugtxt = "";
                        foreach (dfs.DfsFile.FileNode xx in _mapinputchunks)
                        {
                            debugtxt += xx.Name + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    {
                        Log("_inputnodesoffsets");
                        string debugtxt = "";
                        foreach (int xx in _inputnodesoffsets)
                        {
                            debugtxt += xx.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    {
                        Log("_mapfileswithnodes");
                        string debugtxt = "";
                        foreach (string xx in _mapfileswithnodes)
                        {
                            debugtxt += xx + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    {
                        Log("inputrecordlengths");
                        string debugtxt = "";
                        foreach (int xx in _inputrecordlengths)
                        {
                            debugtxt += xx.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
#endif

                    int firstsetpos = -1;
                    MapReduceBlockInfo targetblock = null;
                    string[] dfsfilenames = new string[blockscount];
                    string curfilename = null;
                    int curreclen = 0;
                    int curoffset = 0;
                    int fi = 0;
                    if (_mapinputchunks.Count > 0)
                    {
                        curoffset = _inputnodesoffsets[fi];
                    }
                    for (int mi = 0; mi < _mapinputchunks.Count; mi++)
                    {
                        if (curoffset == mi)
                        {
                            curfilename = _mapfileswithnodes[fi];
                            curreclen = _inputrecordlengths[fi];
                            if (++fi < _inputnodesoffsets.Count)
                            {
                                curoffset = _inputnodesoffsets[fi];
                            }
                        }

                        if (++firstsetpos >= firstset.Length)
                        {
                            firstsetpos = 0;
                        }
                        targetblock = firstset[firstsetpos];

                        targetblock.mapinputdfsnodes.Add(dfs.MapNodeToNetworkStarPath(_mapinputchunks[mi], false, rnd.Next() % jobinfo.dc.Replication));
                        if (dfsfilenames[targetblock.BlockID] != curfilename)
                        {
                            if (targetblock.mapinputfilenames == null)
                            {
                                targetblock.mapinputfilenames = new List<string>();
                                targetblock.mapinputnodesoffsets = new List<int>();
                            }
                            int offset = targetblock.mapinputdfsnodes.Count - 1;
                            targetblock.mapinputnodesoffsets.Add(offset);
                            targetblock.mapinputfilenames.Add(curfilename);
                            targetblock.acl.InputRecordLengths.Add(curreclen);
                            dfsfilenames[targetblock.BlockID] = curfilename;
                        }
                    }
                }

                //scramble each inputfile node path and assign to the other set of blocks
                {
                    foreach (MapReduceBlockInfo block in firstset)
                    {
                        for (int ri = 1; ri < jobinfo.dc.Replication; ri++)
                        {
                            MapReduceBlockInfo repblock = allBlocks[ri * blockscount + block.BlockID];
                            repblock.mapinputnodesoffsets = block.mapinputnodesoffsets;
                            repblock.mapinputfilenames = block.mapinputfilenames;
                            repblock.acl.InputRecordLengths = block.acl.InputRecordLengths;
                        }
                        
                        foreach (string mpinput in block.mapinputdfsnodes)
                        {
                            string[] parts = mpinput.Split('*');
                            for (int ri = 1; ri < jobinfo.dc.Replication; ri++)
                            {
                                int firsthost = rnd.Next() % parts.Length;
                                string sbmpinput = "";
                                for (int pi = 0; pi < parts.Length; pi++)
                                {
                                    if (firsthost >= parts.Length)
                                    {
                                        firsthost = 0;
                                    }
                                    if (sbmpinput.Length > 0)
                                    {
                                        sbmpinput += "*";
                                    }
                                    sbmpinput += parts[firsthost++];
                                }
                                MapReduceBlockInfo repblock = allBlocks[ri * blockscount + block.BlockID];
                                repblock.mapinputdfsnodes.Add(sbmpinput);
                            }
                        }
                    }
                }

#if FAILOVER_DEBUG
                //SANITY CHECK
                //make sure each host doesn't have repeated blockid.
                {
                    Dictionary<string, Dictionary<int, MapReduceBlockInfo>> san = new Dictionary<string, Dictionary<int, MapReduceBlockInfo>>();
                    foreach (MapReduceBlockInfo bl in allBlocks)
                    {
                        if (!san.ContainsKey(bl.SlaveHost.ToLower()))
                        {
                            san.Add(bl.SlaveHost.ToLower(), new Dictionary<int, MapReduceBlockInfo>());
                        }
                        san[bl.SlaveHost.ToLower()].Add(bl.BlockID, bl);
                    }
                    Log("====== SANITY CHECK PASSED #1 ======");

                    Log("hostscount in allBlocks=" + san.Count.ToString());
                    string debugtxt = "";
                    foreach (string h in san.Keys)
                    {
                        debugtxt += h + ":" + san[h].Values.Count.ToString() + Environment.NewLine;
                    }
                    Log(debugtxt);

                    foreach (string host in san.Keys)
                    {
                        Log("host " + host + " has " + san[host].Count.ToString() + " blocks");
                    }

                    Dictionary<int, int> repblockcountperhost = new Dictionary<int, int>();
                    foreach (string host in san.Keys)
                    {
                        Dictionary<int, List<MapReduceBlockInfo>> repfactorToBlocks = new Dictionary<int, List<MapReduceBlockInfo>>();
                        Dictionary<int, MapReduceBlockInfo> blocks = san[host];

                        foreach (MapReduceBlockInfo block in blocks.Values)
                        {
                            int repf = (block.BlockCID - block.BlockID) / blockscount;
                            if (!repfactorToBlocks.ContainsKey(repf))
                            {
                                repfactorToBlocks.Add(repf, new List<MapReduceBlockInfo>());
                            }
                            repfactorToBlocks[repf].Add(block);
                        }

                        Log("Distribution of blocks in host " + host);
                        foreach (int repf in repfactorToBlocks.Keys)
                        {
                            Log("replication index=" + repf.ToString() + "; blocks=" + repfactorToBlocks[repf].Count);

                            if (!repblockcountperhost.ContainsKey(repfactorToBlocks[repf].Count))
                            {
                                repblockcountperhost.Add(repfactorToBlocks[repf].Count, 0);
                            }
                        }
                    }

                    string txt = "";
                    foreach (int r in repblockcountperhost.Keys)
                    {
                        txt += r.ToString() + ",";
                    }
                    Log("Replication blocks count per host:" + txt);
                }

                {
                    if (blockscount != _blocks.Count)
                    {
                        throw new Exception("blockscount != _blocks.Count; blockscount=" + blockscount.ToString() + ";_blocks.count=" + _blocks.Count.ToString());
                    }
                    Log("====== SANITY CHECK PASSED #2 ======");
                }

                //make sure all blockid and blockcid are correct.
                {
                    for (int bi = 0; bi < allBlocks.Length; bi++)
                    {
                        MapReduceBlockInfo bl = allBlocks[bi];
                        if (bi != bl.BlockCID)
                        {
                            throw new Exception("bi doesn't match bl.BlockCID. bi=" + bi.ToString() + ";bl.BlockCID=" + bl.BlockCID.ToString());
                        }

                        if ((bl.BlockCID - bl.BlockID) % blockscount != 0)
                        {
                            throw new Exception("(bl.BlockCID - bl.BlockID) % blockscount != 0.  bl.blockcid=" + bl.BlockCID.ToString() + ";bl.blockid=" + bl.BlockID.ToString());
                        }
                    }
                    Log("====== SANITY CHECK PASSED #3 ======");
                }

                //check blockstatus
                {
                    if (blockStatus.Count != allblockscount)
                    {
                        throw new Exception("blockStatus.Count != allblockscount;blockstatuscount=" + blockStatus.Count.ToString() + ";allblockscount=" + allblockscount.ToString());
                    }
                    List<int> san = new List<int>(blockStatus.Keys);
                    san.Sort();
                    if (san[0] != 0)
                    {
                        throw new Exception("san[0] != 0; san[0]=" + san[0].ToString());
                    }
                    if (san[allblockscount - 1] != allblockscount - 1)
                    {
                        throw new Exception("san[allblockscount - 1] != allblockscount - 1; san[allblockscount-1]=" + san[allblockscount - 1].ToString() + ";allblockscount=" + allblockscount.ToString());
                    }
                    Log("====== SANITY CHECK PASSED #4 ======");
                }

                //make sure each block id has repfactor number of copies and on different host.
                {
                    Dictionary<int, Dictionary<string, MapReduceBlockInfo>> san = new Dictionary<int, Dictionary<string, MapReduceBlockInfo>>();
                    foreach (MapReduceBlockInfo bl in allBlocks)
                    {
                        if (!san.ContainsKey(bl.BlockID))
                        {
                            san.Add(bl.BlockID, new Dictionary<string, MapReduceBlockInfo>());
                        }
                        san[bl.BlockID].Add(bl.SlaveHost, bl);
                    }

                    foreach (int blockid in san.Keys)
                    {
                        Dictionary<string, MapReduceBlockInfo> bls = san[blockid];
                        if (bls.Count != jobinfo.dc.Replication)
                        {
                            throw new Exception("bls.Count != repfactor");
                        }
                    }
                    Log("====== SANITY CHECK PASSED #5 ======");

                    {
                        string txt = "";
                        foreach (int blockid in san.Keys)
                        {
                            txt += blockid.ToString() + Environment.NewLine;
                            Dictionary<string, MapReduceBlockInfo> repbs = san[blockid];
                            foreach (KeyValuePair<string, MapReduceBlockInfo> pair in repbs)
                            {
                                txt += pair.Value.BlockID.ToString() + ":" + pair.Value.BlockCID.ToString() + ":" + pair.Value.SlaveHost +
                                    Environment.NewLine;
                            }
                        }
                        Log("Blocks distribution:");
                        Log(txt);
                    }
                }
#endif
            }

            internal void AssignBlocksToHosts(MapReduceBlockInfo[] scrambled, string[] hosts, ref int nexthost, Dictionary<string, Dictionary<int, MapReduceBlockInfo>> hostToBlockIDs, List<MapReduceBlockInfo> collisions, Dictionary<string, Dictionary<int, MapReduceBlockInfo>> hostToBlockIDsPerReplication, int blocksperhost, int blockscount)
            {
                for (int bi = 0; bi < scrambled.Length; bi++)
                {
                    string thishost = hosts[nexthost].ToLower();                    
                    MapReduceBlockInfo block = scrambled[bi];
                    Dictionary<int, MapReduceBlockInfo> blocksalreadyonhost = null;
                    if (hostToBlockIDs.ContainsKey(thishost))
                    {
                        blocksalreadyonhost = hostToBlockIDs[thishost];
                    }
                    else
                    {
                        blocksalreadyonhost = new Dictionary<int, MapReduceBlockInfo>(blocksperhost);
                        hostToBlockIDs.Add(thishost, blocksalreadyonhost);
                    }

                    if (blocksalreadyonhost.ContainsKey(block.BlockID))
                    {
                        collisions.Add(block);
                        continue;
                    }

                    block.SlaveHost = thishost;
                    blocksalreadyonhost.Add(block.BlockID, block);
                    if (!hostToBlockIDsPerReplication.ContainsKey(thishost))
                    {
                        hostToBlockIDsPerReplication.Add(thishost, new Dictionary<int, MapReduceBlockInfo>(blockscount));
                    }
                    hostToBlockIDsPerReplication[thishost].Add(block.BlockID, block);

                    //move to the next host only if done assigning
                    if (++nexthost >= hosts.Length)
                    {
                        nexthost = 0;
                    }
                }                

                //swap with another host if nothing was assigned, we are stuck at one host, and this host has all the blockids from the collision list.
                if (collisions.Count == scrambled.Length)
                {
#if FAILOVER_DEBUG
                    Log("Swap begins... collisioncount=" + collisions.Count.ToString() + ";scrambledcount=" + scrambled.Length.ToString());
#endif
                    MapReduceBlockInfo xblock = collisions[0];
                    string thishost = hosts[nexthost].ToLower();
                    Dictionary<int, MapReduceBlockInfo> blocksalreadyonhost = null;
                    if (hostToBlockIDs.ContainsKey(thishost))
                    {
                        blocksalreadyonhost = hostToBlockIDs[thishost];
                    }
                    else
                    {
                        blocksalreadyonhost = new Dictionary<int, MapReduceBlockInfo>(blocksperhost);
                        hostToBlockIDs.Add(thishost, blocksalreadyonhost);
                    }

                    int phostindex = rnd.Next() % hosts.Length;
                    for (int hi = 0; hi < hosts.Length; hi++)
                    {
                        string phost = hosts[phostindex].ToLower();
                        if (++phostindex >= hosts.Length)
                        {
                            phostindex = 0;
                        }                       
                        
                        if (!hostToBlockIDsPerReplication.ContainsKey(phost))
                        {
                            continue;
                        }                     
                       
                        if (hostToBlockIDs.ContainsKey(phost))
                        {
                            if (hostToBlockIDs[phost].ContainsKey(xblock.BlockID))
                            {
                                continue;
                            }
                        }     
                        
                        MapReduceBlockInfo blocktoswap = null;
                        Dictionary<int, MapReduceBlockInfo> pblocks = hostToBlockIDsPerReplication[phost];
                        foreach (MapReduceBlockInfo pblock in pblocks.Values)
                        {
                            if (!blocksalreadyonhost.ContainsKey(pblock.BlockID))
                            {
                                blocktoswap = pblock;
                                break;
                            }
                        }
                        if (blocktoswap == null)
                        {
                            continue;
                        }
                        
                        //swap
                        {                            
                            hostToBlockIDs[phost].Remove(blocktoswap.BlockID);
                            pblocks.Remove(blocktoswap.BlockID);
                            
                            blocktoswap.SlaveHost = thishost;
                            blocksalreadyonhost.Add(blocktoswap.BlockID, blocktoswap);
                            if (!hostToBlockIDsPerReplication.ContainsKey(thishost))
                            {
                                hostToBlockIDsPerReplication.Add(thishost, new Dictionary<int, MapReduceBlockInfo>(blockscount));
                            }
                            hostToBlockIDsPerReplication[thishost].Add(blocktoswap.BlockID, blocktoswap);
                            
                            xblock.SlaveHost = phost;
                            hostToBlockIDs[phost].Add(xblock.BlockID, xblock);
                            pblocks.Add(xblock.BlockID, xblock);

                            collisions.RemoveAt(0);
#if FAILOVER_DEBUG
                            Log("Swap done");
#endif
                            break; //done
                        }
                    }

                    if (collisions.Count == scrambled.Length)
                    {                        
                        throw new Exception("Cannot resolve collisions");
                    }

                    if (++nexthost >= hosts.Length)
                    {
                        nexthost = 0;
                    }
                }
            }

            internal bool CheckMachineFailure(string host)
            {
                bool failure = false;
                if (IsMachinePingable(host))
                {
                    string reason = null;
                    failure = diskcheck.IsDiskFailure(host, out reason);                    
                }
                else
                {
                    failure = true;
                }
                return failure;
            }            

            internal int CheckMachineFailures()
            {
                int nthreads = goodHosts.Count;
                if (nthreads > 15)
                {
                    nthreads = 15;
                }

                newBadHosts.Clear();

                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string host)
                {
                    if (CheckMachineFailure(host))
                    {
                        lock (newBadHosts)
                        {
                            newBadHosts.Add(host);
                        }
                    }                                     
                }), new List<string>(goodHosts.Keys), nthreads);

                if (newBadHosts.Count > 0)
                {
                    foreach (string bh in newBadHosts)
                    {
                        goodHosts.Remove(bh);
                        badHosts.Add(bh, 0);
                    }
                }

                return newBadHosts.Count;
            }

            internal bool AllBlocksCompleted(int finalstatus)
            {
                lock (blockStatus)
                {
                    foreach(int bs in blockStatus.Values)
                    {
                        if (bs != finalstatus)
                        {
                            return false;
                        }
                    }
                }                
                return true;
            }

            internal void CloseAllBlocks()
            {
                for (int i = 0; i < allBlocks.Length; i++)
                {
                    if (allBlocks[i] != null)
                    {
                        allBlocks[i].acl.StopZMapBlockServer();
                        allBlocks[i].acl.Close();
                    }
                }
            }

            internal void AbortBlocksFromFailedHost(string badhost)
            {
                if (hostToBlocks.ContainsKey(badhost))
                {
                    List<MapReduceBlockInfo> badblocks = hostToBlocks[badhost];
#if FAILOVER_DEBUG
                    Log(badhost + " contains badblocks; badblockscount=" + badblocks.Count.ToString());
#endif
                    foreach (MapReduceBlockInfo badblock in badblocks)
                    {
                        allBlocks[badblock.BlockCID] = null;

                        lock (blockStatus)
                        {
                            blockStatus.Remove(badblock.BlockCID);
#if FAILOVER_DEBUG
                            Log("Removed badblock from blockstatus; blockcid=" + badblock.BlockCID.ToString());
#endif
                        }

                        try
                        {
                            badblock.thread.Abort();
#if FAILOVER_DEBUG
                            Log("Aborted badblock; blockcid=" + badblock.BlockCID.ToString());
#endif
                            AELight_RemoveTraceThread(badblock.thread); //no join, just remove.
#if FAILOVER_DEBUG
                            Log("AELight removed trace badblock; blockcid=" + badblock.BlockCID.ToString());
#endif
                        }
                        catch
                        {
                        }
                    }

                    hostToBlocks.Remove(badhost);
                }
            }

            internal void UpdateBlockStatus(int blockcid, int status)
            {
                lock (blockStatus)
                {
                    if (blockStatus.ContainsKey(blockcid))
                    {
                        blockStatus[blockcid] = status;
                    }
                }
            }

            internal void Log(string msg)
            {
                string tempfile = @"c:\temp\failoverlog_102C5560-2012-4537-AFC8-C40ADF0AD2B8.txt";
                lock (goodHosts)
                {
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(tempfile, true))
                    {
                        w.WriteLine("  [" + DateTime.Now.ToString() + "]");
                        w.WriteLine(msg);
                    }
                }
            }

            internal class JobInfo
            {
                internal MapReduceBlockInfo.JobBlocksShared jobshared;
                internal List<string> allinputsamples;
                internal bool extraverbose;
                internal List<string> outputfiles;
                internal string outputfile;
                internal dfs dc;
                internal string[] execargs;
                internal string logname;
                internal List<int> outputrecordlengths;
                internal string slaveconfigxml;
                internal bool verbose;
                internal SourceCode.Job cfgj;
                internal List<dfs.DfsFile.FileNode> mapinputchunks;
                internal List<string> mapfileswithnodes;
                internal List<int> inputnodesoffsets;
                internal List<int> inputrecordlengths;
                internal System.Threading.Thread timethread;
            }
        }

        internal class MapReduceBlockInfo
        {
            
            // Shared between all blocks of one job.
            internal class JobBlocksShared
            {
                internal int blockcount = -1;
                internal DateTime sortstart = DateTime.MinValue;
                internal int sortsdone = 0;
#if MR_EXCHANGE_TIME_PRINT
                internal DateTime exchangestart = DateTime.MinValue;
                internal int exchangesdone = 0;
#endif
                internal string ExecOpts;
                internal string noutputmethod; // Set to default when "sorted"
                internal bool anysplits = false;
            }

            internal JobBlocksShared jobshared;

            internal bool AddCacheOnly; // !

            string ExchangeOrder { get { return cfgj.ExchangeOrder; } }

            internal string logname;
            internal SourceCode.Job cfgj;
            internal MySpace.DataMining.DistributedObjects5.ArrayComboList acl;
            internal List<MapReduceBlockInfo> all;
            internal System.Threading.Thread thread;
            internal bool verbose = false;
            internal bool extraverbose = false;
            internal long basefilesize = -1;
            //internal int blockcount = -1;
            internal int blockcount { get { return jobshared.blockcount; } }
            internal List<string> outputfiles;
            internal string outputfile;
            internal int BlockID;
            internal string SlaveHost;
            internal string SlaveIP;
            internal string[] ExecArgs;
            //internal byte CompressZMapBlocks = 1;
            internal string slaveconfigxml;
            internal List<string> allinputsamples;

            internal List<int> ownedzmapblockIDs = new List<int>();

            internal List<string> mapinputdfsnodes = new List<string>();
            internal List<List<string>> reduceoutputdfsnodeses = null;
            internal List<List<long>> reduceoutputsizeses = null;
            internal List<string> mapinputfilenames = null;
            internal List<int> mapinputnodesoffsets = null;

            internal int ZBlockSplitCount = 0;

            internal int BlockCID = -1;
            internal FailoverInfo failover = null;

            string codectx, mapctx;

            //protected internal bool blockfail = false;
            protected internal Exception LastThreadException = null;
            protected internal bool blockfail
            {
                get
                {
                    return null != LastThreadException;
                }
            }


            int userverbose(char ch)
            {
                int i = cfgj.Verbose.IndexOf(ch);
                if (-1 == i)
                {
                    return 0; // Nope!
                }
                if (0 == i || '-' != cfgj.Verbose[i - 1])
                {
                    return 1; // In there.
                }
                return -1; // In there with '-' before it.
            }


            void InZBlocks()
            {
                ZBlockSplitCount = acl.GetZBlockSplitCount();
                if (ZBlockSplitCount > 0)
                {
                    for (int isplit = 0; isplit < ZBlockSplitCount; isplit++)
                    {
                        bool little = true;
                        lock (jobshared)
                        {
                            if (!jobshared.anysplits)
                            {
                                jobshared.anysplits = true;
                                little = false;
                            }
                        }
                        if (little)
                        {
                            Console.Write("(split)");
                        }
                        else
                        {
                            Console.Write("(split occured; consider increasing zblock count)");
                        }
                        ConsoleFlush();
                    }
                }

                bool dosorttime = -1 == jobshared.ExecOpts.IndexOf(" -s-");

                acl.SortBlocks();

                if (extraverbose)
                {
                    {
                        char ev = 's';
                        switch (userverbose(ev))
                        {
                            case -1:
                                break;
                            case 1:
                                Console.Write("[{0}/{1}]", ev, SlaveHost);
                                ConsoleFlush();
                                break;
                            default:
                                Console.Write(ev);
                                ConsoleFlush();
                                break;
                        }
                    }
                }

                if (dosorttime)
                {
                    bool sortdone = false;
                    lock (jobshared)
                    {
                        if (++jobshared.sortsdone == jobshared.blockcount)
                        {
                            sortdone = true;
                        }
                    }
                    if (sortdone)
                    {
                        int sortsecs = (int)Math.Round((DateTime.Now - jobshared.sortstart).TotalSeconds);
                        ConsoleColor oldcolor = Console.ForegroundColor;
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.Write("{0}[sort completed {1}]{2}", isdspace ? "\u00011" : "", DurationString(sortsecs), isdspace ? "\u00010" : "");
                        Console.ForegroundColor = oldcolor;
                        ConsoleFlush();
                    }
                }

                bool failintegratesnow = false;
                if (cfgj.IsDeltaSpecified)
                {
                    if (!acl.IntegrateZBalls(cfgj.Delta.Name))
                    {
                        //Console.Error.WriteLine("Error: unable to integrate snowball; cache not found");
                        failintegratesnow = true;
                    }
                    else
                    {
                        if (extraverbose)
                        {
                            {
                                char ev = 'i';
                                switch (userverbose(ev))
                                {
                                    case -1:
                                        break;
                                    case 1:
                                        Console.Write("[{0}/{1}]", ev, SlaveHost);
                                        ConsoleFlush();
                                        break;
                                    default:
                                        Console.Write(ev);
                                        ConsoleFlush();
                                        break;
                                }
                            }
                        }
                    }
                }

                if (AddCacheOnly)
                {
                    acl.CommitZBalls(cfgj.Delta.Name, false);

                    if (extraverbose)
                    {
                        {
                            char ev = 'c';
                            switch (userverbose(ev))
                            {
                                case -1:
                                    break;
                                case 1:
                                    Console.Write("[{0}/{1}]", ev, SlaveHost);
                                    ConsoleFlush();
                                    break;
                                default:
                                    Console.Write(ev);
                                    ConsoleFlush();
                                    break;
                            }
                        }
                    }

                    if (failintegratesnow)
                    {
                        //Console.WriteLine("New snowball committed: {0}", cfgj.Snowball.Name); // Gets repeated for each slave...
                    }
                }
                else
                {
                    string enumcode = codectx
                                + (@"
private static int userlogsremain = " + AELight.maxuserlogs.ToString() + @";
public void Qizmt_Log(string line) { DSpace_Log(line); }
public void DSpace_Log(string line)
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
        using (System.IO.StreamWriter fstm = System.IO.File.AppendText(DSpace_LogName))
        {
            fstm.WriteLine(`{0}`, line);
        }
    }
    finally
    {
        _logmutex.ReleaseMutex();
    }
}

public void Qizmt_LogResult(string line, bool passed) { DSpace_LogResult(line, passed); }
public void DSpace_LogResult(string name, bool passed)
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
").Replace('`', '"')
                                + cfgj.MapReduce.ReduceInitialize
                                + cfgj.MapReduce.Reduce
                                + cfgj.MapReduce.ReduceFinalize;

                    if (cfgj.MapReduce.Map.Length != 0)
                    {
                        //System.Threading.Thread.Sleep(8000);

                        List<string> basefns = new List<string>(outputfiles.Count);
                        //List<string> basefns = new List<string>(1);
                        for (int n = 0; n < outputfiles.Count; n++)
                        {
                            string ofile = outputfiles[n];
                            if (ofile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                            {
                                ofile = ofile.Substring(6);
                            }
                            basefns.Add(GenerateZdFileDataNodeBaseName(ofile));
                        }

                        acl.EnumerateToFiles(basefns, basefilesize, enumcode, cfgj.Usings); // reduceoutputsizes is appended-to.
                        if (extraverbose)
                        {
                            {
                                char ev = 'r';
                                switch (userverbose(ev))
                                {
                                    case -1:
                                        break;
                                    case 1:
                                        Console.Write("[{0}/{1}]", ev, SlaveHost);
                                        ConsoleFlush();
                                        break;
                                    default:
                                        Console.Write(ev);
                                        ConsoleFlush();
                                        break;
                                }
                            }
                        }

                        reduceoutputsizeses = new List<List<long>>(outputfiles.Count);
                        //reduceoutputsizeses = new List<List<long>>(1);
                        reduceoutputdfsnodeses = new List<List<string>>(outputfiles.Count);
                        //reduceoutputdfsnodeses = new List<List<string>>(1);
                        for (int n = 0; n < outputfiles.Count; n++)
                        {
                            reduceoutputsizeses.Add(new List<long>());
                            List<long> sizes = reduceoutputsizeses[n];
                            int nchunks = acl.GetNumberOfEnumerationFilesCreated(n, sizes);

                            reduceoutputdfsnodeses.Add(new List<string>());
                            List<string> nodes = reduceoutputdfsnodeses[n];
                            string basefilename = basefns[n];
                            for (int ne = 0; ne < nchunks; ne++)
                            {
                                nodes.Add(basefilename.Replace("%n", ne.ToString()));
                            }
                        }

                        if (extraverbose)
                        {
                            long outnbytes = 0;
                            for (int i = 0; i < reduceoutputsizeses.Count; i++)
                            {
                                for (int j = 0; j < reduceoutputsizeses[i].Count; j++)
                                {
                                    outnbytes += reduceoutputsizeses[i][j];
                                }
                            }
                            if (0 == outnbytes)
                            {
                                //Console.Write("[{0}]*", GetFriendlyByteSize(outnbytes).Replace(" ", ""));
                                //Console.Write("[0B]*");
                                {
                                    char ev = '0';
                                    switch (userverbose(ev))
                                    {
                                        case -1:
                                            break;
                                        case 1:
                                            Console.Write("[0B/{0}]", SlaveHost);
                                            ConsoleFlush();
                                            break;
                                        default:
                                            Console.Write("[0B]");
                                            ConsoleFlush();
                                            break;
                                    }
                                }
                            }
                            else
                            {
                                //Console.Write('*');
                                //ConsoleFlush();
                            }
                        }
                        else if (verbose)
                        {
                            //Console.Write('*');
                            //ConsoleFlush();
                        }

                        ZBlockSplitCount = acl.GetZBlockSplitCount();

                    }
                    else
                    {
                        MySpace.DataMining.DistributedObjects5.ArrayComboListEnumerator[] enums
                            = acl.GetEnumeratorsWithCode(enumcode, cfgj.Usings);

                        foreach (MySpace.DataMining.DistributedObjects5.ArrayComboListEnumerator en in enums)
                        {
                            while (en.MoveNext())
                            {
                            }
                        }

                        if (verbose)
                        {
                            Console.Write('*');
                            ConsoleFlush();
                        }
                    }
                }
            }


            // Note: depends on DSpace_LogName being set!
            internal static string MapReduceCommonDynamicCsCode = (@"
    // MapReduce-only functions:
    public static void PadKeyBuffer(List<byte> keybuf)
    {
        if(keybuf.Count > DSpace_KeyLength)
        {
            throw new Exception(`Key too long`);
        }
        PadBytes(keybuf, DSpace_KeyLength, 0);
    }
    public static ByteSlice PrepareAsciiKey(string s)
    {
        List<byte> keybuf = new List<byte>(DSpace_KeyLength);
        Entry.AsciiToBytesAppend(s, keybuf);
        PadKeyBuffer(keybuf);
        return ByteSlice.Prepare(keybuf);
    }
    public static ByteSlice UnpadKeyBuffer(IList<byte> keybuf)
    {
        int len = 0;
        for(; len < keybuf.Count && keybuf[len] != 0; len++)
        {
        }
        return ByteSlice.Prepare(keybuf, 0, len);
    }
    public static ByteSlice UnpadKey(ByteSlice keybuf)
    {
        int len = 0;
        for(; len < keybuf.Length && keybuf[len] != 0; len++)
        {
        }
        return ByteSlice.Prepare(keybuf, 0, len);
    }

static System.Threading.Mutex _logmutex = new System.Threading.Mutex(false, `distobjlog`);

").Replace('`', '"');

#if CONN_BACKLOG_IPSLEEP
            Dictionary<string, int> connbacklog = new Dictionary<string, int>();
#else
#if CONN_BACKLOG_IPSINGLE
            Dictionary<string, string> connbacklog = new Dictionary<string, string>();
#endif
#endif


            internal void fsortedthreadproc()
            {               
                try
                {
                    ensureopen();

                    {
                        bool dosorttime = -1 == jobshared.ExecOpts.IndexOf(" -s-");

                        if (dosorttime)
                        {
                            lock (jobshared)
                            {
                                if (DateTime.MinValue == jobshared.sortstart)
                                {
                                    jobshared.sortstart = DateTime.Now;
                                }
                            }
                        }
                    }

                    {
                        char ev = 'F';
                        switch (userverbose(ev))
                        {
                            case -1:
                                break;
                            case 1:
                                Console.Write(ev);
                                ConsoleFlush();
                                break;
                            default:
                                break;
                        }
                    }

#if DEBUG
                    if (0 != string.Compare(jobshared.noutputmethod, "rsorted")
                        && 0 != string.Compare(jobshared.noutputmethod, "fsorted"))
                    {
                        throw new Exception("output method expected to be rsorted or fsorted, not " + jobshared.noutputmethod);
                    }
#endif
                    
#if DEBUG
                    //System.Threading.Thread.Sleep(1000 * 8);
#endif

                    if (cfgj.IsDeltaSpecified)
                    {
                        acl.FoilCacheName = cfgj.Delta.Name;
                    }
                    acl.DoFoilMapSample(mapinputdfsnodes, mapctx + cfgj.MapReduce.Map, cfgj.Usings, mapinputfilenames, mapinputnodesoffsets);

                    if (extraverbose)
                    {
                        {
                            char ev = 'f';
                            switch (userverbose(ev))
                            {
                                case -1:
                                    break;
                                case 1:
                                    Console.Write("[{0}/{1}]", ev, SlaveHost);
                                    ConsoleFlush();
                                    break;
                                default:
                                    Console.Write(ev);
                                    ConsoleFlush();
                                    break;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    LastThreadException = e;
                    //lock (typeof(BlockInfo))
                    {
                        if (-1 != e.ToString().IndexOf("[Note: ensure the Windows service is running]"))
                        {
                            //LogOutputToFile("Thread exception: " + e.ToString());
                            LogOutput(" <!> Unable to connect to DistributedObjects service on " + SlaveHost + ": Thread exception" + e.ToString());
                            //Environment.Exit(1);
                            //blockfail = true;
                        }
                        else
                        {
                            LogOutput("Thread exception: (rsorted/fsorted thread) " + e.ToString());
                        }
                    }
                }
            }


            internal void gencodectx()
            {
                codectx = (@"
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

    public const int DSpace_KeyLength = " + cfgj.IOSettings.KeyLength.ToString() + @";
    public const int Qizmt_KeyLength = DSpace_KeyLength;
    
    public const string DSpace_LogName = @`" + logname + @"`;
    public const string Qizmt_LogName = DSpace_LogName;

    public static int DSpace_InputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength; } }
    public static int Qizmt_InputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength; } }

    public static int DSpace_OutputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength; } }
    public static int Qizmt_OutputRecordLength { get { return MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength; } }

").Replace('`', '"') + MapReduceCommonDynamicCsCode + CommonDynamicCsCode;

                mapctx = codectx
                            + (@"

private static int userlogsremain = " + AELight.maxuserlogs.ToString() + @";
public void Qizmt_Log(string line) { DSpace_Log(line); }
public void DSpace_Log(string line)
{
    if(MapSampling)
    {
        return;
    }
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
        using (System.IO.StreamWriter fstm = System.IO.File.AppendText(DSpace_LogName))
        {
            fstm.WriteLine(`{0}`, line);
        }
    }
    finally
    {
        _logmutex.ReleaseMutex();
    }
}
public void Qizmt_LogResult(string line, bool passed) { DSpace_LogResult(line, passed); }
public void DSpace_LogResult(string name, bool passed)
{
    if(passed)
    {
        DSpace_Log(`[\u00012PASSED\u00010] - ` + name);
    }
    else
    {
        DSpace_Log(`[\u00014FAILED\u00010] - ` + name);
    }

}").Replace('`', '"');
            }


            bool _isopen = false;

            void ensureopen()
            {
                if (_isopen)
                {
                    return;
                }
                _isopen = true;
                {
                    char ev = 'D';
                    switch (userverbose(ev))
                    {
                        case -1:
                            break;
                        case 1:
                            Console.Write(ev);
                            ConsoleFlush();
                            break;
                        default:
                            break;
                    }
                }
#if CONN_BACKLOG_IPSLEEP
                    {
                        int sleepcounts = 0;
                        lock (connbacklog)
                        {
                            if (!connbacklog.ContainsKey(SlaveIP))
                            {
                                connbacklog[SlaveIP] = 1;
                            }
                            else
                            {
                                sleepcounts = connbacklog[SlaveIP];
                                connbacklog[SlaveIP] = sleepcounts + 1;
                            }
                        }
                        if (sleepcounts > 0)
                        {
                            System.Threading.Thread.Sleep(sleepcounts * 200);
                        }
                    }

                    acl.Open();
#else
#if CONN_BACKLOG_IPSINGLE
                {
                    object openlock;
                    lock (connbacklog)
                    {
                        if (connbacklog.ContainsKey(SlaveIP))
                        {
                            openlock = connbacklog[SlaveIP];
                        }
                        else
                        {
                            openlock = SlaveIP;
                            connbacklog[SlaveIP] = SlaveIP;
                        }
                    }
                    lock (openlock)
                    {
                        acl.Open();
                        System.Threading.Thread.Sleep(50);
                    }
                }
#else
                    acl.Open(); // Neither...
#endif
#endif
                {
                    char ev = 'd';
                    switch (userverbose(ev))
                    {
                        case -1:
                            break;
                        case 1:
                            Console.Write(ev);
                            ConsoleFlush();
                            break;
                        default:
                            break;
                    }
                }
            }


            internal void firstthreadproc()
            {
                try
                {
                    ensureopen();

                    if (null != cfgj.Delta)
                    {
                        acl.EnumeratorCacheName = cfgj.Delta.Name;
                    }

                    //System.Threading.Thread.Sleep(8000);
                    //acl.SendXmlConfig(slaveconfigxml);

                    if (cfgj.MapReduce.Map.Length != 0)
                    {
                        //acl.CompressZMapBlocks = CompressZMapBlocks;

                        bool dosorttime = -1 == jobshared.ExecOpts.IndexOf(" -s-");

                        if (dosorttime)
                        {
                            lock (jobshared)
                            {
                                if (DateTime.MinValue == jobshared.sortstart)
                                {
                                    jobshared.sortstart = DateTime.Now;
                                }
                            }
                        }

#if DEBUG
                        //System.Threading.Thread.Sleep(8000);
#endif

                        if (0 == string.Compare("dsorted", jobshared.noutputmethod, true))
                        {
                            acl.LoadSamples(allinputsamples, blockcount, "MdoDistro");
                        }
                        else if (0 == string.Compare("grouped", jobshared.noutputmethod, true))
                        {
                        }
                        else if (0 == string.Compare("bsorted", jobshared.noutputmethod, true))
                        {
                            acl.LoadSamples(allinputsamples, blockcount, "DsDistro");
                        }
                        else if (0 == string.Compare("rsorted", jobshared.noutputmethod, true)
                            || 0 == string.Compare("fsorted", jobshared.noutputmethod, true))
                        {
                            List<string> allfsamplefiles = new List<string>();
                            for (int i = 0; i < all.Count; i++)
                            {
                                allfsamplefiles.Add(Surrogate.NetworkPathForHost(all[i].SlaveHost) + @"\" + all[i].acl.zfoilbasename);
                            }
                            acl.LoadSamples(allfsamplefiles, blockcount, "FoilDistro");
                        }
                        else if (0 == string.Compare("rhashsorted", jobshared.noutputmethod, true)
                            || 0 == string.Compare("hashsorted", jobshared.noutputmethod, true))
                        {
                            switch (cfgj.IOSettings.KeyMajor)
                            {
                                case 1:
                                    acl.LoadSamples(null, blockcount, "Distro1");
                                    break;
                                case 2:
                                    acl.LoadSamples(null, blockcount, "Distro2");
                                    break;
                                default:
                                    throw new Exception("Job/IOSettings/OutputMethod of hashsorted/rhashsorted can only be used with KeyMajor of 1 or 2");
                            }
                        }
                        else
                        {
                            throw new Exception("Unknown OutputMethod: " + cfgj.IOSettings.OutputMethod);
                        }

                        acl.DoMap(mapinputdfsnodes, mapctx + cfgj.MapReduce.Map, cfgj.Usings, mapinputfilenames, mapinputnodesoffsets);
                        if (extraverbose)
                        {
                            {
                                char ev = 'm';
                                switch (userverbose(ev))
                                {
                                    case -1:
                                        break;
                                    case 1:
                                        Console.Write("[{0}/{1}]", ev, SlaveHost);
                                        ConsoleFlush();
                                        break;
                                    default:
                                        Console.Write(ev);
                                        ConsoleFlush();
                                        break;
                                }
                            }
                        }

                        acl.StartZMapBlockServer();

                        // Continues to exchangethreadproc.
                    }
                    else
                    {
                        acl.BeforeLoad(codectx + cfgj.MapReduce.DirectSlaveLoad);
                        if (extraverbose)
                        {
                            Console.Write('l');
                            ConsoleFlush();
                        }

                        InZBlocks();
                    }
                }
                catch (Exception e)
                {
                    LastThreadException = e;

                    bool donelogging = false;
                    if (failover != null)
                    {
                        if (failover.CheckMachineFailure(SlaveHost))
                        {
                            LogOutput("Thread exception: (first thread): Hardware failure at " + SlaveHost);
                            LogOutputToFile("Thread exception: (first thread) " + e.ToString());
                            donelogging = true;
                        }
                    }

                    //lock (typeof(BlockInfo))
                    if(!donelogging)
                    {
                        if (-1 != e.ToString().IndexOf("[Note: ensure the Windows service is running]"))
                        {
                            //LogOutputToFile("Thread exception: " + e.ToString());
                            LogOutput(" <!> Unable to connect to DistributedObjects service on " + SlaveHost + ": Thread exception" + e.ToString());
                            //Environment.Exit(1);
                            //blockfail = true;
                        }
                        else
                        {
                            LogOutput("Thread exception: (first thread) " + e.ToString());
                        }
                    }
                }

                if (failover != null)
                {
                    failover.UpdateBlockStatus(BlockCID, 1);
                }
            }


            static Random _erand = new Random(unchecked(DateTime.Now.Millisecond + System.Diagnostics.Process.GetCurrentProcess().Id));

            internal void exchangethreadproc()
            {
                try
                {
#if DEBUG
                    //System.Threading.Thread.Sleep(1000 * 10);
#endif
                    if (cfgj.MapReduce.Map.Length != 0)
                    {
                        List<MySpace.DataMining.DistributedObjects5.ArrayComboList> aclall = new List<MySpace.DataMining.DistributedObjects5.ArrayComboList>(all.Count);
                        if (0 == string.Compare("shuffle", ExchangeOrder, StringComparison.OrdinalIgnoreCase))
                        {
                            for (int i = 0; i < all.Count; i++)
                            {
                                aclall.Add(all[i].acl);
                            }
                            for (int i = 0; i < aclall.Count; i++)
                            {
                                MySpace.DataMining.DistributedObjects5.ArrayComboList aclx = aclall[i];
                                int ridx;
                                lock (_erand)
                                {
                                    ridx = _erand.Next(0, aclall.Count);
                                }
                                aclall[i] = aclall[ridx];
                                aclall[ridx] = aclx;
                            }
                        }
                        else if (0 == string.Compare("next", ExchangeOrder, StringComparison.OrdinalIgnoreCase))
                        {
                            // Start at current and wrap back around.
                            for (int i = BlockID + 1; i < all.Count; i++)
                            {
                                aclall.Add(all[i].acl);
                            }
                            for (int i = 0; i <= BlockID; i++)
                            {
                                aclall.Add(all[i].acl);
                            }
                        }
                        else
                        {
                            throw new Exception("Unsupported ExchangeOrder: " + ExchangeOrder);
                        }
#if MR_EXCHANGE_TIME_PRINT
                        lock (jobshared)
                        {
                            if (DateTime.MinValue == jobshared.exchangestart)
                            {
                                jobshared.exchangestart = DateTime.Now;
                            }
                        }
#endif
                        acl.ZBlockExchange(aclall, ownedzmapblockIDs);
                        if (extraverbose)
                        {
                            {
                                char ev = 'e';
                                switch (userverbose(ev))
                                {
                                    case -1:
                                        break;
                                    case 1:
                                        Console.Write("[{0}/{1}]", ev, SlaveHost);
                                        ConsoleFlush();
                                        break;
                                    default:
                                        Console.Write(ev);
                                        ConsoleFlush();
                                        break;
                                }
                            }
                        }

#if MR_EXCHANGE_TIME_PRINT
                        {
                            bool exchangedone = false;
                            lock (jobshared)
                            {
                                if (++jobshared.exchangesdone == jobshared.blockcount)
                                {
                                    exchangedone = true;
                                }
                            }
                            if (exchangedone)
                            {
                                int exchangesecs = (int)Math.Round((DateTime.Now - jobshared.exchangestart).TotalSeconds);
                                ConsoleColor oldcolor = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.Green;
                                Console.Write("{0}[exchange completed {1}]{2}", isdspace ? "\u00011" : "", DurationString(exchangesecs), isdspace ? "\u00010" : "");
                                Console.ForegroundColor = oldcolor;
                                ConsoleFlush();
                            }
                        }
#endif

                        InZBlocks();
                    }
                }
                catch (Exception e)
                {
                    LastThreadException = e;
                    //lock (typeof(BlockInfo))
                    {
                        if (-1 != e.ToString().IndexOf("[Note: ensure the Windows service is running]"))
                        {
                            LogOutputToFile("Thread exception: " + e.ToString());
                            LogOutput(" <!> Unable to connect to DistributedObjects service on " + SlaveHost);
                            //Environment.Exit(1);
                            //blockfail = true;
                        }
                        else
                        {
                            LogOutput("Thread exception: (exchange thread) " + e.ToString());
                        }
                    }
                }
            }

        }


        public static string GetSnowballFilesWildcard(string snowballname)
        {
            return "zsball_" + snowballname + "_*.zsb";
        }

        public static string GetSnowballFoilSampleFilesWildcard(string snowballname)
        {
            return "zfoil_" + snowballname + ".*.zf";
        }


        public static void _KillSnowballFileChunks_unlocked(dfs.DfsFile dfsnowball, bool verbose)
        {
            string snowballname = dfsnowball.Name;
            dfs dc = LoadDfsConfig(); // Just to get the slave list.
            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
            int nprobs = 0;
            for (int si = 0; si < slaves.Length; si++)
            {
                string netpath = NetworkPathForHost(slaves[si]);
                {
                    string fnwc = GetSnowballFilesWildcard(snowballname);
                    try
                    {
                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(fnwc))
                        {
                            try
                            {
                                fi.Delete();
                            }
                            catch (Exception e)
                            {
                                nprobs++;
                                LogOutputToFile(e.ToString());
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        nprobs++;
                        LogOutputToFile(e.ToString());
                    }
                }
                {
                    try
                    {
                        System.IO.File.Delete(netpath + @"\zsballsample_" + snowballname + ".zsb");
                    }
                    catch (Exception e)
                    {
                        nprobs++;
                        LogOutputToFile(e.ToString());
                    }
                }
                {
                    // zfoil cache!
                    string fnwc = GetSnowballFoilSampleFilesWildcard(snowballname);
                    try
                    {
                        foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(fnwc))
                        {
                            try
                            {
                                fi.Delete();
                            }
                            catch (Exception e)
                            {
                                nprobs++;
                                LogOutputToFile(e.ToString());
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        nprobs++;
                        LogOutputToFile(e.ToString());
                    }
                }
            }

            if (0 != nprobs)
            {
                Console.Error.WriteLine("{0} problems were encountered while deleting deltaCache", nprobs);
            }

            if (verbose)
            {
                Console.WriteLine("DeltaCache deleted: {0}", dfsnowball.Name);
            }
        }

        public static void _KillSnowballFileChunks_unlocked_mt(List<dfs.DfsFile> delfiles, bool verbose)
        {
            if (delfiles.Count == 0)
            {
                return;
            }

            dfs dc = LoadDfsConfig();
            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');

            string[] netpaths = new string[slaves.Length];
            for (int i = 0; i < slaves.Length; i++)
            {
                netpaths[i] = NetworkPathForHost(slaves[i]);
            }

            Dictionary<string, int> probs = new Dictionary<string, int>();

            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string netpath)
            {
                for (int i = 0; i < delfiles.Count; i++)
                {
                    dfs.DfsFile dfsnowball = delfiles[i];
                    string snowballname = dfsnowball.Name;

                    try
                    {
                        {
                            string fnwc = GetSnowballFilesWildcard(snowballname);
                            foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(fnwc))
                            {
                                try
                                {
                                    fi.Delete();
                                }
                                catch (Exception e)
                                {
                                    lock (probs)
                                    {
                                        if (!probs.ContainsKey(snowballname))
                                        {
                                            probs.Add(snowballname, 1);
                                        }
                                        else
                                        {
                                            probs[snowballname]++;
                                        }
                                    }
                                    LogOutputToFile(e.ToString());
                                }
                            }
                        }

                        {
                            try
                            {
                                System.IO.File.Delete(netpath + @"\zsballsample_" + snowballname + ".zsb");
                            }
                            catch (Exception e)
                            {
                                lock (probs)
                                {
                                    if (!probs.ContainsKey(snowballname))
                                    {
                                        probs.Add(snowballname, 1);
                                    }
                                    else
                                    {
                                        probs[snowballname]++;
                                    }
                                }
                                LogOutputToFile(e.ToString());
                            }
                        }

                        {
                            // zfoil cache!
                            string fnwc = GetSnowballFoilSampleFilesWildcard(snowballname);
                            try
                            {
                                foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(fnwc))
                                {
                                    try
                                    {
                                        fi.Delete();
                                    }
                                    catch (Exception e)
                                    {
                                        lock (probs)
                                        {
                                            if (!probs.ContainsKey(snowballname))
                                            {
                                                probs.Add(snowballname, 1);
                                            }
                                            else
                                            {
                                                probs[snowballname]++;
                                            }
                                        }
                                        LogOutputToFile(e.ToString());
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                LogOutputToFile(e.ToString());
                            }
                        }

                    }
                    catch (Exception e)
                    {
                        lock (probs)
                        {
                            if (!probs.ContainsKey(snowballname))
                            {
                                probs.Add(snowballname, 1);
                            }
                            else
                            {
                                probs[snowballname]++;
                            }
                        }
                        LogOutputToFile(e.ToString());
                    }
                }
            }
            ), netpaths, netpaths.Length);

            foreach (string f in probs.Keys)
            {
                Console.Error.WriteLine("{0} problems were encountered while deleting deltaCache: {1}", probs[f], f);
            }

            if (verbose)
            {
                foreach (dfs.DfsFile f in delfiles)
                {
                    if (!probs.ContainsKey(f.Name))
                    {
                        Console.WriteLine("DeltaCache deleted: {0}", f.Name);
                    }
                }
            }        
        }

        static void timethreadproc()
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


        public static void ExecOneMapReduce(string ExecOpts, SourceCode.Job cfgj, string[] ExecArgs, bool verbose)
        {
            ExecOneMapReduce(ExecOpts, cfgj, ExecArgs, verbose, verbose);
        }


        public static void ExecOneMapReduce(string ExecOpts, SourceCode.Job cfgj, string[] ExecArgs, bool verbose, bool verbosereplication)
        {
            MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength = int.MinValue;

            if (string.Compare(cfgj.IOSettings.OutputMethod, "grouped", StringComparison.OrdinalIgnoreCase) != 0 &&
              string.Compare(cfgj.IOSettings.OutputMethod, "sorted", StringComparison.OrdinalIgnoreCase) != 0 &&
              string.Compare(cfgj.IOSettings.OutputMethod, "hashsorted", StringComparison.OrdinalIgnoreCase) != 0 &&
              string.Compare(cfgj.IOSettings.OutputMethod, "dsorted", StringComparison.OrdinalIgnoreCase) != 0 &&
              string.Compare(cfgj.IOSettings.OutputMethod, "bsorted", StringComparison.OrdinalIgnoreCase) != 0 &&
              string.Compare(cfgj.IOSettings.OutputMethod, "rsorted", StringComparison.OrdinalIgnoreCase) != 0 &&
              string.Compare(cfgj.IOSettings.OutputMethod, "fsorted", StringComparison.OrdinalIgnoreCase) != 0 &&
              string.Compare(cfgj.IOSettings.OutputMethod, "rhashsorted", StringComparison.OrdinalIgnoreCase) != 0)
            {
                Console.Error.WriteLine("Unknown OutputMethod: {0}", cfgj.IOSettings.OutputMethod);
                SetFailure();
                return;
            }

            if (string.Compare(cfgj.IOSettings.OutputMethod, "hashsorted", StringComparison.OrdinalIgnoreCase) == 0 ||
                string.Compare(cfgj.IOSettings.OutputMethod, "rhashsorted", StringComparison.OrdinalIgnoreCase) == 0)
            {
                if (cfgj.IOSettings.KeyMajor != 1 && cfgj.IOSettings.KeyMajor != 2)
                {
                    Console.Error.WriteLine("Job/IOSettings/OutputMethod of hashsorted/rhashsorted can only be used with KeyMajor of 1 or 2");
                    SetFailure();
                    return;
                }
            }

            if (cfgj.Delta != null)
            {
                string reason = "";
                if (dfs.IsBadFilename(cfgj.Delta.Name, out reason))
                {
                    Console.Error.WriteLine("Invalid cache name: {0}", reason);
                    SetFailure();
                    return;
                }
            }
            MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputDirection = cfgj.IOSettings.OutputDirection;
            MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputDirection_ascending = (0 == string.Compare(cfgj.IOSettings.OutputDirection, "ascending", true));

#if DEBUG
            if(cfgj.Delta != null
                && !string.IsNullOrEmpty(cfgj.Delta.Name)
                && !string.IsNullOrEmpty(cfgj.Delta.DFSInput))
            {
                if (cfgj.IOSettings != null && !string.IsNullOrEmpty(cfgj.IOSettings.DFSInput))
                {
                    if (0 != SplitInputPaths(LoadDfsConfig(), cfgj.IOSettings.DFSInput).Count)
                    {
                        Console.Error.WriteLine("Cannot have both Delta/DFSInput and IOSettings/DFSInput");
                        SetFailure();
                        return;
                    }
                }
            }
#endif

            if (cfgj.IsDeltaSpecified)
            {
                if (cfgj.DynamicFoil != null)
                {
                    Console.Error.WriteLine("DynamicFoil not supported when Delta caching is enabled");
                    SetFailure();
                    return;
                }

                List<dfs.DfsFile> newsnowfiles = new List<dfs.DfsFile>();
                List<dfs.DfsFile.FileNode> newsnownodes = new List<dfs.DfsFile.FileNode>();
                List<string> newsnowfileswithnodes = new List<string>();
                List<int> nodesoffsets = new List<int>();
                List<int> reclens = new List<int>();
                using (LockDfsMutex())
                {
                    dfs dc = LoadDfsConfig();

                    /*if (dc.Replication > 1)
                    {
                        Console.Error.WriteLine("Error: caching cannot be used with replication");
                        SetFailure();
                        return;
                    }*/

                    dfs.DfsFile dfsnowball = DfsFind(dc, cfgj.Delta.Name, "zsb"); // Can be null!
                    int RecordLength = int.MinValue;
                    foreach (string _cachefile in SplitInputPaths(dc, cfgj.Delta.DFSInput))
                    {
                        string cachefile = _cachefile;
                        if (cachefile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                        {
                            cachefile = cachefile.Substring(6);
                        }
                        {
                            int ic = cachefile.IndexOf('@');
                            if (-1 != ic)
                            {
                                try
                                {
                                    int reclen = Surrogate.GetRecordSize(cachefile.Substring(ic + 1));
                                    if (RecordLength != int.MinValue && RecordLength != reclen)
                                    {
                                        Console.Error.WriteLine("Error: all cache inputs must have the same record length: {0}", cachefile);
                                        SetFailure();
                                        return;
                                    }
                                    RecordLength = reclen;
                                    cachefile = cachefile.Substring(0, ic);
                                }
                                catch (FormatException e)
                                {
                                    Console.Error.WriteLine("Error: cache input record length error: {0} ({1})", cachefile, e.Message);
                                    SetFailure();
                                    return;
                                }
                                catch (OverflowException e)
                                {
                                    Console.Error.WriteLine("Error: cache input record length error: {0} ({1})", cachefile, e.Message);
                                    SetFailure();
                                    return;
                                }
                            }
                            else
                            {
                                RecordLength = -1;
                            }
                        }
                        bool flakefound = false;
                        if (null != dfsnowball) // ...
                        {
                            for (int i = 0; i < dfsnowball.Nodes.Count; i++)
                            {
                                if (0 == string.Compare(dfsnowball.Nodes[i].Name, cachefile))
                                {
                                    flakefound = true;
                                    break;
                                }
                            }
                        }
                        if (!flakefound)
                        {
                            // New flake:
                            dfs.DfsFile df = null;
                            if (RecordLength > 0)
                            {
                                df = DfsFind(dc, cachefile, DfsFileTypes.BINARY_RECT);
                                if (null != df && RecordLength != df.RecordLength)
                                {
                                    Console.Error.WriteLine("Error: cache input file does not have expected record length of {0}: {1}@{2}", RecordLength, cachefile, df.RecordLength);
                                    SetFailure();
                                    return;
                                }
                            }
                            else
                            {
                                df = DfsFind(dc, cachefile);
                            }
                            if (null == df)
                            {
                                Console.Error.WriteLine("Cannot add file to delta; file does not exist in DFS: {0}", cachefile);
                                SetFailure();
                                return;
                            }
                            newsnowfiles.Add(df);
                            if (df.Nodes.Count > 0)
                            {
                                string dfsname = df.Name;
                                if (dfsname.StartsWith("dfs://"))
                                {
                                    dfsname = dfsname.Substring(6);
                                }
                                newsnowfileswithnodes.Add(dfsname);
                                nodesoffsets.Add(newsnownodes.Count);
                                reclens.Add(RecordLength);
                            }
                            newsnownodes.AddRange(df.Nodes);
                            if (verbose)
                            {
                                Console.WriteLine("Adding DFS file to delta: {0}", df.Name);
                            }
                        }
                    }
                    MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength = RecordLength;

                }
               
                if (0 != newsnownodes.Count)
                {
                    _ExecOneMapReduce(ExecOpts, cfgj, ExecArgs, verbose, verbosereplication, newsnownodes, newsnowfileswithnodes, nodesoffsets, reclens);
                    using (LockDfsMutex())
                    {
                        dfs dc = LoadDfsConfig();

                        dfs.DfsFile dfsnowball = DfsFind(dc, cfgj.Delta.Name, "zsb");
                        bool dfsnownew = false;
                        if (null == dfsnowball)
                        {
                            dfsnownew = true;
                            dfsnowball = new dfs.DfsFile();
                            dfsnowball.Name = cfgj.Delta.Name;
                            dfsnowball.Size = 0;
                            dfsnowball.Type = "zsb";
                            dfsnowball.Nodes = new List<dfs.DfsFile.FileNode>(newsnowfiles.Count);
                        }
                        for (int i = 0; i < newsnowfiles.Count; i++)
                        {
                            dfs.DfsFile.FileNode fn = new dfs.DfsFile.FileNode();
                            fn.Host = "";
                            fn.Length = 0;
                            fn.Position = 0;
                            fn.Name = newsnowfiles[i].Name;
                            dfsnowball.Nodes.Add(fn);
                        }
                        if (dfsnownew)
                        {
                            dc.Files.Add(dfsnowball);
                        }
                        UpdateDfsXml(dc);
                    }
                }
            }

            for (int nfailovers = 0;; nfailovers++)
            {
                if (nfailovers >= 5) // Max!
                {
                    throw new Exception("Too many attempts at failover; aborting job");
                }
                try
                {
                    _ExecOneMapReduce(ExecOpts, cfgj, ExecArgs, verbose, verbosereplication, null, null, null, null);
                }
                catch (ExceptionFailoverRetryable e)
                {
                    if (e.ShouldRetry)
                    {
                        LogOutputToFile("Failing over: " + e.ToString());
                        Console.WriteLine();
                        {
                            ConsoleColor oldc = ConsoleColor.Gray;
                            if (!isdspace)
                            {
                                oldc = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.DarkGreen;
                            }
                            Console.WriteLine("{0}Failing over...{1}", isdspace ? "\u00013" : "", isdspace ? "\u00010" : "");
                            if (!isdspace)
                            {
                                Console.ForegroundColor = oldc;
                            }
                        }
                        System.Threading.Thread.Sleep(1000 * 1);
                        Console.WriteLine();

                        IPAddressUtil.FlushCachedNames();
                        Surrogate.FlushCachedNetworkPaths();

                        continue; // !
                    }
                    else
                    {
                        LogOutputToFile("Not failing over, failover disabled: " + e.ToString());
                    }
                }
                break;
            }

        }

        public static string[] GetHealthPluginPaths(dfs dc)
        {
            string cacdir = null;
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
                string cddir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + @"\" + dfs.DLL_DIR_NAME;
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

            List<dfs.DfsFile> healthdlls = dc.FindAll("QizmtHealth*.dll");
            string[] pluginpaths = new string[healthdlls.Count];
            for (int pi = 0; pi < healthdlls.Count; pi++)
            {
                pluginpaths[pi] = cacdir + @"\" + healthdlls[pi].Name;
            }
            return pluginpaths;
        }

        // If AddCacheFiles is non-null, it's a ADD-CACHE-ONLY run!
        static void _ExecOneMapReduce(string ExecOpts, SourceCode.Job cfgj, string[] ExecArgs, bool verbose, bool verbosereplication, List<dfs.DfsFile.FileNode> AddCacheNodes, List<string> AddCacheDfsFileNames, List<int> AddCacheNodesOffsets, List<int> AddCacheNodesRecLengths)
        {
            bool extraverbose = true;
            try
            {
                string ev = Environment.GetEnvironmentVariable("EXTRA_VERBOSE");
                if (null != ev)
                {
                    extraverbose = 0 != string.Compare(ev, "false", true);
                }
            }
            catch
            {
            }            
           
            if (verbose)
            {
                string stype = "MapReduce";
                if (null != AddCacheNodes)
                {
                    stype = "DeltaCache";
                }
                Console.WriteLine("[{0}]        [{2}: {3}]", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, stype, cfgj.NarrativeName);

                if (extraverbose)
                {
                    //Console.WriteLine("    Legend: m = map done; e = exchange done; s = sort done; r = reduce done; *");
                    Console.WriteLine("    Legend: m = map done; e = exchange done; s = sort done; r = reduce done");
                }
            }

            string outputfile = cfgj.IOSettings.DFSOutput;
            string logname = Surrogate.SafeTextPath(cfgj.NarrativeName) + "_" + Guid.NewGuid().ToString() + ".j" + sjid + "_log.txt";

            {
                dfs dc = LoadDfsConfig(cfgj.IOSettings.GetSettingOverrideStrings());       
         
                FailoverInfo failover = null;

                if (cfgj.Computing != null && string.Compare(cfgj.Computing.Mode, "speculative", StringComparison.OrdinalIgnoreCase) == 0)
                {
                    if (cfgj.IsDeltaSpecified)
                    {
                        Console.Error.WriteLine("Error: Speculative computing does not support caching.");
                        SetFailure();
                        return;
                    }
                    if(string.Compare(cfgj.IOSettings.OutputMethod, "grouped", StringComparison.OrdinalIgnoreCase) != 0)
                    {
                        Console.Error.WriteLine("Error: Speculative computing supports only mapreduce with groupted OutputMethod.");
                        SetFailure();
                        return;
                    }
                    if (dc.Replication < 2)
                    {
                        Console.Error.WriteLine("Error: Speculative computing applies only when replication factor > 1.");
                        SetFailure();
                        return;
                    }
                    try
                    {
                        failover = new FailoverInfo(dc, cfgj.Computing.InputOrder);
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine("Error: Error while creating failover info instance: " + e.ToString());
                        SetFailure();
                        return;
                    }
                }      

                List<string> outputfiles = new List<string>();
                List<int> outputrecordlengths = new List<int>();
                List<int> inputrecordlengths = new List<int>();
                {                    
                    if (outputfile.Length > 0)
                    {
                        string[] ofiles = outputfile.Split(';');
                        for (int i = 0; i < ofiles.Length; i++)
                        {
                            string thisfile = ofiles[i].Trim();
                            if (thisfile.Length == 0)
                            {
                                continue;
                            }
                            int ic = thisfile.IndexOf('@');
                            int reclen = 0;
                            if (-1 != ic)
                            {
                                try
                                {
                                    reclen = Surrogate.GetRecordSize(thisfile.Substring(ic + 1));
                                    thisfile = thisfile.Substring(0, ic);
                                }
                                catch (FormatException e)
                                {
                                    Console.Error.WriteLine("Error: mapreduce output file record length error: {0} ({1})", thisfile, e.Message);
                                    SetFailure();
                                    return;
                                }
                                catch (OverflowException e)
                                {
                                    Console.Error.WriteLine("Error: mapreduce output file record length error: {0} ({1})", thisfile, e.Message);
                                    SetFailure();
                                    return;
                                }
                            }
                            else
                            {
                                reclen = -1;
                            }

                            if (thisfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                            {
                                thisfile = thisfile.Substring(6);
                            }
                            string reason = "";
                            if (dfs.IsBadFilename(thisfile, out reason))
                            {
                                Console.Error.WriteLine("Invalid output file: {0}, reason: {1}", thisfile, reason);
                                SetFailure();
                                return;
                            }
                            if (null != DfsFind(dc, thisfile))
                            {
                                Console.Error.WriteLine("Error:  output file already exists in DFS: {0}", thisfile);
                                SetFailure();
                                return;
                            }
                            outputfiles.Add(thisfile);
                            outputrecordlengths.Add(reclen);
                        }
                    }
                    else
                    {
                        outputfiles.Add("");
                        outputrecordlengths.Add(-1); 
                    }
                    MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength = outputrecordlengths.Count > 0 ? outputrecordlengths[0] : -1;                    
                }

                string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
                if (dc.Slaves.SlaveList.Length == 0 || slaves.Length < 1)
                {
                    throw new Exception("SlaveList expected in " + dfs.DFSXMLNAME);
                }
                int goodblockcount;
                if (0 == string.Compare(cfgj.IOSettings.OutputMethod, "grouped", StringComparison.OrdinalIgnoreCase))
                {
                    goodblockcount = dc.Blocks.TotalCount;
                }
                else
                {
                    goodblockcount = dc.Blocks.SortedTotalCount;
                }
                if (goodblockcount <= 0)
                {
                    throw new Exception("Invalid process count");
                }
                int removedslavescount = 0;
                if (dc.Replication > 1 || cfgj.IsJobFailoverEnabled)
                {
                    List<string> removedslaves = new List<string>();
                    slaves = ExcludeUnhealthySlaveMachines(slaves, removedslaves, true).ToArray();
                    removedslavescount = removedslaves.Count;
                    if (removedslaves.Count >= dc.Replication)
                    {
                        throw new Exception("Not enough healthy machines to run job (hit replication count)");
                    }
                    if (slaves.Length < 1)
                    {
                        throw new Exception("No healthy machines to run job");
                    }
                    if (removedslaves.Count > 0 && (null != AddCacheNodes || cfgj.IsDeltaSpecified))
                    {
                        // Don't failover if cache.
                        throw new Exception("Cluster must be healthy in order to use cache");
                    }
                }

                System.Threading.Thread timethread = new System.Threading.Thread(new System.Threading.ThreadStart(timethreadproc));
                timethread.Name = "MapReduceJobTime";
                try
                {
                    List<MapReduceBlockInfo> blocks = new List<MapReduceBlockInfo>(goodblockcount);
                    if (verbose)
                    {
                        Console.WriteLine("{0} processes on {1} machines:", goodblockcount, slaves.Length);
                    }

                    int zmaps = goodblockcount;
                    List<dfs.DfsFile.FileNode> mapinputchunks = null; // Data node chunks for every input file of this map job.
                    List<int> inputnodesoffsets = null;
                    List<string> mapfileswithnodes = null;
                    if (cfgj.MapReduce.Map.Length != 0)
                    {
                        if (cfgj.MapReduce.DirectSlaveLoad.Length != 0)
                        {
                            throw new Exception("Cannot have both DirectSlaveLoad and Map at this time");
                        }
                        /*if (cfgj.IOSettings.DFSInput.Trim().Length == 0)
                        {
                            if (null == AddCacheNodes)
                            {
                                Console.Error.WriteLine("No input files for Map (need config/IOSettings/DFSInput)");
                                if (verbose)
                                {
                                    Console.WriteLine("Aborting");
                                }
                                return; // ...
                            }
                        }*/
                        
                        {
                            mapinputchunks = new List<dfs.DfsFile.FileNode>();                            
                            if (null != AddCacheNodes)
                            {
                                mapinputchunks = AddCacheNodes;
                                mapfileswithnodes = AddCacheDfsFileNames;
                                inputnodesoffsets = AddCacheNodesOffsets;
                                inputrecordlengths = AddCacheNodesRecLengths;
                            }
                            else
                            {
                                IList<string> mapfiles = SplitInputPaths(dc, cfgj.IOSettings.DFSInput);
                                mapfileswithnodes = new List<string>(mapfiles.Count);
                                inputnodesoffsets = new List<int>(mapfiles.Count);
                                for (int i = 0; i < mapfiles.Count; i++)
                                {
                                    string dp = mapfiles[i];
                                    int inreclen;
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
                                                Console.Error.WriteLine("Error: map input record length error: {0} ({1})", dp, e.Message);
                                                SetFailure();
                                                return;
                                            }
                                            catch (OverflowException e)
                                            {
                                                Console.Error.WriteLine("Error: map input record length error: {0} ({1})", dp, e.Message);
                                                SetFailure();
                                                return;
                                            }
                                        }
                                        else
                                        {
                                            inreclen = -1;
                                        }
                                    }
                                    dfs.DfsFile df;
                                    if (inreclen > 0)
                                    {
                                        df = DfsFind(dc, dp, DfsFileTypes.BINARY_RECT);
                                        if (null != df && inreclen != df.RecordLength)
                                        {
                                            Console.Error.WriteLine("Error: map input file does not have expected record length of {0}: {1}@{2}", inreclen, dp, df.RecordLength);
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
                                        //throw new Exception("Map input file not found in DFS: " + dp);
                                        Console.Error.WriteLine("Map input file not found in DFS: {0}", dp);
                                        return;
                                    }
                                    if (df.Nodes.Count > 0)
                                    {
                                        mapfileswithnodes.Add(dp);
                                        inputnodesoffsets.Add(mapinputchunks.Count);
                                        inputrecordlengths.Add(inreclen);
                                        mapinputchunks.AddRange(df.Nodes);
                                    }                                    
                                    
                                }

                                MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength = inputrecordlengths.Count > 0 ? inputrecordlengths[0] : -1;

                            }
                        }
                    }
                    MapReduceBlockInfo.JobBlocksShared jobshared = new MapReduceBlockInfo.JobBlocksShared();
                    if (0 == string.Compare("sorted", dc.DefaultSortedOutputMethod, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new Exception("dfs/DefaultSortedOutputMethod cannot be 'sorted'");
                    }
                    {
                        jobshared.noutputmethod = cfgj.IOSettings.OutputMethod;
                        if (0 == string.Compare("sorted", jobshared.noutputmethod, true))
                        {
                            jobshared.noutputmethod = dc.DefaultSortedOutputMethod;
                        }
                    }
                    int FoilKeySkipFactor = -1;
                    if (0 == string.Compare(jobshared.noutputmethod, "rsorted")
                        || 0 == string.Compare(jobshared.noutputmethod, "fsorted"))
                    {
                        //FoilKeySkipFactor = blocks[0].acl.FoilKeySkipFactor;
                        {
                            long totinputsize = 0;
                            for (int i = 0; i < mapinputchunks.Count; i++)
                            {
                                totinputsize += mapinputchunks[i].Length;
                            }
                            const long TENGB = 10737418240;
                            long totinput_lines = totinputsize / cfgj.IOSettings.KeyLength;
                            long TENGB_lines = TENGB / cfgj.IOSettings.KeyLength;
                            long differencefactor = totinput_lines / TENGB_lines;
                            if (differencefactor < 1)
                            {
                                differencefactor = 1;
                            }
                            FoilKeySkipFactor = (int)(dc.slave.FoilBaseSkipFactor * differencefactor);

                            if (verbose)
                            {
                                // Foil distribution index algorithm:
                                // FoilBaseSkipFactor * max((totinputsize / keylength) / (TENGB / keylength), 1) = result
                                Console.WriteLine("    ({0} * max(({1} / {2}) / ({3} / {4}), 1)) = {5}",
                                    dc.slave.FoilBaseSkipFactor, totinputsize, cfgj.IOSettings.KeyLength, TENGB, cfgj.IOSettings.KeyLength, FoilKeySkipFactor);

                                int numfoilentries = checked((int)(totinputsize / cfgj.IOSettings.KeyLength / FoilKeySkipFactor));
                                Console.WriteLine("    Foil count = ~{0}", numfoilentries);
                                Console.WriteLine("    Foil size = ~{0}", GetFriendlyByteSize((long)numfoilentries * (long)cfgj.IOSettings.KeyLength));
                                Console.WriteLine("    Initial ZBlock total count = {0}", dc.slave.zblocks.count * goodblockcount);
                                if (cfgj.DynamicFoil != null)
                                {
                                    if (dc.slave.zblocks.count * goodblockcount < numfoilentries)
                                    {
                                        dc.slave.zblocks.count = numfoilentries / goodblockcount;
                                        if (0 != (numfoilentries % goodblockcount))
                                        {
                                            dc.slave.zblocks.count++;
                                        }
                                        Console.WriteLine("    Adjusted ZBlock total count = {0}", dc.slave.zblocks.count * goodblockcount);
                                    }
                                }

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
                                System.IO.File.WriteAllText(NetworkPathForHost(slaves[si]) + @"\slaveconfig.j" +  sjid + ".xml", slaveconfigxml);
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
                    List<string> allinputsamples = new List<string>(mapinputchunks.Count);
                    {
                        string sampcachename = null;
                        List<dfs.DfsFile.FileNode> sampleinputchunks;
                        if (cfgj.IsDeltaSpecified)
                        {
                            sampcachename = "zsballsample_" + cfgj.Delta.Name + ".zsb";
                            if (null == DfsFind(dc, cfgj.Delta.Name, "zsb"))
                            {
                                // First delta and first inputs...
                                sampleinputchunks = new List<dfs.DfsFile.FileNode>(0x400 * 4);
                                bool StripRecordInfo = true;
                                foreach (string fp in SplitInputPaths(dc, cfgj.Delta.DFSInput, StripRecordInfo))
                                {
                                    dfs.DfsFile df = DfsFindAny(dc, fp);
                                    if (null != df)
                                    {
                                        sampleinputchunks.AddRange(df.Nodes);
                                    }
                                }
                                foreach (string fp in SplitInputPaths(dc, cfgj.IOSettings.DFSInput, StripRecordInfo))
                                {
                                    dfs.DfsFile df = DfsFindAny(dc, fp);
                                    if (null != df)
                                    {
                                        sampleinputchunks.AddRange(df.Nodes);
                                    }
                                }
                            }
                            else
                            {
                                sampleinputchunks = null;
                            }
                        }
                        else
                        {
                            sampleinputchunks = mapinputchunks;
                        }
                        if (null != sampcachename)
                        {
                            allinputsamples.Add(">" + sampcachename);
                        }
                        if (null != sampleinputchunks)
                        {
                            for (int i = 0; i < sampleinputchunks.Count; i++)
                            {
                                if (dc.Replication > 1)
                                {
                                    allinputsamples.Add(dfs.MapNodeToNetworkStarPath(sampleinputchunks[i], true)); // samples=true
                                }
                                else
                                {
                                    allinputsamples.Add(dfs.MapNodeToNetworkPath(sampleinputchunks[i], true)); // samples=true
                                }
                            }
                        }
                    }
                    int nextslave = 0;
                    bool rangesort = 0 == string.Compare("rsorted", jobshared.noutputmethod, true)
                        || 0 == string.Compare("rhashsorted", jobshared.noutputmethod, true);
                    int perslave = 1;
                    int curslave = 0;
                    if (rangesort)
                    {
                        perslave = goodblockcount / slaves.Length;
                        if ((goodblockcount % slaves.Length) != 0)
                        {
                            perslave++;
                        }
                    }
#if DEBUG
                    //System.Threading.Thread.Sleep(1000 * 8);
#endif
                    jobshared.blockcount = goodblockcount;
                    jobshared.ExecOpts = ExecOpts;                    
                    if (failover == null)
                    {
                        for (int BlockID = 0; BlockID < goodblockcount; BlockID++)
                        {
                            string sblockid = BlockID.ToString();
                            MapReduceBlockInfo bi = new MapReduceBlockInfo();
                            bi.jobshared = jobshared;
                            bi.allinputsamples = allinputsamples;
                            //bi.CompressZMapBlocks = dc.CompressZMapBlocks;
                            bi.extraverbose = extraverbose;
                            bi.AddCacheOnly = null != AddCacheNodes;
                            bi.outputfiles = outputfiles;
                            bi.outputfile = outputfile;
                            //bi.blockcount = goodblockcount;
                            bi.basefilesize = dc.DataNodeBaseSize;
                            bi.cfgj = cfgj;
                            bi.BlockID = BlockID;
                            bi.SlaveHost = slaves[nextslave];
                            bi.SlaveIP = IPAddressUtil.GetIPv4Address(bi.SlaveHost);
                            bi.ExecArgs = ExecArgs;
                            if (rangesort)
                            {
                                if (++curslave >= perslave)
                                {
                                    nextslave++;
                                    curslave = 0;
                                }
                            }
                            else
                            {
                                if (++nextslave >= slaves.Length)
                                {
                                    nextslave = 0;
                                }
                            }
                            bi.logname = logname;
                            bi.acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList(cfgj.NarrativeName + "_BlockID" + sblockid, cfgj.IOSettings.KeyLength);
                            bi.acl.SetJID(jid, CurrentJobFileName + " MapReduce: " + cfgj.NarrativeName);
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif
                            int IntermediateDataAddressing = cfgj.IntermediateDataAddressing;
                            if (0 == IntermediateDataAddressing)
                            {
                                IntermediateDataAddressing = dc.IntermediateDataAddressing;
                            }
                            bi.acl.ValueOffsetSize = IntermediateDataAddressing / 8;
                            if (bi.acl.ValueOffsetSize <= 0)
                            {
                                throw new InvalidOperationException("Invalid value for IntermediateDataAddressing: " + IntermediateDataAddressing.ToString());
                            }
                            bi.acl.InputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                            bi.acl.OutputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength;
                            bi.acl.OutputRecordLengths = outputrecordlengths;
                            bi.acl.InputRecordLengths = new List<int>();// inputrecordlengths;
                            bi.acl.CookRetries = dc.slave.CookRetries;
                            bi.acl.CookTimeout = dc.slave.CookTimeout;
                            bi.acl.LocalCompile = (0 == BlockID); // Only compile first one locally.
                            //bi.acl.CompilerInvoked += new MySpace.DataMining.DistributedObjects5.ArrayComboList.CompilerInvokedEvent(_compilerinvoked);
                            bi.acl.BTreeCapSize = dc.BTreeCapSize;
                            MySpace.DataMining.DistributedObjects5.DistObject.FILE_BUFFER_SIZE = FILE_BUFFER_SIZE;
                            bi.acl.atype = atype;
                            bi.acl.DfsSampleDistance = dc.DataNodeBaseSize / dc.DataNodeSamples;
                            if (0 != cfgj.IOSettings.KeyMajor)
                            {
                                int mbc;
                                if (cfgj.IOSettings.KeyMajor < 0)
                                {
                                    mbc = cfgj.IOSettings.KeyLength + cfgj.IOSettings.KeyMajor;
                                }
                                else
                                {
                                    mbc = cfgj.IOSettings.KeyMajor;
                                }
                                if (mbc <= 0 || mbc > cfgj.IOSettings.KeyLength)
                                {
                                    throw new Exception("KeyMajor of " + cfgj.IOSettings.KeyMajor.ToString() + " is invalid for key length of " + cfgj.IOSettings.KeyLength.ToString());
                                }
                                bi.acl.KeyModByteCount = mbc;
                                /*if (verbose)
                                {
                                    Console.WriteLine("KeyModByteCount = {0}", mbc);
                                }*/
                            }
                            bi.slaveconfigxml = slaveconfigxml;
                            bi.acl.CompressFileOutput = dc.slave.CompressDfsChunks;
                            bi.acl.ZMapBlockCount = zmaps;
                            bi.verbose = verbose;
                            bi.acl.CompilerOptions = cfgj.IOSettings.CompilerOptions;
                            bi.acl.CompilerVersion = cfgj.IOSettings.CompilerVersion;
                            if (cfgj.AssemblyReferencesCount > 0)
                            {
                                cfgj.AddAssemblyReferences(bi.acl.CompilerAssemblyReferences, Surrogate.NetworkPathForHost(bi.SlaveHost));
                            }
                            if (cfgj.OpenCVExtension != null)
                            {
                                bi.acl.AddOpenCVExtension();
                            }
                            if (cfgj.Unsafe != null)
                            {
                                bi.acl.AddUnsafe();
                            }
                            bi.gencodectx();
                            bi.acl.AddBlock("1", "1", bi.SlaveHost + @"|" + bi.logname + @"|slaveid=0");
                            blocks.Add(bi);
                        }
                        timethread.Start();
                        if (null != mapinputchunks) // Only null if DirectSlaveLoad
                        {
                            int fslavetarget = 0; // Failover target if a particular chunk doesn't 'belong' to a slave...                       
                            string[] dfsfilenames = new string[blocks.Count];
                            string curfilename = null;
                            int curreclen = 0;
                            int curoffset = 0;
                            int fi = 0;
                            if (mapinputchunks.Count > 0)
                            {
                                curoffset = inputnodesoffsets[fi];
                            }
                            for (int mi = 0; mi < mapinputchunks.Count; mi++)
                            {
                                if (curoffset == mi)
                                {
                                    curfilename = mapfileswithnodes[fi];
                                    curreclen = inputrecordlengths[fi];
                                    if (++fi < inputnodesoffsets.Count)
                                    {
                                        curoffset = inputnodesoffsets[fi];
                                    }
                                }
                                int leastcount = int.MaxValue;
                                int leastindex = -1;
                                for (int BlockID = 0; BlockID < blocks.Count; BlockID++)
                                {
                                    if (0 == string.Compare(IPAddressUtil.GetName(mapinputchunks[mi].Host.Split(';')[0]), IPAddressUtil.GetName(blocks[BlockID].SlaveHost), StringComparison.OrdinalIgnoreCase))
                                    {
                                        if (blocks[BlockID].mapinputdfsnodes.Count < leastcount)
                                        {
                                            leastcount = blocks[BlockID].mapinputdfsnodes.Count;
                                            leastindex = BlockID;
                                        }
                                    }
                                }

                                try
                                {
                                    if (-1 == leastindex)
                                    {
                                        if (dc.Replication > 1)
                                        {
                                            // This chunk doesn't 'belong' to a slave, so round robin...
                                            if (null != AddCacheNodes || cfgj.IsDeltaSpecified)
                                            {
                                                // Don't failover if cache.
                                                throw new Exception("Cannot assign file part to another machine if cache is enabled");
                                            }
                                            fslavetarget++;
                                            if (fslavetarget >= blocks.Count)
                                            {
                                                fslavetarget = 0;
                                            }
                                            blocks[fslavetarget].mapinputdfsnodes.Add(dfs.MapNodeToNetworkStarPath(mapinputchunks[mi]));
                                            if (dfsfilenames[fslavetarget] != curfilename)
                                            {
                                                if (blocks[fslavetarget].mapinputfilenames == null)
                                                {
                                                    blocks[fslavetarget].mapinputfilenames = new List<string>();
                                                    blocks[fslavetarget].mapinputnodesoffsets = new List<int>();
                                                }
                                                int offset = blocks[fslavetarget].mapinputdfsnodes.Count - 1;
                                                blocks[fslavetarget].mapinputnodesoffsets.Add(offset);
                                                blocks[fslavetarget].mapinputfilenames.Add(curfilename);
                                                blocks[fslavetarget].acl.InputRecordLengths.Add(curreclen);
                                                dfsfilenames[fslavetarget] = curfilename;
                                            }
                                        }
                                        else
                                        {
                                            Console.Error.WriteLine("Warning: unable to assign chunk '{0}' to sub-process because replication is disabled", mapinputchunks[mi]);
                                        }
                                    }
                                    else
                                    {
                                        blocks[leastindex].mapinputdfsnodes.Add(dfs.MapNodeToNetworkStarPath(mapinputchunks[mi]));
                                        if (dfsfilenames[leastindex] != curfilename)
                                        {
                                            if (blocks[leastindex].mapinputfilenames == null)
                                            {
                                                blocks[leastindex].mapinputfilenames = new List<string>();
                                                blocks[leastindex].mapinputnodesoffsets = new List<int>();
                                            }
                                            int offset = blocks[leastindex].mapinputdfsnodes.Count - 1;
                                            blocks[leastindex].mapinputnodesoffsets.Add(offset);
                                            blocks[leastindex].mapinputfilenames.Add(curfilename);
                                            blocks[leastindex].acl.InputRecordLengths.Add(curreclen);
                                            dfsfilenames[leastindex] = curfilename;
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    LogOutput("Map input error with input file '" + mapinputchunks[mi].Name + "': " + e.ToString());
                                }
                            }
                            //System.Threading.Thread.Sleep(8000);
                            {
                                int nparts = zmaps / blocks.Count;
                                if ((zmaps % blocks.Count) != 0)
                                {
                                    nparts++;
                                }
                                int izm = 0; // Current zmapblock index; up to zmaps.
                                for (int i = 0; i < blocks.Count; i++)
                                {
                                    int izmend = izm + nparts;
                                    if (izmend > zmaps)
                                    {
                                        izmend = zmaps;
                                    }
                                    for (; izm < izmend; izm++)
                                    {
                                        blocks[i].ownedzmapblockIDs.Add(izm);
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        FailoverInfo.JobInfo jobinfo = new FailoverInfo.JobInfo();
                        jobinfo.allinputsamples = allinputsamples;
                        jobinfo.cfgj = cfgj;
                        jobinfo.dc = dc;
                        jobinfo.execargs = ExecArgs;
                        jobinfo.extraverbose = extraverbose;
                        jobinfo.inputnodesoffsets = inputnodesoffsets;
                        jobinfo.inputrecordlengths = inputrecordlengths;
                        jobinfo.jobshared = jobshared;
                        jobinfo.logname = logname;
                        jobinfo.mapfileswithnodes = mapfileswithnodes;
                        jobinfo.mapinputchunks = mapinputchunks;
                        jobinfo.outputfile = outputfile;
                        jobinfo.outputfiles = outputfiles;
                        jobinfo.outputrecordlengths = outputrecordlengths;
                        jobinfo.slaveconfigxml = slaveconfigxml;
                        jobinfo.verbose = verbose;
                        jobinfo.timethread = timethread;
                        failover.CreateBlocks(slaves, blocks, goodblockcount, 0, jobinfo);
#if FAILOVER_DEBUG
                        {
                            failover.Log("Done CreateBlocks()...");
                            string debugtxt = "==========failover.allBlocks==========" + Environment.NewLine;
                            foreach (MapReduceBlockInfo bl in failover.allBlocks)
                            {
                                debugtxt += Environment.NewLine +
                                    "****blockid=" + bl.BlockID.ToString() + ";blockcid=" + bl.BlockCID.ToString()
                                    + ";host=" + bl.SlaveHost + Environment.NewLine +
                                    string.Join(";", bl.mapinputdfsnodes.ToArray()) + Environment.NewLine;
                            }
                            failover.Log(debugtxt);
                        }
                        {
                            string debugtxt = "==========Blockstatus==========" + Environment.NewLine;
                            lock (failover.blockStatus)
                            {
                                foreach (KeyValuePair<int, int> pair in failover.blockStatus)
                                {
                                    debugtxt += "****blockcid=" + pair.Key.ToString() + ";status=" + pair.Value.ToString() + Environment.NewLine;
                                }
                            }
                            failover.Log(debugtxt);
                        }
                        failover.Log("FailoverTimeout=" + dc.FailoverTimeout.ToString() + ";FailoverDoCheck=" + dc.FailoverDoCheck.ToString());
#endif
                    }         
                    
                    if (0 == string.Compare(jobshared.noutputmethod, "rsorted")
                        || 0 == string.Compare(jobshared.noutputmethod, "fsorted"))
                    {
                        // fsorted has another 'foil load' phase with join...
                        if (-1 == FoilKeySkipFactor)
                        {
                            FoilKeySkipFactor = blocks[0].acl.FoilKeySkipFactor;
                        }
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            MapReduceBlockInfo bi = blocks[i];
                            bi.acl.FoilKeySkipFactor = FoilKeySkipFactor;
                            bi.all = blocks;
                            bi.thread = new System.Threading.Thread(new System.Threading.ThreadStart(bi.fsortedthreadproc));
                            bi.thread.Name = "MapReduceJobBlock" + bi.BlockID + "_fsorted";
                            bi.thread.IsBackground = true;
                            AELight_StartTraceThread(bi.thread);
                        }
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            MapReduceBlockInfo bi = blocks[i];
                            AELight_JoinTraceThread(bi.thread);
                        }
                        if (verbose)
                        {
                            Console.WriteLine((extraverbose ? "\r\n" : "") + "    [{0}]        Distribution index done; starting map", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
                        }
                    }
                    
                    if (failover == null)
                    {
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            if (null != mapinputchunks) // Only null if DirectSlaveLoad
                            {
                                if (blocks[i].mapinputdfsnodes.Count == 0)
                                {
#if DEBUGoff
                                Console.Error.WriteLine("Warning: no data nodes for sub process on host '" + blocks[i].SlaveIP + "' (" + blocks[i].SlaveHost + ")");
#endif
                                }
                            }
                            MapReduceBlockInfo bi = blocks[i];
                            bi.all = blocks;
                            bi.thread = new System.Threading.Thread(new System.Threading.ThreadStart(bi.firstthreadproc));
                            bi.thread.Name = "MapReduceJobBlock" + bi.BlockID + "_map";
                            bi.thread.IsBackground = true;
                            AELight_StartTraceThread(bi.thread);
                        }

                        {
                            Exception ee = null;
                            for (int i = 0; i < blocks.Count; i++)
                            {
                                AELight_JoinTraceThread(blocks[i].thread);
                                // If failover, check failures..
                                if (cfgj.IsJobFailoverEnabled && blocks[i].blockfail)
                                {
                                    ee = blocks[i].LastThreadException;
                                    if (!CanClientExceptionFailover(ee))
                                    {
                                        throw ee; // Throw the worst one.
                                    }
                                }
                            }
                            if (null != ee)
                            {
                                if (cfgj.IsJobFailoverEnabled)
                                {
                                    throw new ExceptionFailoverRetryable(ee, cfgj.IsJobFailoverEnabled && CheckFoFilesShouldFailover(slaves, logname));
                                }
                            }
                        }
                    }
                    else
                    {
                        foreach (MapReduceBlockInfo bl in failover.allBlocks)
                        {
                            bl.all = blocks; 
                            bl.thread = new System.Threading.Thread(new System.Threading.ThreadStart(bl.firstthreadproc));
                            bl.thread.Name = "MapReduceJobBlock" + bl.BlockID + "_map";
                            bl.thread.IsBackground = true;
                            AELight_StartTraceThread(bl.thread);
                        }
                        
                        for (; ; )
                        {
#if FAILOVER_DEBUG       
                            {
                                failover.Log("Loop.  Sleepcnt=" + failover.sleepCnt.ToString());
                                string debugtxt = "==========Blockstatus==========" + Environment.NewLine;
                                lock (failover.blockStatus)
                                {
                                    foreach (KeyValuePair<int, int> pair in failover.blockStatus)
                                    {
                                        debugtxt += "****blockcid=" + pair.Key.ToString() + ";status=" + pair.Value.ToString() + Environment.NewLine;
                                    }
                                }                                
                                failover.Log(debugtxt);
                            }
#endif

                            if (failover.AllBlocksCompleted(1))
                            {
#if FAILOVER_DEBUG
                                failover.Log("All blocks completed.  Breaking out of loop...");
#endif
                                break;
                            }

                            System.Threading.Thread.Sleep(dc.FailoverTimeout);

                            if (failover.sleepCnt++ > dc.FailoverDoCheck)
                            {
                                failover.sleepCnt = 0;
#if FAILOVER_DEBUG
                                failover.Log("Health check;sleepCnt=" + failover.sleepCnt.ToString());
#endif

                                if (failover.CheckMachineFailures() > 0)
                                {
#if FAILOVER_DEBUG
                                    failover.Log("Disk failure detected...");
                                    {
                                        string debugtxt = "======Bad hosts found=======" + Environment.NewLine +
                                            string.Join(";", failover.newBadHosts.ToArray());
                                        failover.Log(debugtxt);
                                    }   
#endif

                                    {
                                        int hoffset = (failover.badHosts.Count - failover.newBadHosts.Count) + removedslavescount;
                                        string recovered = failover.badHosts.Count + removedslavescount >= dc.Replication ? "NoRecovery" : "Recovered";
                                        for (int hi = 0; hi < failover.newBadHosts.Count; hi++)
                                        {
                                            string badhost = failover.newBadHosts[hi];
                                            Console.WriteLine(Environment.NewLine + "HWFailure:{0}:{1}:{2}/{3}", recovered, badhost, hi + 1 + hoffset, dc.Replication);
                                            ConsoleFlush();
                                            failover.AbortBlocksFromFailedHost(badhost);
                                        }
                                    }
                                    
#if FAILOVER_DEBUG                                    
                                    {
                                        failover.Log("Done removing all bad blocks");
                                        string debugtxt = "";
                                        failover.Log("========failover.allblocks========");
                                        foreach (MapReduceBlockInfo bl in failover.allBlocks)
                                        {
                                            if (bl != null)
                                            {
                                                debugtxt += Environment.NewLine + 
                                                "****blockid=" + bl.BlockID.ToString() + ";blockcid=" + bl.BlockCID.ToString()
                                                + ";host=" + bl.SlaveHost + Environment.NewLine +
                                                string.Join(";", bl.mapinputdfsnodes.ToArray()) + Environment.NewLine;
                                            }
                                        }
                                        failover.Log(debugtxt);
                                    }
                                    {
                                        string debugtxt = "==========Blockstatus==========" + Environment.NewLine;
                                        lock (failover.blockStatus)
                                        {
                                            foreach (KeyValuePair<int, int> pair in failover.blockStatus)
                                            {
                                                debugtxt += "****blockcid=" + pair.Key.ToString() + ";status=" + pair.Value.ToString() + Environment.NewLine;
                                            }
                                        }                                        
                                        failover.Log(debugtxt);
                                        failover.Log("hostToBlocksCount=" + failover.hostToBlocks.Count.ToString());
                                    }
#endif
                                }
                            }
                        }
                                         
                        if (failover.CheckMachineFailures() > 0)
                        {
                            int hoffset = (failover.badHosts.Count - failover.newBadHosts.Count) + removedslavescount;
                            string recovered = failover.badHosts.Count + removedslavescount >= dc.Replication ? "NoRecovery" : "Recovered";
                            for (int hi = 0; hi < failover.newBadHosts.Count; hi++)
                            {
                                string badhost = failover.newBadHosts[hi];                                
                                Console.WriteLine(Environment.NewLine + "HWFailure:{0}:{1}:{2}/{3}", recovered, badhost, hi + 1 + hoffset, dc.Replication);                               
                                failover.AbortBlocksFromFailedHost(badhost);
                            }
                        }

                        if (failover.badHosts.Count > 0)
                        {
                            string sbadhosts = "";
                            foreach (string badhost in failover.badHosts.Keys)
                            {
                                if (sbadhosts.Length > 0)
                                {
                                    sbadhosts += ", ";
                                }
                                sbadhosts += badhost;
                            }
                            ConsoleColor oldcolor = Console.ForegroundColor;
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine(Environment.NewLine + 
                                "{0}{1} machine(s) with hardware failure during map: {2}{3}", isdspace ? "\u00014" : "", failover.badHosts.Count, sbadhosts, isdspace ? "\u00010" : "");                            
                            Console.ForegroundColor = oldcolor;
                        }

                        for (int bi = 0; bi < failover.allBlocks.Length; bi++)
                        {
                            MapReduceBlockInfo bl = failover.allBlocks[bi];
                            if (bl != null)
                            {
                                AELight_JoinTraceThread(bl.thread);
                            }
                        }                  
#if FAILOVER_DEBUG
                        failover.Log("All blocks joined.");
#endif
                        if (failover.badHosts.Count + removedslavescount >= dc.Replication)
                        {
                            failover.CloseAllBlocks();
                            throw new Exception("Error: Cannot continue to exchange/sort/reduce phase.  The number of machines with hardware failure (" + (failover.badHosts.Count + removedslavescount).ToString() + ") is greater than or equal to replication factor (" + dc.Replication.ToString() + ").");
                        }
                      
                        for (int bi = 0; bi < blocks.Count; bi++)
                        {
                            MapReduceBlockInfo bl = blocks[bi];
                            if (failover.allBlocks[bl.BlockCID] == null || bl.blockfail)
                            {
                                bool foundgoodblock = false;
                                for (int ri = 0; ri < dc.Replication - 1; ri++)
                                {
                                    int nextrepblockcid = (ri + 1) * blocks.Count + bl.BlockCID;
                                    MapReduceBlockInfo nextrepblock = failover.allBlocks[nextrepblockcid];
                                    if (nextrepblock != null && !nextrepblock.blockfail)
                                    {
                                        foundgoodblock = true;
                                        blocks[bi] = nextrepblock;
                                        break;
                                    }
                                }
                                if (!foundgoodblock)
                                {
                                    failover.CloseAllBlocks();
                                    throw new Exception("Error: Cannot find a good replicated map block to replace the failed block.  Block index = " + bi.ToString());
                                }
                            }
                        }
#if FAILOVER_DEBUG
                        {
                            failover.Log("=======Blocks going forward to exchange=========");
                            string debugtxt = "";
                            foreach (MapReduceBlockInfo bl in blocks)
                            {
                                debugtxt += Environment.NewLine +
                                    "****blockid=" + bl.BlockID.ToString() + ";blockcid=" + bl.BlockCID.ToString() + ";host=" + bl.SlaveHost +
                                    ";inputfiles=" + string.Join(";", bl.mapinputdfsnodes.ToArray()) + Environment.NewLine;     
                            }
                            failover.Log(debugtxt);
                        }
#endif
                    }                    
                    
                    if (null != mapinputchunks) // Only null if DirectSlaveLoad
                    {
                        if (verbose)
                        {
                            Console.WriteLine((extraverbose ? "\r\n" : "") + "    [{0}]        Map done; starting map exchange", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
                        }
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            blocks[i].thread = new System.Threading.Thread(new System.Threading.ThreadStart(blocks[i].exchangethreadproc));
                            blocks[i].thread.Name = "MapReduceJobBlock" + blocks[i].BlockID + "_aftermap";
                            blocks[i].thread.IsBackground = true;
                            AELight_StartTraceThread(blocks[i].thread);
                        }
                        {
                            Exception ee = null;
                            for (int i = 0; i < blocks.Count; i++)
                            {
                                AELight_JoinTraceThread(blocks[i].thread);
                                // If failover, check failures..
                                if (cfgj.IsJobFailoverEnabled && blocks[i].blockfail)
                                {
                                    ee = blocks[i].LastThreadException;
                                    if (!CanClientExceptionFailover(ee))
                                    {
                                        throw ee; // Throw the worst one.
                                    }
                                }
                            }
                            if (null != ee)
                            {
                                if (cfgj.IsJobFailoverEnabled)
                                {
                                    throw new ExceptionFailoverRetryable(ee, cfgj.IsJobFailoverEnabled && CheckFoFilesShouldFailover(slaves, logname));
                                }
                            }
                        }
                        int TotalZBlockSplits = 0;
                        checked
                        {
                            for (int i = 0; i < blocks.Count; i++)
                            {
                                TotalZBlockSplits += blocks[i].ZBlockSplitCount;
                            }
                        }
                        if (failover == null)
                        {
                            for (int i = 0; i < blocks.Count; i++)
                            {
                                blocks[i].acl.StopZMapBlockServer();
                                blocks[i].acl.Close();
                            }
                        }
                        else
                        {
                            failover.CloseAllBlocks();
#if FAILOVER_DEBUG
                            failover.Log("done StopZMapBlockServer()");
#endif
                        }
                        
                        if (verbose)
                        {
                            Console.WriteLine(); // Separate the 'r's and stuff from following stuff.
                        }
                        if (verbose)
                        {
                            if (TotalZBlockSplits > 0)
                            {
                                Console.WriteLine("{1}Split count: {0}{2}", TotalZBlockSplits, isdspace ? "\u00014" : "", isdspace ? "\u00010" : "");
                            }
                        }
                        if (null == AddCacheNodes && outputfiles.Count > 0) // If not just caching...
                        {
                            bool anyoutput = false;
                            {
                                List<string> dfsnames = new List<string>();
                                List<string> dfsnamesreplicating = new List<string>();
                                // Reload DFS config to make sure changes since starting get rolled in, and make sure the output file wasn't created in that time...
                                using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                                {
                                    dc = LoadDfsConfig(); // Reload in case of change or user modifications.
                                    {
                                        for (int nfile = 0; nfile < outputfiles.Count; nfile++)
                                        {
                                            dfs.DfsFile df = new dfs.DfsFile();
                                            string ofile = outputfiles[nfile];
                                            if (ofile.Length == 0)
                                            {
                                                continue;
                                            }
                                            if (outputrecordlengths[nfile] > 0)
                                            {
                                                df.XFileType = DfsFileTypes.BINARY_RECT + "@" + outputrecordlengths[nfile].ToString();
                                            }
                                            df.Nodes = new List<dfs.DfsFile.FileNode>();
                                            df.Size = -1; // Preset
                                            string dfsname = ofile;
                                            if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                            {
                                                dfsname = dfsname.Substring(6);
                                            }
                                            string dfsnamereplicating = ".$" + dfsname + ".$replicating-" + Guid.NewGuid().ToString();
                                            df.Name = dfsnamereplicating;
                                            if (null != DfsFind(dc, df.Name))
                                            {
                                                Console.Error.WriteLine("Error:  output file '{0}' was created during job: " + df.Name, ofile);
                                                continue;
                                            }
                                            dfsnames.Add(dfsname);
                                            dfsnamesreplicating.Add(dfsnamereplicating);
                                            long totalsize = 0;
                                            bool anybad = false;
                                            bool foundzero = false;
                                            for (int i = 0; i < blocks.Count; i++)
                                            {
                                                MapReduceBlockInfo block = blocks[i];
                                                List<string> nodes = block.reduceoutputdfsnodeses[nfile];
                                                List<long> sizes = block.reduceoutputsizeses[nfile];
                                                if (nodes.Count != sizes.Count)
                                                {
                                                    Console.Error.WriteLine("Warning: chunk accounting error");
                                                }
                                                for (int j = 0; j < nodes.Count; j++)
                                                {
                                                    dfs.DfsFile.FileNode fn = new dfs.DfsFile.FileNode();
                                                    fn.Host = block.SlaveHost;
                                                    fn.Name = nodes[j];
                                                    df.Nodes.Add(fn);
                                                    fn.Length = -1; // Preset
                                                    fn.Position = -1; // Preset
                                                    if (anybad)
                                                    {
                                                        continue;
                                                    }
                                                    fn.Position = totalsize; // Position must be set before totalsize updated!
                                                    if (j >= sizes.Count)
                                                    {
                                                        Console.Error.WriteLine("Warning: size not provided for data node chunk from host " + fn.Host);
                                                        anybad = true;
                                                        continue;
                                                    }
                                                    if (0 == sizes[j])
                                                    {
                                                        if (!foundzero)
                                                        {
                                                            foundzero = true;
                                                            Console.Error.WriteLine("Warning: zero-size data node chunk encountered from host " + fn.Host);
                                                        }
                                                    }
                                                    fn.Length = sizes[j];
                                                    totalsize += sizes[j];
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
                                            // Always produce output file, even if no data.
                                            dc.Files.Add(df);
                                        }
                                        UpdateDfsXml(dc);
                                    }
                                }
                                {
#if MR_REPLICATION_TIME_PRINT
                                    DateTime replstart = DateTime.Now;
#endif
                                    string[] replicatehosts = slaves;
                                    if (failover != null)
                                    {
                                        if (failover.badHosts.Count > 0)
                                        {
                                            replicatehosts = new List<string>(failover.goodHosts.Keys).ToArray();
                                        }                                        
                                    }
                                    if (ReplicationPhase(verbosereplication, jobshared.blockcount, replicatehosts, dfsnamesreplicating))
                                    {
#if MR_REPLICATION_TIME_PRINT
                                        int replsecs = (int)Math.Round((DateTime.Now - replstart).TotalSeconds);
                                        ConsoleColor oldcolor = Console.ForegroundColor;
                                        Console.ForegroundColor = ConsoleColor.Green;
                                        Console.WriteLine("{0}[replication completed {1}]{2}", isdspace ? "\u00011" : "", DurationString(replsecs), isdspace ? "\u00010" : "");
                                        Console.ForegroundColor = oldcolor;
#endif
                                    }
                                }
                                using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                                {
                                    dc = LoadDfsConfig(); // Reload in case of change or user modifications.
                                    for (int nfile = 0; nfile < dfsnames.Count; nfile++)
                                    {
                                        string dfsname = dfsnames[nfile];
                                        string dfsnamereplicating = dfsnamesreplicating[nfile];
                                        dfs.DfsFile dfu = dc.FindAny(dfsnamereplicating);
                                        if (null != dfu)
                                        {
                                            if (null != DfsFindAny(dc, dfsname))
                                            {
                                                Console.Error.WriteLine("Error:  output file '{0}' was created during job", dfsname);
                                                continue;
                                            }
                                            dfu.Name = dfsname;
                                        }
                                    }
                                    UpdateDfsXml(dc);
                                }
                            }
                            if (!anyoutput && verbose)
                            {
                                Console.Write(" (no DFS output) ");
                                ConsoleFlush();
                            }
                        }
                    }
                    else
                    {
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            blocks[i].acl.Close();
                        }
                    }
                }
                catch (Exception e)
                {
                    if (CanClientExceptionFailover(e))
                    {
                        LogOutput(e.ToString());
                        // Don't check CheckFoFilesShouldFailover() here because these exceptions aren't about the slave's processing.
                        if (cfgj.IsJobFailoverEnabled)
                        {
                            throw new ExceptionFailoverRetryable(e, cfgj.IsJobFailoverEnabled);
                        }
                    }
                    throw;
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

                    timethread.Abort();
                    DeleteRemoteFoFiles(slaves, logname);
                    AELight.CheckUserLogs(slaves, logname);
                }

                if (verbose && null == AddCacheNodes)
                {
                    Console.WriteLine();
                    Console.WriteLine("[{0}]        Done", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
                    Console.WriteLine("Output:   {0}", cfgj.IOSettings.DFSOutput);
                }
            }

        }

        static void DeleteRemoteFoFiles(IList<string> hosts, string logfilename)
        {
            for (int i = 0; i < hosts.Count; i++)
            {
                try
                {
                    string np = NetworkPathForHost(hosts[i]) + @"\" + logfilename + ".fo";
                    System.IO.File.Delete(np);
                }
                catch
                {
                }
            }
        }

        static bool CheckFoFilesShouldFailover(IList<string> hosts, string logfilename)
        {
            for (int ih = 0; ih < hosts.Count; ih++)
            {
                try
                {
                    string np = NetworkPathForHost(hosts[ih]) + @"\" + logfilename + ".fo";
                    string[] lines = System.IO.File.ReadAllLines(np);
                    try
                    {
                    }
                    catch
                    {
                        System.IO.File.Delete(np);
                    }
                    for (int iline = 0; ih < lines.Length; iline++)
                    {
                        string line = lines[iline];
                        if (line.Length > 1)
                        {
                            switch (line[0])
                            {
                                case 'r': // Recoverable.
                                    // Good, keep going..
                                    break;

                                //case 'x': // Not recoverable.
                                default:
                                    DeleteRemoteFoFiles(hosts, logfilename);
                                    return false;
                            }
                        }
                    }
                }
                catch (System.IO.IOException ioex)
                {
                    // File not there or unreachable, keep going..
                }
            }
            return true;
        }


        static void _compilerinvoked(string action, bool complete)
        {
            Console.Write(complete ? 'b' : 'B');
        }


    }
}
