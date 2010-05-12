#define FAILOVER_TEST
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
            internal Dictionary<int, int> blockStatus = null;
            internal List<string> newBadHosts = null;
            internal MySpace.DataMining.DistributedObjects.DiskCheck diskcheck = null;
            internal Random rnd = null;
            internal uint awakeCnt = 0;
            internal string[] healthpluginpaths = null;
            internal List<MapReduceBlockInfo> workingBlocks = null;
            internal Dictionary<string, List<MapReduceBlockInfo>> hostToESRBlocks = null;
            internal FailoversShared failoverShared = null;
            internal int blockCount = 0;
            internal int cID = 0;
            internal Exception LastException;
            internal Dictionary<int, FailoverInfo> childFailovers;

            Dictionary<string, List<ReplWorker>> hostToReplWorkers = null;
            internal Dictionary<string, List<string>> dfsnamesToReplnames = null;
            internal Dictionary<string, int> badOutputFilenodes = null;

            internal FailoverInfo(dfs dc)
            {
                healthpluginpaths = GetHealthPluginPaths(dc);
                diskcheck = new MySpace.DataMining.DistributedObjects.DiskCheck(healthpluginpaths);
                rnd = new Random(unchecked(DateTime.Now.Millisecond + System.Diagnostics.Process.GetCurrentProcess().Id));
            }

            internal void CreateBlocks(int blockcount, List<string> mapinputfilepaths, List<string> mapinputfilenames, List<int> mapinputoffsets, List<int> mapinputreclengths, FailoversShared failovershared, string[] goodhosts, string[] badhosts, bool rehash)
            {
#if FAILOVER_DEBUG
                {
                    string debugtxt = Environment.NewLine + "Begin CreateBlocks:" + Environment.NewLine +
                    "blockcount=" + blockcount.ToString() + Environment.NewLine +
                     "rehash=" + rehash.ToString() + Environment.NewLine +
                     "goodhosts=" + string.Join(";", goodhosts) + Environment.NewLine +
                     "badhosts=" + string.Join(";", badhosts) + Environment.NewLine;                    
                    Log(debugtxt);
                }
#endif

                blockCount = blockcount;
                failoverShared = failovershared;
                int allblockscount = blockcount * failovershared.dc.Replication;
                int blocksperhost = allblockscount / goodhosts.Length;
                if ((blocksperhost * goodhosts.Length) != allblockscount)
                {
                    blocksperhost++;
                }
                hostToBlocks = new Dictionary<string, List<MapReduceBlockInfo>>(goodhosts.Length);
                allBlocks = new MapReduceBlockInfo[allblockscount];
                goodHosts = new Dictionary<string, int>(goodhosts.Length);
                blockStatus = new Dictionary<int, int>(allblockscount);
                badHosts = new Dictionary<string, int>(goodhosts.Length + badhosts.Length);
                newBadHosts = new List<string>(goodhosts.Length);
                workingBlocks = new List<MapReduceBlockInfo>(blockcount);
                childFailovers = new Dictionary<int, FailoverInfo>(failovershared.dc.Replication);
                hostToESRBlocks = new Dictionary<string, List<MapReduceBlockInfo>>(goodhosts.Length);

                foreach (string host in goodhosts)
                {
                    goodHosts.Add(host.ToLower(), 0);
                }

                foreach (string host in badhosts)
                {
                    badHosts.Add(host.ToLower(), 0);
                }

                for (int ri = 0; ri < failovershared.dc.Replication; ri++)
                {
                    for (int bi = 0; bi < blockcount; bi++)
                    {
                        MapReduceBlockInfo block = new MapReduceBlockInfo();
                        block.BlockID = bi;
                        block.BlockCID = ri * blockcount + block.BlockID;
                        cID = block.BlockCID;
                        block.rehash = rehash;
                        block.failover = this;
                        allBlocks[block.BlockCID] = block;
                        blockStatus.Add(block.BlockCID, 0);
                    }
                }
                cID++;

                MapReduceBlockInfo[] firstset = new MapReduceBlockInfo[blockcount];
                {
                    Dictionary<string, Dictionary<int, MapReduceBlockInfo>> hostToBlockIDs = new Dictionary<string, Dictionary<int, MapReduceBlockInfo>>(goodhosts.Length);
                    int nexthost = 0;
                    List<MapReduceBlockInfo> collisions = new List<MapReduceBlockInfo>(blockcount);
                    Dictionary<string, Dictionary<int, MapReduceBlockInfo>> hostToBlockIDsPerRep = new Dictionary<string, Dictionary<int, MapReduceBlockInfo>>(goodhosts.Length);

                    for (int ri = 0; ri < failovershared.dc.Replication; ri++)
                    {
#if FAILOVER_DEBUG
                        Log("Assigning hosts to blocks in replication index = " + ri.ToString());
#endif

                        hostToBlockIDsPerRep.Clear();

                        MapReduceBlockInfo[] scrambled = new MapReduceBlockInfo[blockcount]; //!
                        for (int bi = 0; bi < scrambled.Length; bi++)
                        {
                            scrambled[bi] = allBlocks[bi + ri * blockcount];
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
                            /*string debugtxt = "firstset:" + Environment.NewLine;
                            foreach (MapReduceBlockInfo bl in firstset)
                            {
                                debugtxt += bl.BlockID.ToString() + ":" + bl.BlockCID.ToString() + ":" + (bl.SlaveHost == null ? "null" : bl.SlaveHost) + Environment.NewLine;
                            }
                            Log(debugtxt);*/
                        }
#endif

                        int tryremains = blockcount;
                        for (; ; )
                        {
                            AssignBlocksToHosts(scrambled, goodhosts, ref nexthost, hostToBlockIDs, collisions, hostToBlockIDsPerRep, blocksperhost, blockcount);
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

                MapReduceBlockInfo.JobBlocksShared jobshared = new MapReduceBlockInfo.JobBlocksShared();
                jobshared.noutputmethod = failovershared.cfgj.IOSettings.OutputMethod;
                jobshared.blockcount = blockcount;
                jobshared.ExecOpts = failovershared.execopts;
                foreach (MapReduceBlockInfo block in allBlocks)
                {
                    block.jobshared = jobshared;
                    block.allinputsamples = null;
                    block.extraverbose = failovershared.extraverbose;
                    block.AddCacheOnly = false;
                    block.outputfiles = failovershared.outputfiles;
                    block.outputfile = failovershared.outputfile;
                    block.basefilesize = failovershared.dc.DataNodeBaseSize;
                    block.cfgj = failovershared.cfgj;
                    block.SlaveIP = IPAddressUtil.GetIPv4Address(block.SlaveHost);
                    block.ExecArgs = failovershared.execargs;
                    block.logname = failovershared.logname;
                    block.acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList(failovershared.cfgj.NarrativeName + "_BlockID" + block.BlockID.ToString(), failovershared.cfgj.IOSettings.KeyLength);
                    block.acl.SetJID(jid);
                    block.acl.HealthPluginPaths = healthpluginpaths;
                    int IntermediateDataAddressing = failovershared.cfgj.IntermediateDataAddressing;
                    if (0 == IntermediateDataAddressing)
                    {
                        IntermediateDataAddressing = failovershared.dc.IntermediateDataAddressing;
                    }
                    block.acl.ValueOffsetSize = IntermediateDataAddressing / 8;
                    if (block.acl.ValueOffsetSize <= 0)
                    {
                        throw new InvalidOperationException("Invalid value for IntermediateDataAddressing: " + IntermediateDataAddressing.ToString());
                    }
                    block.acl.InputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength;
                    block.acl.OutputRecordLength = MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength;
                    block.acl.OutputRecordLengths = failovershared.outputrecordlengths;
                    block.acl.InputRecordLengths = new List<int>();
                    block.acl.CookRetries = failovershared.dc.slave.CookRetries;
                    block.acl.CookTimeout = failovershared.dc.slave.CookTimeout;
                    block.acl.LocalCompile = (0 == block.BlockID);
                    block.acl.BTreeCapSize = failovershared.dc.BTreeCapSize;
                    MySpace.DataMining.DistributedObjects5.DistObject.FILE_BUFFER_SIZE = FILE_BUFFER_SIZE;
                    block.acl.atype = atype;
                    block.acl.DfsSampleDistance = failovershared.dc.DataNodeBaseSize / failovershared.dc.DataNodeSamples;
                    block.slaveconfigxml = failovershared.slaveconfigxml;
                    block.acl.CompressFileOutput = failovershared.dc.slave.CompressDfsChunks;
                    block.acl.ZMapBlockCount = blockcount;
                    block.verbose = failovershared.verbose;
                    block.acl.CompilerOptions = failovershared.cfgj.IOSettings.CompilerOptions;
                    block.acl.CompilerVersion = failovershared.cfgj.IOSettings.CompilerVersion;
                    if (failovershared.cfgj.AssemblyReferencesCount > 0)
                    {
                        failovershared.cfgj.AddAssemblyReferences(block.acl.CompilerAssemblyReferences, Surrogate.NetworkPathForHost(block.SlaveHost));
                    }
                    if (failovershared.cfgj.OpenCVExtension != null)
                    {
                        block.acl.AddOpenCVExtension();
                    }
                    if (failovershared.cfgj.Unsafe != null)
                    {
                        block.acl.AddUnsafe();
                    }
                    block.gencodectx();
                    block.acl.AddBlock("1", "1", block.SlaveHost + @"|"
                        + (failovershared.cfgj.ForceStandardError != null ? "&" : "") + block.logname + @"|slaveid=0");
                    block.ownedzmapblockIDs.Add(block.BlockID);

                    if (block.BlockCID < blockcount)
                    {
                        workingBlocks.Add(block);
                    }
                }

#if FAILOVER_DEBUG
                /*{
                    Log("mapinputfilepaths");
                    string debugtxt = "";
                    foreach (string xx in mapinputfilepaths)
                    {
                        debugtxt += xx + Environment.NewLine;
                    }
                    Log(debugtxt);
                }
                if (mapinputoffsets != null)
                {
                    Log("mapinputoffsets");
                    string debugtxt = "";
                    foreach (int xx in mapinputoffsets)
                    {
                        debugtxt += xx.ToString() + Environment.NewLine;
                    }
                    Log(debugtxt);
                }
                if (mapinputfilenames != null)
                {
                    Log("mapinputfilenames");
                    string debugtxt = "";
                    foreach (string xx in mapinputfilenames)
                    {
                        debugtxt += xx + Environment.NewLine;
                    }
                    Log(debugtxt);
                }
                if (mapinputreclengths != null)
                {
                    Log("mapinputreclengths");
                    string debugtxt = "";
                    foreach (int xx in mapinputreclengths)
                    {
                        debugtxt += xx.ToString() + Environment.NewLine;
                    }
                    Log(debugtxt);
                }*/
#endif

                {
                    List<string> _mapinputfilepaths = null;
                    List<string> _mapinputfilenames = null;
                    List<int> _mapinputoffsets = null;
                    List<int> _mapinputreclengths = null;
                    if (string.Compare("next", failoverShared.cfgj.Computing.MapInputOrder, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        _mapinputfilepaths = mapinputfilepaths;
                        _mapinputfilenames = mapinputfilenames;
                        _mapinputoffsets = mapinputoffsets;
                        _mapinputreclengths = mapinputreclengths;
                    }
                    else if (string.Compare("shuffle", failoverShared.cfgj.Computing.MapInputOrder, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        if (!rehash)
                        {
                            _mapinputfilepaths = new List<string>(mapinputfilepaths.Count);
                            _mapinputfilenames = new List<string>(mapinputfilepaths.Count);
                            _mapinputoffsets = new List<int>(mapinputfilepaths.Count);
                            _mapinputreclengths = new List<int>(mapinputfilepaths.Count);

                            for (int ci = 0; ci < mapinputfilepaths.Count; ci++)
                            {
                                _mapinputfilepaths.Add(mapinputfilepaths[ci]);
                                _mapinputoffsets.Add(ci);
                            }

                            for (int oi = 0; oi < mapinputoffsets.Count; oi++)
                            {
                                string fname = mapinputfilenames[oi];
                                int reclen = mapinputreclengths[oi];
                                int expand = (oi == mapinputoffsets.Count - 1 ? mapinputfilepaths.Count : mapinputoffsets[oi + 1]);
                                expand = expand - _mapinputfilenames.Count;

                                for (int ei = 0; ei < expand; ei++)
                                {
                                    _mapinputfilenames.Add(fname);
                                    _mapinputreclengths.Add(reclen);
                                }
                            }

                            for (int ci = 0; ci < _mapinputfilepaths.Count; ci++)
                            {
                                int rndindex = rnd.Next() % _mapinputfilepaths.Count;
                                string oldchunk = _mapinputfilepaths[ci];
                                _mapinputfilepaths[ci] = _mapinputfilepaths[rndindex];
                                _mapinputfilepaths[rndindex] = oldchunk;

                                string oldfname = _mapinputfilenames[ci];
                                _mapinputfilenames[ci] = _mapinputfilenames[rndindex];
                                _mapinputfilenames[rndindex] = oldfname;

                                int oldreclen = _mapinputreclengths[ci];
                                _mapinputreclengths[ci] = _mapinputreclengths[rndindex];
                                _mapinputreclengths[rndindex] = oldreclen;
                            }
                        }
                        else
                        {
                            _mapinputfilepaths = new List<string>(mapinputfilepaths.Count);
                            for (int ci = 0; ci < mapinputfilepaths.Count; ci++)
                            {
                                _mapinputfilepaths.Add(mapinputfilepaths[ci]);
                            }

                            for (int ci = 0; ci < _mapinputfilepaths.Count; ci++)
                            {
                                int rndindex = rnd.Next() % _mapinputfilepaths.Count;

                                string oldchunk = _mapinputfilepaths[ci];
                                _mapinputfilepaths[ci] = _mapinputfilepaths[rndindex];
                                _mapinputfilepaths[rndindex] = oldchunk;
                            }
                        }
                    }
                    else
                    {
                        throw new Exception("Computing InputOrder is not valid");
                    }

#if FAILOVER_DEBUG
                    /*Log("Done shuffling; MapInputOrder=" + failoverShared.cfgj.Computing.MapInputOrder);
                    {
                        Log("mapinputfilepaths");
                        string debugtxt = "";
                        foreach (string xx in mapinputfilepaths)
                        {
                            debugtxt += xx + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    if (mapinputoffsets != null)
                    {
                        Log("mapinputoffsets");
                        string debugtxt = "";
                        foreach (int xx in mapinputoffsets)
                        {
                            debugtxt += xx.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    if (mapinputfilenames != null)
                    {
                        Log("mapinputfilenames");
                        string debugtxt = "";
                        foreach (string xx in mapinputfilenames)
                        {
                            debugtxt += xx + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    if (mapinputreclengths != null)
                    {
                        Log("mapinputreclengths");
                        string debugtxt = "";
                        foreach (int xx in mapinputreclengths)
                        {
                            debugtxt += xx.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }

                    {
                        Log("_mapinputfilepaths");
                        string debugtxt = "";
                        foreach (string xx in _mapinputfilepaths)
                        {
                            debugtxt += xx + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    if (_mapinputoffsets != null)
                    {
                        Log("_mapinputoffsets");
                        string debugtxt = "";
                        foreach (int xx in _mapinputoffsets)
                        {
                            debugtxt += xx.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    if (_mapinputfilenames != null)
                    {
                        Log("_mapinputfilenames");
                        string debugtxt = "";
                        foreach (string xx in _mapinputfilenames)
                        {
                            debugtxt += xx + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
                    if (_mapinputreclengths != null)
                    {
                        Log("_mapinputreclengths");
                        string debugtxt = "";
                        foreach (int xx in _mapinputreclengths)
                        {
                            debugtxt += xx.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }*/
#endif

                    int firstsetpos = -1;
                    MapReduceBlockInfo targetblock = null;
                    string[] dfsfilenames = null;
                    string curfilename = null;
                    int curreclen = 0;
                    int curoffset = -1;
                    int fi = 0;
                    if (!rehash)
                    {
                        dfsfilenames = new string[blockcount];
                        curoffset = _mapinputoffsets[fi];
                    }
                    for (int mi = 0; mi < _mapinputfilepaths.Count; mi++)
                    {
                        if (curoffset == mi)
                        {
                            curfilename = _mapinputfilenames[fi];
                            curreclen = _mapinputreclengths[fi];
                            if (++fi < _mapinputoffsets.Count)
                            {
                                curoffset = _mapinputoffsets[fi];
                            }
                        }

                        if (++firstsetpos >= firstset.Length)
                        {
                            firstsetpos = 0;
                        }
                        targetblock = firstset[firstsetpos];

                        targetblock.mapinputdfsnodes.Add(_mapinputfilepaths[mi]);
                        if (!rehash && dfsfilenames[targetblock.BlockID] != curfilename)
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

                //scramble each inputfile node * path and assign to the other set of blocks
                {
                    foreach (MapReduceBlockInfo block in firstset)
                    {
                        for (int ri = 1; ri < failovershared.dc.Replication; ri++)
                        {
                            MapReduceBlockInfo repblock = allBlocks[ri * blockcount + block.BlockID];
                            repblock.mapinputnodesoffsets = block.mapinputnodesoffsets;
                            repblock.mapinputfilenames = block.mapinputfilenames;
                            repblock.acl.InputRecordLengths = block.acl.InputRecordLengths;
                        }

                        for (int mi = 0; mi < block.mapinputdfsnodes.Count; mi++)
                        {
                            string mpinput = block.mapinputdfsnodes[mi];
                            string[] parts = mpinput.Split('*');
                            for (int ri = 0; ri < failovershared.dc.Replication; ri++)
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
                                MapReduceBlockInfo repblock = allBlocks[ri * blockcount + block.BlockID];
                                if (ri == 0)
                                {
                                    repblock.mapinputdfsnodes[mi] = sbmpinput;  //just replace with scrambled one.
                                }
                                else
                                {
                                    repblock.mapinputdfsnodes.Add(sbmpinput);
                                }
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
                            int repf = (block.BlockCID - block.BlockID) / blockcount;
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
                    if (blockcount != workingBlocks.Count)
                    {
                        throw new Exception("blockscount != workingBlocks.Count; blockscount=" + blockcount.ToString() + ";workingBlocks.count=" + workingBlocks.Count.ToString());
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

                        if ((bl.BlockCID - bl.BlockID) % blockcount != 0)
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
                        if (bls.Count != failovershared.dc.Replication)
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

                {
                    /*Log("Done CreateBlocks()...");
                    string debugtxt = "==========failover.allBlocks==========" + Environment.NewLine;
                    foreach (MapReduceBlockInfo bl in allBlocks)
                    {
                        debugtxt += Environment.NewLine +
                            "****blockid=" + bl.BlockID.ToString() + ";blockcid=" + bl.BlockCID.ToString()
                            + ";host=" + bl.SlaveHost + Environment.NewLine +
                            string.Join(";", bl.mapinputdfsnodes.ToArray()) + Environment.NewLine;
                    }
                    Log(debugtxt);*/
                }
                {
                    /*string debugtxt = "==========Blockstatus==========" + Environment.NewLine;
                    lock (blockStatus)
                    {
                        foreach (KeyValuePair<int, int> pair in blockStatus)
                        {
                            debugtxt += "****blockcid=" + pair.Key.ToString() + ";status=" + pair.Value.ToString() + Environment.NewLine;
                        }
                    }
                    Log(debugtxt);*/
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

            private bool _checkMachineFailure(bool dochecknow, string host)
            {
                string reason = null;
                bool failure = false;
                if (dochecknow)
                {
                    failure = diskcheck.IsDiskFailure(host, HeartBeats.rogueHosts, out reason);
                }
                else
                {
                    failure = diskcheck.IsDiskFailure(awakeCnt, host, HeartBeats.rogueHosts, out reason);
                }

                if (failure)
                {
                    LogOutputToFile("CheckMachineFailure: " + host + ";reason=" + reason);
                }
                
#if FAILOVER_DEBUG
                if (failure)
                {
                    Log("CheckMachineFailure: " + host + ";reason=" + reason);
                }
#endif
                return failure;
            }

            internal bool CheckMachineFailure(string host)
            {
                return _checkMachineFailure(true, host);
            }

            internal int CheckMachineFailures(bool dochecknow)
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
                    if (_checkMachineFailure(dochecknow, host))
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

            internal bool ESRBlockOnFailedMachine()
            {
                lock (hostToESRBlocks)
                {
                    foreach (string blockhost in hostToESRBlocks.Keys)
                    {
                        if (badHosts.ContainsKey(blockhost))
                        {
                            return true;
                        }
                    }
                    return false;
                }                
            }
            
            internal bool AllBlocksCompleted(int finalstatus)
            {
                lock (blockStatus)
                {
                    foreach (int bs in blockStatus.Values)
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
                    if (!allBlocks[i].diskfailuredetected)
                    {
                        try
                        {
                            allBlocks[i].acl.StopZMapBlockServer();
                            allBlocks[i].acl.Close();                            
                        }
                        catch
                        {
                        }      
                    }                    
                }
                foreach (FailoverInfo failover in childFailovers.Values)
                {
                    failover.CloseAllBlocks();               
                }
            }

            internal void AbortBlocksFromFailedHosts(bool needabort)
            {
                foreach (string badhost in badHosts.Keys)
                {
                    if (hostToBlocks.ContainsKey(badhost))
                    {
                        AbortBlocksFromFailedHost(badhost, needabort);
                    }
                }    
            }

            internal void AbortBlocksFromFailedHost(string badhost)
            {
                AbortBlocksFromFailedHost(badhost, true);
            }

            internal void AbortBlocksFromFailedHost(string badhost, bool needabort)
            {
                if (hostToBlocks.ContainsKey(badhost))
                {
                    List<MapReduceBlockInfo> badblocks = hostToBlocks[badhost];

#if FAILOVER_DEBUG
                    Log("AbortBlocksFromFailedHost: " + badhost + " contains badblocks; badblockscount=" + badblocks.Count.ToString());
#endif

                    foreach (MapReduceBlockInfo badblock in badblocks)
                    {
                        badblock.diskfailuredetected = true;

                        if (needabort)
                        {
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
                    }

                    hostToBlocks.Remove(badhost);
                }
            }

            internal void AbortESRBlocksFromFailedHosts(List<string> exchangeworkload, out int minblockcount)
            {
                minblockcount = Int32.MaxValue;

                lock (hostToESRBlocks)
                {                    
                    foreach (string badhost in badHosts.Keys)
                    {
                        if (hostToESRBlocks.ContainsKey(badhost))
                        {
                            List<MapReduceBlockInfo> badblocks = hostToESRBlocks[badhost];

#if FAILOVER_DEBUG
                            Log("AbortESRBlocksFromFailedHosts: " + badhost + " contains esrbadblocks; esrbadblockscount=" + badblocks.Count.ToString());
#endif

                            foreach (MapReduceBlockInfo badblock in badblocks)
                            {
                                badblock.diskfailuredetected = true;

                                if (badblock.acl.ZMapBlockCount < minblockcount)
                                {
                                    minblockcount = badblock.acl.ZMapBlockCount;
                                }
                                exchangeworkload.Add(badblock.ownedzmapblocks.Replace("%n", badblock.BlockID.ToString()));
                                exchangeworkload.AddRange(badblock.remotezmapblocks.Replace("%n", badblock.BlockID.ToString()).Split(';'));

                                if (badblock.failover == this)  //if this badblock belongs to me
                                {
                                    lock (blockStatus)
                                    {
                                        blockStatus.Remove(badblock.BlockCID);
#if FAILOVER_DEBUG
                                        Log("Removed esrbadblock from blockstatus; blockcid=" + badblock.BlockCID.ToString());
#endif
                                    }

                                    try
                                    {
                                        badblock.thread.Abort();
#if FAILOVER_DEBUG
                                        Log("Aborted esrbadblock; blockcid=" + badblock.BlockCID.ToString());
#endif
                                        AELight_RemoveTraceThread(badblock.thread); //no join, just remove.
#if FAILOVER_DEBUG
                                        Log("AELight esrremoved trace badblock; blockcid=" + badblock.BlockCID.ToString());
#endif
                                    }
                                    catch
                                    {
                                    }
                                }
                            }
                            hostToESRBlocks.Remove(badhost);   
                        }                                             
                    }
                }

                AbortBlocksFromFailedHosts(false);

#if FAILOVER_DEBUG
                {
                    string debugtxt = "Done AbortESRBlocksFromFailedHosts" + Environment.NewLine +                        
                        "minblockcount=" + minblockcount.ToString();
                    Log(debugtxt);
                }
#endif
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
           
            internal void ExecOneMapReduceFailover()
            {
                //parent=null.
                ExecOneMapReduceFailover(null, -1);
            }

            internal void ExecOneMapReduceFailover(FailoverInfo parent, int myCID)
            {
#if FAILOVER_DEBUG
                {
                    Log("Begin ExecOneMapReduceFailover: parent=" + (parent == null ? "null" : "has parent") + "; myCID=" + myCID.ToString());                               
                }
#endif                

                try
                {
                    if (allBlocks == null)
                    {
                        throw new Exception("FailoverInfo allBlocks is null.");
                    }

                    //Start mapblocks firstthread
                    uint sleepCnt = 0;
                    {

#if FAILOVER_TEST
                        {
                            Console.WriteLine(@"FAILOVER_TEST: before map threads start");                            
                            while (System.IO.File.Exists(@"c:\temp\failovertest1.txt"))
                            {
                                Console.Write("z");
                                System.Threading.Thread.Sleep(10000);
                            }
                        }
#endif

                        foreach (MapReduceBlockInfo bl in allBlocks)
                        {
                            bl.all = workingBlocks;
                            bl.thread = new System.Threading.Thread(new System.Threading.ThreadStart(bl.firstthreadproc));
                            bl.thread.Name = "MapReduceJobBlock" + bl.BlockID + "_map";
                            bl.thread.IsBackground = true;
                            AELight_StartTraceThread(bl.thread);
                        }

                        awakeCnt = 0;

                        for (; ; )
                        {
#if FAILOVER_DEBUG
                            {
                                Log("Loop at map.  Sleepcnt=" + sleepCnt.ToString());
                                string debugtxt = "==========Blockstatus at map==========" + Environment.NewLine;
                                lock (blockStatus)
                                {
                                    foreach (KeyValuePair<int, int> pair in blockStatus)
                                    {
                                        debugtxt += "****blockcid=" + pair.Key.ToString() + ";status=" + pair.Value.ToString() + Environment.NewLine;
                                    }
                                }
                                Log(debugtxt);
                            }
#endif

                            if (AllBlocksCompleted(1))
                            {
#if FAILOVER_DEBUG
                                Log("All map blocks completed.  Breaking out of map loop...");
#endif
                                break;
                            }

                            System.Threading.Thread.Sleep(failoverShared.dc.FailoverTimeout);

                            if (sleepCnt++ > failoverShared.dc.FailoverDoCheck)
                            {
                                sleepCnt = 0;  //sleep again

#if FAILOVER_DEBUG
                                Log("Health check at map loop;awakeCnt=" + awakeCnt.ToString());
#endif

                                if (CheckMachineFailures(false) > 0)
                                {
#if FAILOVER_DEBUG                                    
                                    {
                                        Log("Disk failure detected at map loop...");
                                        string debugtxt = "======Bad hosts found=======" + Environment.NewLine +
                                            string.Join(";", newBadHosts.ToArray());
                                        Log(debugtxt);
                                    }
#endif                               
                                    DisplayNewBadHosts();    

                                    for (int hi = 0; hi < newBadHosts.Count; hi++)
                                    {                                     
                                        AbortBlocksFromFailedHost(newBadHosts[hi]);
                                    }
                                    

#if FAILOVER_DEBUG
                                    {
                                        Log("Done removing all bad blocks at map loop");
                                        string debugtxt = "";
                                        Log("========failover.allblocks========");
                                        foreach (MapReduceBlockInfo bl in allBlocks)
                                        {    
                                            debugtxt += Environment.NewLine +
                                            "****blockid=" + bl.BlockID.ToString() + ";blockcid=" + bl.BlockCID.ToString()
                                            + ";dfdetected=" + bl.diskfailuredetected.ToString()
                                            + ";host=" + bl.SlaveHost + Environment.NewLine + Environment.NewLine;
                                        }
                                        Log(debugtxt);
                                    }
                                    {
                                        string debugtxt = "==========Blockstatus at map==========" + Environment.NewLine;
                                        lock (blockStatus)
                                        {
                                            foreach (KeyValuePair<int, int> pair in blockStatus)
                                            {
                                                debugtxt += "****blockcid=" + pair.Key.ToString() + ";status=" + pair.Value.ToString() + Environment.NewLine;
                                            }
                                        }
                                        Log(debugtxt);
                                        Log("hostToBlocksCount=" + hostToBlocks.Count.ToString());
                                    }
#endif
                                }
                                awakeCnt++;
                            }
                        }

#if FAILOVER_TEST
                        {
                            Console.WriteLine(@"FAILOVER_TEST: after map threads joined");
                            while (System.IO.File.Exists(@"c:\temp\failovertest2.txt"))
                            {
                                Console.Write("z");
                                System.Threading.Thread.Sleep(10000);
                            }
                        }
#endif

                        if (CheckMachineFailures(true) > 0)
                        {
                            DisplayNewBadHosts();                           
                            for (int hi = 0; hi < newBadHosts.Count; hi++)
                            {
                                AbortBlocksFromFailedHost(newBadHosts[hi]);
                            }
                        }                        

                        for (int bi = 0; bi < allBlocks.Length; bi++)
                        {
                            MapReduceBlockInfo bl = allBlocks[bi];
                            if (!bl.diskfailuredetected)
                            {
                                AELight_JoinTraceThread(bl.thread);
                            }
                        }
#if FAILOVER_DEBUG
                        Log("All map blocks joined.");
#endif
                        if (badHosts.Count >= failoverShared.dc.Replication)
                        {
                            throw new Exception("Error: Cannot continue to exchange/sort/reduce phase.  The number of machines with hardware failure (" + (badHosts.Count).ToString() + ") is greater than or equal to replication factor (" + failoverShared.dc.Replication.ToString() + ").");
                        }

                        for (int bi = 0; bi < workingBlocks.Count; bi++)
                        {
                            MapReduceBlockInfo bl = workingBlocks[bi];
                            if (bl.diskfailuredetected || bl.blockfail)
                            {
                                bool foundgoodblock = false;
                                for (int ri = 0; ri < failoverShared.dc.Replication - 1; ri++)
                                {
                                    int nextrepblockcid = (ri + 1) * workingBlocks.Count + bl.BlockCID;
                                    MapReduceBlockInfo nextrepblock = allBlocks[nextrepblockcid];
                                    if (!nextrepblock.diskfailuredetected && !nextrepblock.blockfail)
                                    {
                                        foundgoodblock = true;
                                        workingBlocks[bi] = nextrepblock;
                                        break;
                                    }
                                }
                                if (!foundgoodblock)
                                {
                                    throw new Exception("Error: Cannot find a good replicated map block to replace the failed block.  Block index = " + bi.ToString());
                                }
                            }
                        }
#if FAILOVER_DEBUG
                        {
                            Log("=======Blocks going forward to exchange=========");
                            string debugtxt = "";
                            foreach (MapReduceBlockInfo bl in workingBlocks)
                            {
                                debugtxt += Environment.NewLine +
                                    "****blockid=" + bl.BlockID.ToString() + ";blockcid=" + bl.BlockCID.ToString() + ";host=" + bl.SlaveHost +
                                    ";dfdetected=" + bl.diskfailuredetected.ToString() + Environment.NewLine;
                            }
                            Log(debugtxt);
                        }
#endif
                    }

                    if (failoverShared.verbose)
                    {
                        Console.WriteLine((failoverShared.extraverbose ? "\r\n" : "") + "    [{0}]        Map done; starting map exchange", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
                        ConsoleFlush();
                    }

                    //all map joined
                    //Get good zmapblocks and their copies.
                    string[] zmapblocks = new string[blockCount];
                    {
                        foreach (MapReduceBlockInfo bl in allBlocks)
                        {
                            if (!bl.diskfailuredetected && !bl.blockfail)
                            {
                                string zm = zmapblocks[bl.BlockID];
                                if (zm != null)
                                {
                                    zm += "*";
                                }
                                else
                                {
                                    zm = "";
                                }
                                zm += Surrogate.NetworkPathForHost(bl.SlaveHost) + @"\" + bl.acl.GetZMapBlockBaseName();
                                zmapblocks[bl.BlockID] = zm;
                            }
                        }
                    }
#if FAILOVER_DEBUG
                    {
                        /*string debugtxt = "zmapblocks: len=" + zmapblocks.Length.ToString() + Environment.NewLine;
                        for (int zi = 0; zi < zmapblocks.Length; zi++)
                        {
                            debugtxt += zi.ToString() + ":" + zmapblocks[zi] + Environment.NewLine;
                        }
                        Log(debugtxt);*/
                    }
#endif

                    //assign zmapblocks workload for each working thread that is about to go into exchange.
                    for (int wi = 0; wi < workingBlocks.Count; wi++)
                    {
                        MapReduceBlockInfo wb = workingBlocks[wi];
                        wb.ownedzmapblocks = zmapblocks[wb.BlockID];

                        string remotezms = "";
                        for (int zi = 0; zi < zmapblocks.Length; zi++)
                        {
                            if (zi != wb.BlockID)
                            {
                                if (remotezms.Length > 0)
                                {
                                    remotezms += ";";
                                }
                                remotezms += zmapblocks[zi];
                            }
                        }
                        wb.remotezmapblocks = remotezms;
                    }

#if FAILOVER_DEBUG
                    {
                        /*
                        string debugtxt = "Done assign zmapblocks workload for each working thread:" + Environment.NewLine;
                        for (int wi = 0; wi < workingBlocks.Count; wi++)
                        {
                            MapReduceBlockInfo wb = workingBlocks[wi];
                            debugtxt += "blockid=" + wb.BlockID.ToString() + ";blockcid=" + wb.BlockCID.ToString() + Environment.NewLine +
                                "owned=" + wb.ownedzmapblocks + Environment.NewLine
                                + "remote=" + wb.remotezmapblocks.Split(';').Length.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);*/
                    }
#endif


                    //Start exchange/sort/reduce  
                    blockStatus.Clear();

                    for (int bi = 0; bi < workingBlocks.Count; bi++)
                    {
                        MapReduceBlockInfo bl = workingBlocks[bi];
                        blockStatus.Add(bl.BlockCID, 0);
                        string host = bl.SlaveHost.ToLower();
                        if (!hostToESRBlocks.ContainsKey(host))
                        {
                            hostToESRBlocks.Add(host, new List<MapReduceBlockInfo>());
                        }
                        hostToESRBlocks[host].Add(bl);
                    }

#if FAILOVER_TEST
                    {
                        Console.WriteLine(@"FAILOVER_TEST: before esr threads start");
                        while (System.IO.File.Exists(@"c:\temp\failovertest3.txt"))
                        {
                            Console.Write("z");
                            System.Threading.Thread.Sleep(10000);
                        }
                    }
#endif

                    //start esr threads
                    for (int bi = 0; bi < workingBlocks.Count; bi++)
                    {
                        MapReduceBlockInfo bl = workingBlocks[bi];
                        bl.thread = new System.Threading.Thread(new System.Threading.ThreadStart(bl.exchangethreadproc));
                        bl.thread.Name = "MapReduceJobBlock" + bl.BlockID + "_aftermap";
                        bl.thread.IsBackground = true;
                        AELight_StartTraceThread(bl.thread);
                    }

                    sleepCnt = 0; //!
                    awakeCnt = 0; //!
                    for (; ; )
                    {

#if FAILOVER_DEBUG
                        {
                            Log("Loop at esr.  SleepCnt=" + sleepCnt.ToString());
                            string debugtxt = "==========Blockstatus at esr==========" + Environment.NewLine;
                            lock (blockStatus)
                            {
                                foreach (KeyValuePair<int, int> pair in blockStatus)
                                {
                                    debugtxt += "****blockcid=" + pair.Key.ToString() + ";status=" + pair.Value.ToString() + Environment.NewLine;
                                }
                            }
                            Log(debugtxt);
                        }
#endif

                        if (AllBlocksCompleted(1))
                        {
#if FAILOVER_DEBUG
                            Log("All esr blocks completed.");
#endif

#if FAILOVER_TEST
                            {
                                Console.WriteLine(@"FAILOVER_TEST: all esr blocks completed, before breaking out of loop");                               
                                while (System.IO.File.Exists(@"c:\temp\failovertest4.txt"))
                                {
                                    Console.Write("z");
                                    System.Threading.Thread.Sleep(10000);
                                }
                            }
#endif

                            //do one more check before breaking out of loop.
                            if (CheckMachineFailures(true) > 0 || ESRBlockOnFailedMachine())
                            {

#if FAILOVER_DEBUG
                                Log("df detected before breaking out of esr loop");
#endif

                                sleepCnt = 0;
                                DisplayNewBadHosts();                                
                                RehashESRBlocksFromFailedHosts();
                            }
                            else
                            {
                                break;
                            }
                        }

                        System.Threading.Thread.Sleep(failoverShared.dc.FailoverTimeout);

                        if (sleepCnt++ > failoverShared.dc.FailoverDoCheck)
                        {
                            sleepCnt = 0;  //sleep again

#if FAILOVER_DEBUG
                            Log("Health check at esr loop;awakeCnt=" + awakeCnt.ToString());
#endif

                            if (CheckMachineFailures(false) > 0 || ESRBlockOnFailedMachine())
                            {
                                DisplayNewBadHosts();
                                RehashESRBlocksFromFailedHosts();
                            }
                            awakeCnt++;
                        }
                    }
                    
#if FAILOVER_DEBUG
                    Log("all esr joined...");
#endif
                    //check my good workingblocks
                    {
                        Exception ee = null;
                        foreach (MapReduceBlockInfo bl in workingBlocks)
                        {
                            if (!bl.diskfailuredetected)  //still good.
                            {
                                AELight_JoinTraceThread(bl.thread);
                                if (bl.blockfail)
                                {
                                    ee = bl.LastThreadException;
                                }
                            }
                        }
                        if (null != ee)
                        {
                            throw new Exception("ESR workingblock error: " + ee.ToString());
                        }
                    }

                    //check child failover
                    foreach (FailoverInfo failover in childFailovers.Values)
                    {
                        if (failover.LastException != null)
                        {
                            throw new Exception("childFailovers.count=" + childFailovers.Count.ToString() + ";Child failover error: " + failover.LastException.ToString());
                        }
                    }

#if FAILOVER_DEBUG
                    Log("no esr exceptions...");
#endif

                    //ALL DONE.  Append blocks to parent only if everything is ok.
                    if (parent != null)
                    {
#if FAILOVER_DEBUG
                        Log("Adding my esrblocks to parent:");
                        {
                            string debugtxt = "my esrblocks:" + Environment.NewLine;
                            foreach (KeyValuePair<string, List<MapReduceBlockInfo>> pair in hostToESRBlocks)
                            {
                                debugtxt += pair.Key + ":" + pair.Value.Count.ToString() + Environment.NewLine;
                            }
                            Log(debugtxt);
                        }
#endif
                        parent.AddHostToESRBlocks(hostToESRBlocks);
                    }
                }
                catch (Exception e)
                {
                    LastException = e;

                    try
                    {
                        CloseAllBlocks();
                    }
                    catch
                    {
                    }
                    
                    LogOutput("ExecOneMapReduceFailover error: " + e.ToString());

#if FAILOVER_DEBUG
                    Log("ExecOneMapReduceFailover error: " + e.ToString());
#endif
                   
                }

                if (parent != null) //report that i am done no matter if there is exception or not.
                {
#if FAILOVER_DEBUG
                    Log("UpdateBlockStatus mycID: " + myCID.ToString());
#endif
                    parent.UpdateBlockStatus(myCID, 1);
                }

#if FAILOVER_DEBUG
                {
                    string debugtxt = "Exiting ExecOneMapReduceFailover...Final esrblocks:" + Environment.NewLine;
                    foreach (KeyValuePair<string, List<MapReduceBlockInfo>> pair in hostToESRBlocks)
                    {
                        debugtxt += "host=" + pair.Key + ":" + pair.Value.Count.ToString() + Environment.NewLine;
                        foreach (MapReduceBlockInfo bl in pair.Value)
                        {
                            debugtxt += "blockid=" + bl.BlockID.ToString() + ";blockcid=" + bl.BlockCID.ToString() + Environment.NewLine +
                                "owned=" + bl.ownedzmapblocks + Environment.NewLine;
                        }
                    }
                    Log(debugtxt);
                }
#endif
            }

            internal void DisplayNewBadHosts()
            {
                if (newBadHosts.Count > 0)
                {
                    int hoffset = (badHosts.Count - newBadHosts.Count);
                    string recovered = badHosts.Count >= failoverShared.dc.Replication ? "NoRecovery" : "Recovered";
                    for (int hi = 0; hi < newBadHosts.Count; hi++)
                    {
                        string badhost = newBadHosts[hi];
                        Console.WriteLine(Environment.NewLine + "HWFailure:{0}:{1}:{2}/{3}",
                            recovered, badhost, hi + 1 + hoffset, failoverShared.dc.Replication);
                    }
                    ConsoleFlush();
                }               
            }

            internal void RehashESRBlocksFromFailedHosts()
            {
                List<string> exchangeworkload = new List<string>();
                int minblockcount = 0;
                AbortESRBlocksFromFailedHosts(exchangeworkload, out minblockcount);

                if (badHosts.Count >= failoverShared.dc.Replication)
                {
                    throw new Exception("The number of hardware failures >= Replication factor");
                }

                //if there is anything to rehash.
                if (exchangeworkload.Count > 0)
                {
                    lock (blockStatus)
                    {
                        blockStatus.Add(cID, 0);
                    }
                    FailoverInfo failover = new FailoverInfo(failoverShared.dc);
                    childFailovers.Add(cID, failover);
                    int newblockcount = minblockcount;
                    for (; ; )
                    {
                        newblockcount = AELight.NearestPrimeLE(newblockcount - 1);
                        if (newblockcount != failoverShared.dc.slave.zblocks.count)
                        {
                            break;
                        }
                    }
#if FAILOVER_DEBUG
                    Log("rehashing...newblockcount:" + newblockcount.ToString() + ";cid:" + cID.ToString());
#endif
                    failover.CreateBlocks(newblockcount, exchangeworkload, null, null, null, failoverShared, new List<string>(goodHosts.Keys).ToArray(), new List<string>(badHosts.Keys).ToArray(), true); //rehash=true
                    int childcid = cID;
                    System.Threading.Thread th = new System.Threading.Thread(new System.Threading.ThreadStart(delegate()
                    {
                        failover.ExecOneMapReduceFailover(this, childcid);
                    }));
                    th.IsBackground = true;
                    th.Start();
                    cID++;
                }
            }

            internal void AddHostToESRBlocks(Dictionary<string, List<MapReduceBlockInfo>> addblocks)
            {
                lock (hostToESRBlocks)
                {
                    foreach (KeyValuePair<string, List<MapReduceBlockInfo>> pair in addblocks)
                    {
                        string host = pair.Key;
                        List<MapReduceBlockInfo> blocks = pair.Value;
                        if (!hostToESRBlocks.ContainsKey(host))
                        {
                            hostToESRBlocks.Add(host, new List<MapReduceBlockInfo>());
                        }
                        foreach (MapReduceBlockInfo bl in blocks)
                        {
                            hostToESRBlocks[host].Add(bl);
                        }
                    }
                }                
            }

            internal void ReplicationPhaseFailover()
            {
                blockStatus.Clear(); //!

#if FAILOVER_TEST
                {
                    Console.WriteLine(@"FAILOVER_TEST: before replication main threads start");
                    while (System.IO.File.Exists(@"c:\temp\failovertest5.txt"))
                    {
                        Console.Write("z");
                        System.Threading.Thread.Sleep(10000);
                    }
                }
#endif

                hostToReplWorkers = new Dictionary<string, List<ReplWorker>>(goodHosts.Count);
                dfsnamesToReplnames = new Dictionary<string, List<string>>(failoverShared.outputfiles.Count);
                badOutputFilenodes = new Dictionary<string, int>();
                DateTime replstart = ESRBlocksToReplicationPhase();


#if FAILOVER_TEST
                {
                    Console.WriteLine(@"FAILOVER_TEST: all replication main threads started; before join loop");
                    while (System.IO.File.Exists(@"c:\temp\failovertest6.txt"))
                    {
                        Console.Write("z");
                        System.Threading.Thread.Sleep(10000);
                    }
                }
#endif

                List<int> needrep = new List<int>(failoverShared.dc.Replication);
                uint sleepCnt = 0;
                awakeCnt = 0;

                for (; ; )
                {
#if FAILOVER_DEBUG
                    {
                        string debugtxt = "replication loop; sleepcnt=" + sleepCnt.ToString() + Environment.NewLine +
                            "blockstatus: " + Environment.NewLine;
                        foreach (KeyValuePair<int, int> pair in blockStatus)
                        {
                            debugtxt += "cid=" + pair.Key.ToString() + ";status=" + pair.Value.ToString() + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
#endif
                    if (AllBlocksCompleted(3))
                    {
#if FAILOVER_DEBUG
                        Log("replication blocks completed.");
#endif

#if FAILOVER_TEST
                        {
                            Console.WriteLine(@"FAILOVER_TEST: before breaking out of replication join loop");
                            while (System.IO.File.Exists(@"c:\temp\failovertest7.txt"))
                            {
                                Console.Write("z");
                                System.Threading.Thread.Sleep(10000);
                            }
                        }
#endif

                        if (CheckMachineFailures(true) > 0 || ESRBlockOnFailedMachine())
                        {
#if FAILOVER_DEBUG
                            Log("df found after rep blocks all completed.");
#endif
                            sleepCnt = 0;
                            DisplayNewBadHosts();
                            AddBadOutputFilenodesAndRehash();
                            AbortReplWorkersFromFailedHosts();
                        }
                        else
                        {
#if FAILOVER_DEBUG
                            Log("break out of replication loop.");
#endif
                            break;
                        }
                    }

                    //check if there is status=1, the rehash-esr blocks are done.
                    lock (blockStatus)
                    {
                        needrep.Clear();
                        foreach (int _cid in blockStatus.Keys)
                        {
                            if (blockStatus[_cid] == 1)   //status == 1; rehash-esr done
                            {
                                if (childFailovers[_cid].LastException != null)
                                {
                                    throw new Exception("rehashesr childFailovers[" + _cid.ToString() + "] error: " + childFailovers[_cid].LastException.ToString());
                                }
                                needrep.Add(_cid);
                            }
                        }

                        if (needrep.Count > 0)
                        {
#if FAILOVER_DEBUG
                            Log("found status=1;needrep.count=" + needrep.Count.ToString() + "; begin ESRBlocksToReplicationPhase");
#endif
                            foreach (int _cid in needrep)
                            {
                                blockStatus.Remove(_cid); //don't need to keep this status anymore, done with this already.
                            }
                            ESRBlocksToReplicationPhase();
                        }
                    }

                    System.Threading.Thread.Sleep(failoverShared.dc.FailoverTimeout);

                    if (sleepCnt++ > failoverShared.dc.FailoverDoCheck)
                    {
                        sleepCnt = 0;  //sleep again

#if FAILOVER_DEBUG
                        Log("Health check at rep loop;awakeCnt=" + awakeCnt.ToString());
#endif
                        if (CheckMachineFailures(false) > 0 || ESRBlockOnFailedMachine())
                        {
#if FAILOVER_DEBUG
                            Log("df found at replication loop");
#endif
                            DisplayNewBadHosts();
                            AddBadOutputFilenodesAndRehash();
                            AbortReplWorkersFromFailedHosts();
                        }
                        awakeCnt++;
                    }
                }

                //replication joined                
                foreach (KeyValuePair<string, List<ReplWorker>> pair in hostToReplWorkers)
                {
                    foreach (ReplWorker worker in pair.Value)
                    {
                        if (worker.LastException != null)
                        {
                            throw new Exception("Replication worker on host: " + worker.info.host + " reported an exception: " + worker.LastException.ToString());
                        }
                    }
                }


#if MR_REPLICATION_TIME_PRINT
                {
                    int replsecs = (int)Math.Round((DateTime.Now - replstart).TotalSeconds);
                    ConsoleColor oldcolor = Console.ForegroundColor;
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("{0}[replication completed {1}]{2}", isdspace ? "\u00011" : "", DurationString(replsecs), isdspace ? "\u00010" : "");
                    Console.ForegroundColor = oldcolor;
                }                
#endif

                bool anyoutput = false;
                using (LockDfsMutex())
                {
                    // Note: "new*" means new changes to dfs.xml outside of the replication.
                    dfs newdc = LoadDfsConfig();
                    int replication = newdc.Replication;

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

                    //gather all the replicated file nodes.
                    Dictionary<string, dfs.DfsFile.FileNode> replfilenodes = new Dictionary<string, dfs.DfsFile.FileNode>();
                    foreach (KeyValuePair<string, List<ReplWorker>> pair in hostToReplWorkers)
                    {
                        foreach (ReplWorker worker in pair.Value)
                        {
                            foreach (ReplFileNode fn in worker.info.pullfiles)
                            {
                                if (!replfilenodes.ContainsKey(fn.node.Name))
                                {
                                    replfilenodes.Add(fn.node.Name, fn.node);
                                }
                            }
                        }
                    }

                    //merge
                    List<dfs.DfsFile.FileNode> fnremove = new List<dfs.DfsFile.FileNode>();
                    foreach (string dfsname in dfsnamesToReplnames.Keys)
                    {
                        if (null != DfsFindAny(newdc, dfsname))
                        {
                            Console.Error.WriteLine("Error:  output file '{0}' was created during job", dfsname);
                            continue;
                        }

                        dfs.DfsFile combinedf = null;
                        List<string> replnames = dfsnamesToReplnames[dfsname];
                        for (int ri = 0; ri < replnames.Count; ri++)
                        {
                            string replname = replnames[ri];
                            if (newdfsfiles.ContainsKey(replname))
                            {
                                dfs.DfsFile newdf = newdfsfiles[replname];

                                if (ri == 0)
                                {
                                    combinedf = newdf;
                                }

                                fnremove.Clear();

                                //update hosts of file nodes                                
                                foreach (dfs.DfsFile.FileNode newfn in newdf.Nodes)
                                {
                                    if (badOutputFilenodes.ContainsKey(newfn.Name))
                                    {
                                        fnremove.Add(newfn);
                                        continue;
                                    }
                                    if (replfilenodes.ContainsKey(newfn.Name))
                                    {
                                        dfs.DfsFile.FileNode replfn = replfilenodes[newfn.Name];
                                        if (replfn.Host != newfn.Host)
                                        {
                                            Dictionary<string, bool> allchosts = new Dictionary<string, bool>(new Surrogate.CaseInsensitiveEqualityComparer());
                                            foreach (string chost in replfn.Host.Split(';'))
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
                                //remove bad fn.
                                foreach (dfs.DfsFile.FileNode newfn in fnremove)
                                {
                                    newdf.Nodes.Remove(newfn);
                                }

                                //combine the file nodes and remove the file.
                                if (ri != 0)
                                {
                                    combinedf.Nodes.AddRange(newdf.Nodes);
                                    newdc.Files.Remove(newdf);
                                }
                            }
                        }

                        //change filename and fix positions
                        combinedf.Name = dfsname;
                        {
                            long totalsize = 0;
                            foreach (dfs.DfsFile.FileNode fn in combinedf.Nodes)
                            {
                                fn.Position = totalsize; //Position must be set before totalsize updated!
                                totalsize += fn.Length;
                            }
                            if (totalsize > 0)
                            {
                                anyoutput = true;
                            }
                            combinedf.Size = totalsize;
                        }
                    }

                    UpdateDfsXml(newdc);
                }

                if (!anyoutput && failoverShared.verbose)
                {
                    Console.Write(" (no DFS output) ");
                    ConsoleFlush();
                }

                if (badHosts.Count > 0)
                {
                    string sbadhosts = "";
                    foreach (string badhost in badHosts.Keys)
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
                        "{0}{1} machine(s) with hardware failure: {2}{3}", isdspace ? "\u00014" : "", badHosts.Count, sbadhosts, isdspace ? "\u00010" : "");
                    ConsoleFlush();
                    Console.ForegroundColor = oldcolor;
                }

                //After dfs.xml updated,
                //check if there is any false negative plugin failure reported from replworker
                //that is, replworker sees a df but surrogate doesn't
                //recommend actions for clients.
                foreach (KeyValuePair<string, List<ReplWorker>> pair in hostToReplWorkers)
                {
                    foreach (ReplWorker worker in pair.Value)
                    {
                        if (!string.IsNullOrEmpty(worker.sFailedHosts))
                        {

#if FAILOVER_DEBUG
                            Log("worker.sFailedHosts=" + worker.sFailedHosts);
#endif
                            string[] fhs = worker.sFailedHosts.Split(';');
                            foreach (string fh in fhs)
                            {
                                if (!badHosts.ContainsKey(fh.ToLower()))
                                {
                                    //throw?
                                    Console.Error.WriteLine("Health plugin reported a false negative on host: " + fh + ".  Recommended actions:  ReplicationPhase and Health -a to discover missing file chunks and replicates.");
                                }
                            }
                        }
                    }
                }
            }

            internal void AddBadOutputFilenodesAndRehash()
            {
#if FAILOVER_DEBUG
                Log("AddBadOutputFilenodesAndRehash");
#endif
                lock (hostToESRBlocks)
                {
                    foreach (string badhost in badHosts.Keys)
                    {
                        if (hostToESRBlocks.ContainsKey(badhost))
                        {
                            List<MapReduceBlockInfo> badblocks = hostToESRBlocks[badhost];
                            foreach (MapReduceBlockInfo badblock in badblocks)
                            {
                                List<List<string>> outputfilenodeses = badblock.reduceoutputdfsnodeses;
                                foreach (List<string> outputfilenodes in outputfilenodeses)
                                {
                                    foreach (string outputfilenode in outputfilenodes)
                                    {
                                        if (!badOutputFilenodes.ContainsKey(outputfilenode))
                                        {
                                            badOutputFilenodes.Add(outputfilenode, 1);
                                        }
                                    }
                                }
                            }
                        }
                    }

#if FAILOVER_DEBUG
                    {
                        string debugtxt = "badOutputFilenodes:" + Environment.NewLine;
                        foreach (string str in badOutputFilenodes.Keys)
                        {
                            debugtxt += str + Environment.NewLine;
                        }
                        Log(debugtxt);
                    }
#endif
                    RehashESRBlocksFromFailedHosts();
                }
            }

            DateTime ESRBlocksToReplicationPhase()
            {
                List<dfs.DfsFile> repfiles = GetOutputFilesFromESRBlocks();
                DateTime replstart = DateTime.Now;
                if (repfiles.Count > 0)
                {
                    CreateReplWorkers(blockCount, repfiles);
                }
                return replstart;
            }

            internal void AbortReplWorkersFromFailedHosts()
            {
#if FAILOVER_DEBUG
                Log("AbortReplWorkersFromFailedHosts");
#endif
                //find replworkers on the failed hosts, abort them and need to delegate their workload to new replworkers.
                //so that they can help to download those file nodes.                
                Dictionary<string, dfs.DfsFile> ownerfiles = new Dictionary<string, dfs.DfsFile>();
                Dictionary<string, Dictionary<string, dfs.DfsFile.FileNode>> ownerfiletonodes = new Dictionary<string, Dictionary<string, dfs.DfsFile.FileNode>>();
                foreach (string badhost in newBadHosts)
                {
                    if (hostToReplWorkers.ContainsKey(badhost))
                    {
                        List<ReplWorker> badworkers = hostToReplWorkers[badhost];
                        foreach (ReplWorker badworker in badworkers)
                        {
                            List<ReplFileNode> pullfiles = badworker.info.pullfiles;
                            foreach (ReplFileNode fn in pullfiles)
                            {
                                if (badOutputFilenodes.ContainsKey(fn.node.Name)) //we don't care for bad file nodes.
                                {
                                    continue;
                                }

                                Dictionary<string, dfs.DfsFile.FileNode> fnodes = null;
                                if (!ownerfiles.ContainsKey(fn.ownerfile.Name))
                                {
                                    ownerfiles.Add(fn.ownerfile.Name, fn.ownerfile);
                                    fnodes = new Dictionary<string, dfs.DfsFile.FileNode>();
                                    ownerfiletonodes.Add(fn.ownerfile.Name, fnodes);
                                }
                                else
                                {
                                    fnodes = ownerfiletonodes[fn.ownerfile.Name];
                                }

                                if (!fnodes.ContainsKey(fn.node.Name))
                                {
                                    fnodes.Add(fn.node.Name, fn.node);
                                }

                                //remove the badhost from fnode's host list.
                                string[] fhosts = fn.node.Host.Split(';');
                                string remaininghosts = "";
                                foreach (string fhost in fhosts)
                                {
                                    if (string.Compare(fhost, badhost, StringComparison.OrdinalIgnoreCase) != 0)
                                    {
                                        if (remaininghosts.Length > 0)
                                        {
                                            remaininghosts += ";";
                                        }
                                        remaininghosts += fhost;
                                    }
                                }
                                fn.node.Host = remaininghosts;
                            }

                            try
                            {
                                badworker.thread.Abort();
                            }
                            catch
                            {
                            }

                            lock (blockStatus)
                            {
                                blockStatus.Remove(badworker.cID);
                            }                            
                        }
                        hostToReplWorkers.Remove(badhost);
                    }
                }

                //need to create new replworkers to download these fnodes.
                List<dfs.DfsFile> filestorep = new List<dfs.DfsFile>(ownerfiles.Count);
                List<List<dfs.DfsFile.FileNode>> fnodestorep = new List<List<dfs.DfsFile.FileNode>>(ownerfiles.Count);
                foreach (dfs.DfsFile file in ownerfiles.Values)
                {
                    filestorep.Add(file);
                    fnodestorep.Add(new List<dfs.DfsFile.FileNode>(ownerfiletonodes[file.Name].Values));
                }

#if FAILOVER_DEBUG
                {
                    string debugtxt = "file nodes to delegate to new repl workers:" + Environment.NewLine;
                    for(int fi = 0; fi < filestorep.Count; fi++)
                    {
                        debugtxt += Environment.NewLine + filestorep[fi].Name + "==";
                        List<dfs.DfsFile.FileNode> fns = fnodestorep[fi];
                        foreach (dfs.DfsFile.FileNode fn in fns)
                        {
                            debugtxt += fn.Name + ";";
                        }                        
                    }
                    Log(debugtxt);
                }
#endif
                CreateReplWorkers(blockCount, filestorep, fnodestorep);
            }

            private void CreateReplWorkers(int NumberOfCores, List<dfs.DfsFile> files)
            {
                List<List<dfs.DfsFile.FileNode>> filenodes = new List<List<dfs.DfsFile.FileNode>>(files.Count);
                foreach (dfs.DfsFile file in files)
                {
                    filenodes.Add(file.Nodes);
                }
                CreateReplWorkers(NumberOfCores, files, filenodes);
            }

            private void CreateReplWorkers(int NumberOfCores, List<dfs.DfsFile> files, List<List<dfs.DfsFile.FileNode>> filenodes)
            {
                int numwarnings = 0;
                bool verbose = true;
                bool anywork = false;
                int needsayreplicating = 1;
                int replication = failoverShared.dc.Replication;
                string[] slaves = new List<string>(goodHosts.Keys).ToArray();
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

                //Build lists of which hosts pull which chunks...
                for (int fi = 0; fi < files.Count; fi++)
                {
                    dfs.DfsFile df = files[fi];
                    if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                        || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                    {
                        bool printedthisfile = false; // verbose only
                        List<dfs.DfsFile.FileNode> fnodes = filenodes[fi];
                        foreach (dfs.DfsFile.FileNode fn in fnodes)
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
                                        Console.WriteLine("    [{0}]        Replicating dfs://{1}", DateTime.Now.ToString(), dfn);
                                    }

                                    // Find host(s) for the chunk, ensuring it's not already there...
                                    {
                                        int neednhosts = replication - chosts.Length;
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

                                                    infos.DequeueAt(ii);
                                                    infos.Enqueue(info);

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
                                        Console.Error.WriteLine("Create Replication worker error: {0}", e.Message);
                                        Console.Error.Flush();
                                    }
                                }
                                throw new ReplicationException(string.Format("Create Replication worker error: {0}", e.Message), e);
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

                //Randomize pull files list...
                for (int ri = 0; ri < infos.Count; ri++)
                {
                    ReplInfo info = infos[ri];
                    for (int pfir = 0; pfir < info.pullfiles.Count; pfir++)
                    {
                        int ni = rnd.Next(0, info.pullfiles.Count);
                        ReplFileNode trfn = info.pullfiles[ni];
                        info.pullfiles[ni] = info.pullfiles[pfir];
                        info.pullfiles[pfir] = trfn;
                    }
                }

                //Append newly assigned replicate hosts to each filenode, in advance
                for (int ii = 0; ii < infos.Count; ii++)
                {
                    ReplInfo rinfo = infos[ii];
                    for (int ni = 0; ni < rinfo.pullfiles.Count; ni++)
                    {
                        ReplFileNode fn = rinfo.pullfiles[ni];
                        fn.node.Host += ";" + rinfo.host;
                    }
                }

                //Create replworker and start work
                lock (blockStatus)
                {
                    for (int ri = 0; ri < infos.Count; ri++)
                    {
                        ReplWorker worker = new ReplWorker();
                        worker.cID = cID++;
                        blockStatus.Add(worker.cID, 2);   //status = begin replication
                        worker.failover = this;
                        worker.info = infos[ri];
                        worker.infos = infos;
                        worker.machineCompletedString = MachineCompletedString;
                        worker.verbose = true;
                        worker.thread = new System.Threading.Thread(new System.Threading.ThreadStart(worker.ThreadProc));
                        worker.thread.IsBackground = true; //!
                        string whost = worker.info.host.ToLower();
                        if (!hostToReplWorkers.ContainsKey(whost))
                        {
                            hostToReplWorkers.Add(whost, new List<ReplWorker>());
                        }
                        hostToReplWorkers[whost].Add(worker);
                        worker.thread.Start();
                    }
                }

#if FAILOVER_DEBUG
                {
                    string debugtxt = "hostToReplWorkers: " + Environment.NewLine;
                    foreach (KeyValuePair<string, List<ReplWorker>> pair in hostToReplWorkers)
                    {
                        debugtxt += Environment.NewLine + "***host=" + pair.Key + ";replworkers=" + pair.Value.Count.ToString() + Environment.NewLine;
                        foreach (ReplWorker w in pair.Value)
                        {
                            debugtxt += "replworker cid=" + w.cID.ToString() + ";pullfiles:" + Environment.NewLine;
                            foreach (ReplFileNode fn in w.info.pullfiles)
                            {
                                //debugtxt += fn.ownerfile.Name + @"==\\" + fn.node.Host + @"\" + fn.node.Name + Environment.NewLine;
                            }
                        }
                    }
                    Log(debugtxt);
                }
#endif
            }

            internal List<dfs.DfsFile> GetOutputFilesFromESRBlocks()
            {
                //gather output file nodes and sizes from hostToESRBlocks which are not yet in replicatingoutput mode.
                //update dfs with temp filename, save dfs.     
                List<dfs.DfsFile> files = new List<dfs.DfsFile>();                
                lock (hostToESRBlocks)
                {
                    //scramble the esrblocks ordering here.
                    List<MapReduceBlockInfo> scrambled = new List<MapReduceBlockInfo>();
                    foreach (KeyValuePair<string, List<MapReduceBlockInfo>> pair in hostToESRBlocks)
                    {
                        foreach (MapReduceBlockInfo block in pair.Value)
                        {
                            if (!block.replicatingoutput)
                            {
                                scrambled.Add(block);
                                block.replicatingoutput = true;
                            }                            
                        }
                    }
                    for (int bi = 0; bi < scrambled.Count; bi++)
                    {
                        int ind = rnd.Next() % scrambled.Count;
                        MapReduceBlockInfo oldvalue = scrambled[bi];
                        scrambled[bi] = scrambled[ind];
                        scrambled[ind] = oldvalue;
                    }

#if FAILOVER_DEBUG
                    Log("esrblocks going into replication=" + scrambled.Count.ToString());
#endif

                    if (scrambled.Count > 0)
                    {
                        using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                        {
                            // Reload DFS config to make sure changes since starting get rolled in, and make sure the output file wasn't created in that time...
                            dfs dc = LoadDfsConfig(); // Reload in case of change or user modifications.                        

                            for (int nfile = 0; nfile < failoverShared.outputfiles.Count; nfile++)
                            {
                                dfs.DfsFile df = new dfs.DfsFile();
                                string ofile = failoverShared.outputfiles[nfile];
                                if (ofile.Length == 0)
                                {
                                    continue;
                                }
                                if (failoverShared.outputrecordlengths[nfile] > 0)
                                {
                                    df.XFileType = DfsFileTypes.BINARY_RECT + "@" + failoverShared.outputrecordlengths[nfile].ToString();
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

                                if (!dfsnamesToReplnames.ContainsKey(dfsname))
                                {
                                    dfsnamesToReplnames.Add(dfsname, new List<string>(failoverShared.dc.Replication));
                                }
                                dfsnamesToReplnames[dfsname].Add(dfsnamereplicating);
                                files.Add(df);

                                long totalsize = 0;
                                bool anybad = false;
                                bool foundzero = false;                                
                                foreach (MapReduceBlockInfo block in scrambled)
                                {
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
                                //Always produce output file, even if no data.
                                dc.Files.Add(df);
                            }
                            UpdateDfsXml(dc);
                        }
                    }                    
                }

#if FAILOVER_DEBUG
                {
                    string debugtxt = "getoutputfilesfromesrblocks:" + files.Count.ToString() + Environment.NewLine;
                    foreach (dfs.DfsFile file in files)
                    {
                        debugtxt += file.Name + "; nodes: " + file.Nodes.Count.ToString() + Environment.NewLine;
                        foreach (dfs.DfsFile.FileNode fn in file.Nodes)
                        {
                            debugtxt += fn.Host + @"\" + fn.Name + Environment.NewLine;
                        }
                    }
                    Log(debugtxt);
                }
#endif
                
                return files;
            }

            internal void Log(string msg)
            {
                string tempfile = @"c:\temp\failoverlog_102C5560-2012-4537-AFC8-C40ADF0AD2B8.txt";
                lock (typeof(FailoverInfo))
                {
                    using (System.IO.StreamWriter w = new System.IO.StreamWriter(tempfile, true))
                    {
                        w.WriteLine("  [" + DateTime.Now.ToString() + "]");
                        w.WriteLine(msg);
                    }
                }
            }           

            internal class FailoversShared
            {
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
                internal string execopts;
            }

            class ReplWorker
            {
                internal ReplInfo info;
                internal System.Threading.Thread thread = null;
                internal FailoverInfo failover = null;
                internal int cID = -1;
                internal bool verbose = false;
                internal string machineCompletedString = null;
                internal Exception LastException = null;
                internal RAQueue<ReplInfo> infos = null;   //!!!!!!!!!!!!!!!USE SOMETHING ELSE FOR LOCK
                internal string sFailedHosts = null;

                internal void ThreadProc()
                {
                    if (info.pullfiles.Count > 0)
                    {
                        System.Net.Sockets.NetworkStream nstm = null;
                        try
                        {
                            nstm = Surrogate.ConnectService(info.host);

                            string spullpaths;
                            {
                                StringBuilder sbpullpaths = new StringBuilder(info.pullfiles.Count * 100);
                                for (int pfi = 0; pfi < info.pullfiles.Count; pfi++)
                                {
                                    if (0 != sbpullpaths.Length)
                                    {
                                        sbpullpaths.Append('\u0001');
                                    }
                                    ReplFileNode pf = info.pullfiles[pfi];
                                    sbpullpaths.Append(Surrogate.NetworkPathForHost(pf.node.Host.Split(';')[0]) + @"\" + pf.node.Name);
                                    if (pf.ownerfile.HasZsa)
                                    {
                                        sbpullpaths.Append('\u0001');
                                        sbpullpaths.Append(Surrogate.NetworkPathForHost(pf.node.Host.Split(';')[0]) + @"\" + pf.node.Name + ".zsa");
                                    }
                                }
                                spullpaths = sbpullpaths.ToString();
                            }

                            if (ReplicationDebugVerbose)
                            {
                                lock (infos)
                                {
                                    Console.WriteLine("[{0} pulling files: {1}]", info.host, spullpaths.Replace("\u0001", ";").Replace("\u0002", "->"));
                                }
                            }

                            byte[] opts = new byte[1 + 4 + 4 + 16];
                            opts[0] = 0; // Disabled: per-file download feedback.
                            MySpace.DataMining.DistributedObjects.Entry.ToBytes(failover.failoverShared.dc.slave.CookTimeout, opts, 1);
                            MySpace.DataMining.DistributedObjects.Entry.ToBytes(failover.failoverShared.dc.slave.CookRetries, opts, 1 + 4);

                            nstm.WriteByte((byte)'Y'); // Batch send.
                            XContent.SendXContent(nstm, opts);
                            XContent.SendXContent(nstm, spullpaths);
                            XContent.SendXContent(nstm, string.Join(";", failover.healthpluginpaths));

                            if ('+' != nstm.ReadByte())
                            {
                                throw new ReplicationException("Host " + info.host + " did not report a success for bulk file transfer");
                            }

                            //Any reported df?
                            sFailedHosts = XContent.ReceiveXString(nstm, opts);
                        }
                        catch (Exception e)
                        {
                            LastException = e;

                            bool donelogging = false;
                            if (failover.CheckMachineFailure(info.host))
                            {
                                Console.Error.WriteLine("Thread exception: (replication thread): Hardware failure at " + info.host);
                                LogOutputToFile("Thread exception: (replication thread): Hardware failure at " + info.host + ": " + e.ToString());
                                donelogging = true;
                            }

                            if (!donelogging)
                            {
                                LogOutputToFile("Thread exception: (replication thread) error: " + e.ToString());
                                if (verbose)
                                {
                                    ConsoleFlush();
                                    Console.Error.WriteLine("Thread exception: (replication thread): {0}", e.Message);
                                    Console.Error.Flush();                                    
                                }
                            }
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
                            Console.Write(machineCompletedString);
                            ConsoleFlush();
                        }
                    }

                    failover.UpdateBlockStatus(cID, 3);                    
                }
            }
        }
    }
}
