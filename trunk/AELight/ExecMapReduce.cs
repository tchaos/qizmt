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

    public const int DSpace_InputRecordLength = " + MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength.ToString() + @";
    public const int Qizmt_InputRecordLength = DSpace_InputRecordLength;

    public const int DSpace_OutputRecordLength = " + MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength.ToString() + @";
    public const int Qizmt_OutputRecordLength = DSpace_OutputRecordLength;

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
                    if (connbacklog.ContainsKey(SlaveIP))
                    {
                        openlock = connbacklog[SlaveIP];
                    }
                    else
                    {
                        openlock = SlaveIP;
                        connbacklog[SlaveIP] = SlaveIP;
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
                            LogOutput("Thread exception: (first thread) " + e.ToString());
                        }
                    }
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
                List<dfs.DfsFile> newsnowfiles = new List<dfs.DfsFile>();
                List<dfs.DfsFile.FileNode> newsnownodes = new List<dfs.DfsFile.FileNode>();
                List<string> newsnowfileswithnodes = new List<string>();
                List<int> nodesoffsets = new List<int>();
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
                    _ExecOneMapReduce(ExecOpts, cfgj, ExecArgs, verbose, verbosereplication, newsnownodes, newsnowfileswithnodes, nodesoffsets);
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
                    _ExecOneMapReduce(ExecOpts, cfgj, ExecArgs, verbose, verbosereplication, null, null, null);
                }
                catch (ExceptionFailoverRetryable e)
                {
                    if (e.ShouldRetry)
                    {
                        LogOutputToFile("Failing over: " + e.Message);
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
                        LogOutputToFile("Not failing over, failover disabled: " + e.Message);
                    }
                }
                break;
            }

        }

        // If AddCacheFiles is non-null, it's a ADD-CACHE-ONLY run!
        static void _ExecOneMapReduce(string ExecOpts, SourceCode.Job cfgj, string[] ExecArgs, bool verbose, bool verbosereplication, List<dfs.DfsFile.FileNode> AddCacheNodes, List<string> AddCacheDfsFileNames, List<int> AddCacheNodesOffsets)
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

                List<string> outputfiles = new List<string>();
                {
                    int RecordLength = Int32.MinValue;
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
                            if (RecordLength != Int32.MinValue && RecordLength != reclen)
                            {
                                Console.Error.WriteLine("Error: all map outputs must have the same record length: {0}", thisfile);
                                SetFailure();
                                return;
                            }
                            RecordLength = reclen;

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
                        }
                    }
                    else
                    {
                        outputfiles.Add("");
                    }
                    MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength = RecordLength > 0 ? RecordLength : -1;                    
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
                if (dc.Replication > 1 || cfgj.IsJobFailoverEnabled)
                {
                    List<string> removedslaves = new List<string>();
                    slaves = ExcludeUnhealthySlaveMachines(slaves, removedslaves, true).ToArray();
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
                            }
                            else
                            {
                                IList<string> mapfiles = SplitInputPaths(dc, cfgj.IOSettings.DFSInput);
                                mapfileswithnodes = new List<string>(mapfiles.Count);
                                inputnodesoffsets = new List<int>(mapfiles.Count);
                                int RecordLength = int.MinValue;
                                for (int i = 0; i < mapfiles.Count; i++)
                                {
                                    string dp = mapfiles[i];
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
                                                if (RecordLength != int.MinValue && RecordLength != reclen)
                                                {
#if DEBUG
                                                    System.Diagnostics.Debugger.Launch();
#endif
                                                    Console.Error.WriteLine("Error: all map inputs must have the same record length: {0}", dp);
                                                    SetFailure();
                                                    return;
                                                }
                                                RecordLength = reclen;
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
                                            RecordLength = -1;
                                        }
                                    }
                                    dfs.DfsFile df;
                                    if (RecordLength > 0)
                                    {
                                        df = DfsFind(dc, dp, DfsFileTypes.BINARY_RECT);
                                        if (null != df && RecordLength != df.RecordLength)
                                        {
                                            Console.Error.WriteLine("Error: map input file does not have expected record length of {0}: {1}@{2}", RecordLength, dp, df.RecordLength);
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
                                    }                                    
                                    mapinputchunks.AddRange(df.Nodes);                                    
                                }
                                if (RecordLength != int.MinValue)
                                {
                                    if (MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength != RecordLength)
                                    {
                                        if (int.MinValue != MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength)
                                        {
                                            Console.Error.WriteLine("Record lengths are not consistent ({0} != {1}) between phases",
                                                RecordLength,
                                                MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength);
                                            return;
                                        }
                                        MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_InputRecordLength = RecordLength;
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
                        bi.acl.SetJID(jid);
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
                                curfilename = mapfileswithnodes[fi++];
                                if (fi < inputnodesoffsets.Count)
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
                                    //blocks[leastindex].mapinputdfsnodes.Add(mapinputchunks[mi].Name);
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
                    if (0 == string.Compare(jobshared.noutputmethod, "rsorted")
                        || 0 == string.Compare(jobshared.noutputmethod, "fsorted"))
                    {
                        // fsorted has another 'foil load' phase with join...
                        int FoilKeySkipFactor = blocks[0].acl.FoilKeySkipFactor;
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
                            }
                        }
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            MapReduceBlockInfo bi = blocks[i];
                            bi.acl.FoilKeySkipFactor = FoilKeySkipFactor;
                            bi.all = blocks;
                            bi.thread = new System.Threading.Thread(new System.Threading.ThreadStart(bi.fsortedthreadproc));
                            bi.thread.IsBackground = true;
                            bi.thread.Start();
                        }
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            MapReduceBlockInfo bi = blocks[i];
                            bi.thread.Join();
                        }
                        if (verbose)
                        {
                            Console.WriteLine((extraverbose ? "\r\n" : "") + "    [{0}]        Distribution index done; starting map", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
                        }
                    }
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
                        bi.thread.IsBackground = true;
                        bi.thread.Start();
                    }
                    {
                        Exception ee = null;
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            blocks[i].thread.Join();
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
                    if (null != mapinputchunks) // Only null if DirectSlaveLoad
                    {
                        if (verbose)
                        {
                            Console.WriteLine((extraverbose ? "\r\n" : "") + "    [{0}]        Map done; starting map exchange", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
                        }
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            blocks[i].thread = new System.Threading.Thread(new System.Threading.ThreadStart(blocks[i].exchangethreadproc));
                            blocks[i].thread.IsBackground = true;
                            blocks[i].thread.Start();
                        }
                        {
                            Exception ee = null;
                            for (int i = 0; i < blocks.Count; i++)
                            {
                                blocks[i].thread.Join();
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
                        for (int i = 0; i < blocks.Count; i++)
                        {
                            blocks[i].acl.StopZMapBlockServer();
                            blocks[i].acl.Close();
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
                            for (int nfile = 0; nfile < outputfiles.Count; nfile++)
                            {
                                string ofile = outputfiles[nfile];
                                if (ofile.Length == 0)
                                {
                                    continue;
                                }
                                string dfsname;
                                string dfsnamereplicating;
                                // Reload DFS config to make sure changes since starting get rolled in, and make sure the output file wasn't created in that time...
                                using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                                {
                                    dc = LoadDfsConfig(); // Reload in case of change or user modifications.
                                    {
                                        dfs.DfsFile df = new dfs.DfsFile();
                                        if (MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength > 0)
                                        {
                                            df.XFileType = DfsFileTypes.BINARY_RECT + "@" + MySpace.DataMining.DistributedObjects.StaticGlobals.DSpace_OutputRecordLength.ToString();
                                        }
                                        df.Nodes = new List<dfs.DfsFile.FileNode>();
                                        df.Size = -1; // Preset
                                        dfsname = ofile; // Init.
                                        if (dfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                        {
                                            dfsname = dfsname.Substring(6);
                                        }
                                        dfsnamereplicating = ".$" + dfsname + ".$replicating-" + Guid.NewGuid().ToString();
                                        df.Name = dfsnamereplicating;
                                        if (null != DfsFind(dc, df.Name))
                                        {
                                            Console.Error.WriteLine("Error:  output file '{0}' was created during job: " + df.Name, ofile);
                                            return;
                                        }
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
                                        {
                                            dc.Files.Add(df);
                                            UpdateDfsXml(dc); // !
                                        }   
                                    }
                                }
                                {
#if MR_REPLICATION_TIME_PRINT
                                    DateTime replstart = DateTime.Now;
#endif
                                    if (ReplicationPhase(dfsnamereplicating, verbosereplication, jobshared.blockcount, slaves))
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
                                    dfs.DfsFile dfu = dc.FindAny(dfsnamereplicating);
                                    if (null != dfu)
                                    {
                                        if (null != DfsFindAny(dc, dfsname))
                                        {
                                            Console.Error.WriteLine("Error:  output file '{0}' was created during job", ofile);
                                            return;
                                        }
                                        dfu.Name = dfsname;
                                        UpdateDfsXml(dc);
                                    }
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
