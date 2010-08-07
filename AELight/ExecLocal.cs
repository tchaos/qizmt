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
        public static void ExecOneLocal(SourceCode.Job cfgj, string[] ExecArgs, bool verbose)
        {
            if (verbose)
            {
                Console.WriteLine("[{0}]        [Local: {2}]", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, cfgj.NarrativeName);
            }

            int BlockID = 0;
            string SlaveIP = null;

            string logname = Surrogate.SafeTextPath(cfgj.NarrativeName) + "_" + Guid.NewGuid().ToString() + ".j" + sjid + "_log.txt";

            bool aborting = false;
            try
            {
                dfs dc = LoadDfsConfig();
                string firstslave = dc.Slaves.GetFirstSlave();

                string SlaveHost = cfgj.IOSettings.LocalHost;
                if (SlaveHost == null || SlaveHost.Length == 0)
                {
                    SlaveHost = firstslave;
                }
                SlaveIP = IPAddressUtil.GetIPv4Address(SlaveHost);

                MySpace.DataMining.DistributedObjects5.Remote rem = new MySpace.DataMining.DistributedObjects5.Remote(cfgj.NarrativeName + "_local");
                rem.OutputStartingPoint = BlockID;
                rem.LocalCompile = true;
                rem.CompilerOptions = cfgj.IOSettings.CompilerOptions;
                rem.CompilerVersion = cfgj.IOSettings.CompilerVersion;
                if (cfgj.OpenCVExtension != null)
                {
                    rem.AddOpenCVExtension();
                }
                if (cfgj.MemCache != null)
                {
                    rem.AddMemCacheExtension();
                }
                if (cfgj.Unsafe != null)
                {
                    rem.AddUnsafe();
                }
                if (cfgj.AssemblyReferencesCount > 0)
                {
                    cfgj.AddAssemblyReferences(rem.CompilerAssemblyReferences, Surrogate.NetworkPathForHost(firstslave));
                }
                rem.SetJID(jid, CurrentJobFileName + " Local: " + cfgj.NarrativeName);
                rem.AddBlock(SlaveHost + @"|"
                    + (cfgj.ForceStandardError != null ? "&" : "") + logname + @"|slaveid=0");
                rem.Open();

                string codectx = (@"

    public const int DSpace_BlockID = " + BlockID.ToString() + @";
    public const int DSpace_ProcessID = DSpace_BlockID;
    public const int Qizmt_ProcessID = DSpace_ProcessID;

    public const int DSpace_BlocksTotalCount = 1;
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

    public const string DSpace_ExecDir = @`" + System.Environment.CurrentDirectory + @"`;
    public const string Qizmt_ExecDir = DSpace_ExecDir;


    static string Shell(string line, bool suppresserrors)
    {
        return MySpace.DataMining.DistributedObjects.Exec.Shell(line, suppresserrors);
    }


    static string Shell(string line)
    {
        return MySpace.DataMining.DistributedObjects.Exec.Shell(line, false);
    }


const string _userlogname = `" + logname + @"`;
static System.Threading.Mutex _logmutex = new System.Threading.Mutex(false, `distobjlog`);

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

                rem.LocalExec(codectx + cfgj.Local, cfgj.Usings);
                rem.Close();

                if (verbose)
                {
                    Console.Write('*');
                    ConsoleFlush();
                }

            }
            catch (System.Threading.ThreadAbortException)
            {
                aborting = true;
            }
            finally
            {
                if (!aborting)
                {
                    CheckUserLogs(new string[] { SlaveIP }, logname);
                }
            }

            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine("[{0}]        Done", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
            }
        }
    }
}
