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
        public static void ExecOneTest(SourceCode.Job cfgj, string[] ExecArgs, bool verbose)
        {
            if (verbose)
            {
                Console.WriteLine("[{0}]        [Test: {2}]", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, cfgj.NarrativeName);
            }

            //string SlaveIP = IPAddressUtil.GetIPv4Address(cfgj.IOSettings.LocalHost);

            int BlockID = 0;

            string logname = Surrogate.SafeTextPath(cfgj.NarrativeName) + "_" + Guid.NewGuid().ToString() + ".j" + sjid + "_log.txt";

            try
            {
                string outputguid = Guid.NewGuid().ToString();
                TestRemote dobj = new TestRemote(cfgj.NarrativeName + "_test");
                string outputfilename = outputguid + ".local";
                dobj.SetJID(jid, CurrentJobFileName + " Test: " + cfgj.NarrativeName);
                dobj.AddBlock(@"127.0.0.1|" + outputfilename + @".log|slaveid=0");
                string codectx = (@"
    public const int DSpace_BlockID = 0;
    public const int DSpace_ProcessID = DSpace_BlockID;
    public const int Qizmt_ProcessID = DSpace_ProcessID;

    public const int DSpace_BlocksTotalCount = 1;
    public const int DSpace_ProcessCount = DSpace_BlocksTotalCount;
    public const int Qizmt_ProcessCount = DSpace_ProcessCount;

    public const string DSpace_SlaveHost = `localhost`;
    public const string DSpace_MachineHost = DSpace_SlaveHost;
    public const string Qizmt_MachineHost = DSpace_MachineHost;

    public const string DSpace_SlaveIP = `127.0.0.1`;
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


    public static void Qizmt_Log(string line) { DSpace_Log(line); }
    public static void DSpace_Log(string line)
    {
        Console.WriteLine(line);
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

").Replace('`', '"') + MySpace.DataMining.DistributedObjects.CommonCs.CommonDynamicCsCode;
                dobj.LocalExec(codectx + "\r\n" + cfgj.Test, cfgj.Usings, "Test");
                string fullsource = dobj.RemoteSource;

                System.Reflection.Assembly asm = null;
                try
                {
                    dobj.CompilePluginSource(fullsource, true, ref asm);
                }
                catch (BadImageFormatException)
                {
                }
                // dobj.RemoteClassName is of type IRemote

                MySpace.DataMining.DistributedObjects.IRemote iface = _LoadPluginInterface<MySpace.DataMining.DistributedObjects.IRemote>(asm, dobj.RemoteClassName);
                iface.OnRemote(); // !

                if (verbose)
                {
                    Console.Write('*');
                    ConsoleFlush();
                }

            }
            finally
            {
                //CheckUserLogs(new string[] { SlaveIP }, logname);
            }

            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine("[{0}]        Done", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond);
            }
        }


        // Local copy; doesn't have custom exceptions.
        public static Iface _LoadPluginInterface<Iface>(System.Reflection.Assembly assembly, string classname)
        {
            try
            {
                Iface plugin = default(Iface);
                string ifacename = typeof(Iface).FullName;
                foreach (Type type in assembly.GetTypes())
                {
                    if (type.IsClass)
                    {
                        if (null == classname
                            || 0 == string.Compare(type.Name, classname))
                        {
                            if (type.GetInterface(ifacename) != null)
                            {
                                plugin = (Iface)System.Activator.CreateInstance(type);
                                break;
                            }
                            if (null != classname)
                            {
                                throw new Exception("Class " + classname + " was found, but does not implement interface " + typeof(Iface).Name);
                            }
                        }
                    }
                }
                if (null == plugin)
                {
                    throw new Exception("Plugin from '" + assembly.FullName + "' not found");
                }
                return plugin;
            }
            catch (System.Reflection.ReflectionTypeLoadException e)
            {
                string x = "";
                foreach (Exception ex in e.LoaderExceptions)
                {
                    x += "\n\t";
                    x += ex.Message;
                }
                throw new Exception("ReflectionTypeLoadException error(s) with plugin '" + assembly.FullName + "': " + x);
            }
        }


        public class TestRemote : MySpace.DataMining.DistributedObjects5.Remote
        {
            public TestRemote(string objectname)
                : base("Test_" + objectname)
            {
            }


            public string RemoteSource = null;
            public string RemoteClassName = null;

            public override void RemoteExecFullSource(string code, string classname)
            {
                RemoteSource = code;
                RemoteClassName = classname;
            }

            protected override int GetNumberOfRemoteOutputFilesCreated(int n, IList<long> appendsizes)
            {
                throw new NotSupportedException();
            }

            protected override void GetDGlobals()
            {
                //
            }

            public override void Open()
            {
                throw new NotSupportedException();
            }

        }


    }
}
