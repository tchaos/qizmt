using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

using MySpace.DataMining.AELight;

namespace RegressionTest2
{
    public partial class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                ShowUsage();
                return;
            }

            string action = args[0].ToLower();

            if (args.Length > 1 && "..." == args[1])
            {
                args[1] = Exec.Shell("Qizmt metapath").Trim();
            }

            try
            {
                switch (action)
                {
                    case "mrdebug":
                        Mrdebug(args);
                        break;

                    case "removesurrogate":
                        RemoveSurrogate(args);
                        break;

                    case "removemachine":
                    case "removemachine2to1":
                    case "removemachine3to2":
                        RemoveMachine(args);
                        break;

                    case "metaremove":
                    case "metaremovemachine":
                        MetaRemoveMachine(args);
                        break;

                    case "enablereplication":
                    case "enablereplicationwithcache":
                        EnableReplication(args);
                        break;

                    case "cachewithredundancy":
                        CacheWithRedundancy(args);
                        break;

                    case "replication":
                        Replication(args);
                        break;

                    case "replicationfailover":
                        ReplicationFailover(args);
                        break;

                    case "cachenofailover":
                        CacheNoFailover(args);
                        break;

                    case "metapath":
                        MetaPath(args);
                        break;

                    case "servicecommands":
                        ServiceCommands(args);
                        break;

                    case "deploy":
                        Deploy(args);
                        break;

                    case "killall":
                        Killall(args);
                        break;

                    case "kill":
                        Kill(args);
                        break;

                    case "dfsupdatestresstest":
                        DfsUpdateStressTest(args);
                        break;

                    case "rsorted":
                    case "rhashsorted":
                        RangeSort(args);
                        break;

                    case "metadatacomplexity":
                    case "metadatacomplexitylargesort":
                        MetaDataComplexity(args);
                        break;

                    case "reducefinalizethrow":
                        ReduceFinalizeThrow(args);
                        break;

                    case "reduceinitializethrow":
                        ReduceInitializeThrow(args);
                        break;

                    case "replicationchecks":
                        ReplicationChecks(args);
                        break;

                    case "sortedcache":
                        SortedCache(args);
                        break;

                    case "globalcriticalsectionkillall":
                        GlobalCriticalSectionKillall(args);
                        break;

                    case "newdfs":
                        NewDFS(args); // Test.
                        break;

                    default:
                        Console.Error.WriteLine("Unknown action: {0}", action);
                        Environment.Exit(1);
                        break;
                }
            }
            catch
            {
                Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                throw;
            }

        }


        private static void ShowUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("    RegressionTest2 <action> [<arguments>]");
            Console.WriteLine("Actions:");
            Console.WriteLine("    mrdebug [<mrdebug-test-name>]");
            Console.WriteLine("    RemoveSurrogate <dfsxmlpath> <incluster|isolated>");
            Console.WriteLine("    RemoveMachine <dfsxmlpath> [#<replication>]");
            Console.WriteLine("    RemoveMachine2to1 <dfsxmlpath> [#<replication>]");
            Console.WriteLine("    RemoveMachine3to2 <dfsxmlpath> [#<replication>]");
            Console.WriteLine("    EnableReplication <dfsxmlpath> [<bytes-to-add> [<#files>]]");
            Console.WriteLine("    EnableReplicationWithCache <dfsxmlpath> [<bytes-to-add> [<#files>]]");
            Console.WriteLine("    CacheWithRedundancy <dfsxmlpath>");
            Console.WriteLine("    Replication <dfsxmlpath>");
            Console.WriteLine("    ReplicationFailover <dfsxmlpath>");
            Console.WriteLine("    MetaPath [<dfsxmlpath>]");
            Console.WriteLine("    ServiceCommands <dfsxmlpath>");
            Console.WriteLine("    CacheNoFailover <dfsxmlpath>");
            Console.WriteLine("    deploy <dfsxmlpath>");
            Console.WriteLine("    killall <dfsxmlpath>");
            Console.WriteLine("    kill <dfsxmlpath>");
            Console.WriteLine("    DfsUpdateStressTest <dfsxmlpath>");
            Console.WriteLine("    rsorted <dfsxmlpath> [pause]");
            Console.WriteLine("    rhashsorted <dfsxmlpath> [pause]");
            Console.WriteLine("    MetaDataComplexity <dfsxmlpath> [<bytes-of-metadata>] [#<replication>]");
            Console.WriteLine("    MetaDataComplexityLargeSort <dfsxmlpath> [<bytes-of-md>] [#<replication>]");
            Console.WriteLine("    ReduceFinalizeThrow <dfsxmlpath>");
            Console.WriteLine("    ReduceInitializeThrow <dfsxmlpath>");
            Console.WriteLine("    ReplicationChecks <dfsxmlpath>");
            Console.WriteLine("    SortedCache <dfsxmlpath>");
            Console.WriteLine("    GlobalCriticalSectionKillall <dfsxmlpath>");
            Console.WriteLine("    MetaRemove <dfsxmlpath>");
        }


        static void EnsurePerfectQizmtHealthaOutput(string healthaoutput)
        {
            StringBuilder sbx = new StringBuilder(healthaoutput.Length);
            for(int i = 0; i < healthaoutput.Length; i++)
            {
                switch (healthaoutput[i])
                {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        break;
                    default:
                        sbx.Append(healthaoutput[i]);
                        break;
                }
            }
            string x = sbx.ToString();
            if (-1 == x.IndexOf("[MachinesHealth]100%healthy"))
            {
                throw new Exception("Health check: machines not 100% healthy: " + healthaoutput);
            }
            if (-1 == x.IndexOf("[DFSHealth]100%healthy"))
            {
                throw new Exception("Health check: DFS not 100% healthy: " + healthaoutput);
            }
        }

        static void EnsurePerfectQizmtHealtha()
        {
            EnsurePerfectQizmtHealthaOutput(Exec.Shell("Qizmt health -a"));
        }


        static string DfsSum(string sumcmd, string dfsfile)
        {
            string output = Exec.Shell("Qizmt \"" + sumcmd + "\" \"" + dfsfile + "\"");
            int ilc = output.LastIndexOf(':');
            if (-1 == ilc)
            {
                throw new Exception("Unexpected output for " + sumcmd);
            }
            return output.Substring(ilc + 1).Trim();
        }

        static string DfsSum(string dfsfile)
        {
            return DfsSum("sum", dfsfile);
        }


        public class IPAddressUtil
        {

            public static string GetIPv4Address(string HostnameOrIP)
            {
                System.Net.IPAddress[] addresslist = System.Net.Dns.GetHostAddresses(HostnameOrIP);
                for (int i = 0; i < addresslist.Length; i++)
                {
                    if (System.Net.Sockets.AddressFamily.InterNetwork == addresslist[i].AddressFamily)
                    {
                        return addresslist[i].ToString();
                    }
                }
                throw new Exception("IPAddressUtil.GetAddress: No IPv4 address found for " + HostnameOrIP);
            }

            public static string GetNameNoCache(string ipaddr)
            {
                try
                {
                    System.Net.IPHostEntry iphe = System.Net.Dns.GetHostEntry(ipaddr);
                    if (null == iphe || null == iphe.HostName)
                    {
                        return ipaddr;
                    }
                    return iphe.HostName;
                }
                catch (Exception e)
                {
#if CLIENT_LOG_ALL
                AELight.LogOutputToFile("CLIENT_LOG_ALL: IPAddressUtil.GetNameNoCache: unable to lookup host for IP address '" + ipaddr + "': " + e.ToString());
#endif
                    return ipaddr;
                }
            }

            static Dictionary<string, string> namecache = new Dictionary<string, string>(new Surrogate.CaseInsensitiveEqualityComparer());
            public static string GetName(string ipaddr)
            {
                lock (namecache)
                {
                    if (!namecache.ContainsKey(ipaddr))
                    {
                        string result = GetNameNoCache(ipaddr);
                        namecache[ipaddr] = result;
                        //namecache[result] = result;
                        return result;
                    }
                }
                return namecache[ipaddr];
            }

            public static void FlushCachedNames()
            {
                lock (namecache)
                {
                    namecache = new Dictionary<string, string>(new Surrogate.CaseInsensitiveEqualityComparer());
                }
            }

        }


        public class ShellCtrl: System.IO.TextReader
        {
            private System.Diagnostics.Process m_Process;
            private System.IO.StreamWriter m_input;
            private System.IO.StreamReader m_output;
            private System.Diagnostics.ProcessStartInfo m_starInfo;

            public ShellCtrl(string sPath, bool OutputReader)
            {
                int isp = sPath.IndexOf(' ');
                string sExe;
                string sParam;
                if (-1 == isp)
                {
                    sExe = sPath;
                    sParam = "";
                }
                else
                {
                    sExe = sPath.Substring(0, isp);
                    sParam = sPath.Substring(isp + 1);
                }
                m_starInfo = new System.Diagnostics.ProcessStartInfo(sExe, sParam);
                m_starInfo.UseShellExecute = false;
                m_starInfo.RedirectStandardInput = true;
                if (OutputReader)
                {
                    m_starInfo.RedirectStandardOutput = true;
                }
                m_Process = System.Diagnostics.Process.Start(m_starInfo);
                m_input = m_Process.StandardInput;
                if (OutputReader)
                {
                    m_output = m_Process.StandardOutput;
                }
            }

            public void WriteLine(string s)
            {
                m_input.WriteLine(s);
            }

            public void Write(string s)
            {
                m_input.Write(s);
            }

            public override string ReadLine()
            {
                _canread();
                return m_output.ReadLine();
            }

            public override string ReadToEnd()
            {
                _canread();
                return m_output.ReadToEnd();
            }

            public override int Read(char[] buffer, int index, int count)
            {
                _canread();
                return m_output.Read(buffer, index, count);
            }

            public override int Read()
            {
                _canread();
                return m_output.Read();
            }

            void _canread()
            {
                if (null == m_output)
                {
                    throw new Exception("Read not available");
                }
            }

        }


    }
}
