using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace RegressionTest
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                ShowUsage();
                return;
            }

            string action = args[0].ToLower();

            try
            {
                switch (action)
                {
                    case "clustercheck":
                        ClusterCheck.TestClusterCheck(args);
                        break;
                    case "dspacegetfilepermission":
                    case "qizmtgetfilepermission":
                        DSpace.TestDSpaceGetFilePermissions(args);
                        break;
                    case "dspacegetbinaryfilepermission":
                    case "qizmtgetbinaryfilepermission":
                        DSpace.TestDSpaceGetBinaryFilePermissions(args);
                        break;
                    case "localjobhost":
                        LocalJob.TestHost(args);
                        break;
                    case "localjobaddrefnonparticipatingcluster":
                        LocalJob.TestAddRefNonParticipatingCluster(args);
                        break;
                    case "iocook":
                        IOCook.TestIOCook(args);
                        break;
                    case "dspacehosts":
                    case "qizmthosts":
                        DSpace.TestDSpaceHosts(args);
                        break;
                    case "dspaceformatmetaonlyswitch":
                    case "qizmtformatmetaonlyswitch":
                        DSpace.TestFormatMetaOnlySwitch(args);
                        break;
                    case "addremovemachineclearcache":
                        DSpace.TestAddRemoveMachineClearCache(args);
                        break;
                    case "criticalsection":
                        CriticalSection.TestCriticalSection(args);
                        break;
                    case "perfmonadmincommandlock":
                    case "packetsniffadmincommandlock":
                        DSpace.TestAdminCommandLock(action, args);
                        break;
                    case "clearlogs":
                        DSpace.TestClearLogs(args);
                        break;
                    case "killallproxy":
                        KillallProxy.TestKillallProxy(args);
                        break;
                    case "speculativecomputingtesthdfailurebeforemapstarts":
                        SpeculativeComputingMapPhase.TestHDFailureBeforeMapStarts(new string[]{"TestHDFailureBeforeMapStarts"});
                        break;
                    case "speculativecomputingtesthdfailureaftermapstarts":
                        SpeculativeComputingMapPhase.TestHDFailureAfterMapStarts(new string[] { "TestHDFailureAfterMapStarts" });
                        break;
                    default:
                        ShowUsage();
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
            Console.WriteLine("    regressionTest <action> [<arguments>]");
            Console.WriteLine("Actions:");
            Console.WriteLine("    ClusterCheck <dfsXmlPath> [verbose]");
            Console.WriteLine("    CriticalSection");
            Console.WriteLine("    ClearLogs");
            Console.WriteLine("    QizmtHosts <dfsXmlPath>");
            Console.WriteLine("    QizmtFormatMetaOnlySwitch <dfsXmlPath>");
            Console.WriteLine("    QizmtGetFilePermission");
            Console.WriteLine("    QizmtGetbinaryFilePermission");
            Console.WriteLine("    LocalJobHost <dfsXmlPath>");
            Console.WriteLine("    LocalJobAddRefNonParticipatingCluster <dfsXmlPath>");
            Console.WriteLine("    AddRemoveMachineClearCache <dfsXmlPath>");
            Console.WriteLine("    IOCook [writeCount=<number>] ");
            Console.WriteLine("           [readCount=<number>]");
            Console.WriteLine("           [writeDir=<write directory>]");
            Console.WriteLine("           [readDir=<read directory>]");
            Console.WriteLine("           [readExisting=<bool>]");
            Console.WriteLine("           [writeExisting=<bool>]");
            Console.WriteLine("    PerfmonAdminCommandLock <dfsXmlPath>");
            Console.WriteLine("    PacketSniffAdminCommandLock <dfsXmlPath>");
            Console.WriteLine("    KillallProxy");
            Console.WriteLine("    SpeculativeComputingTestHDFailureBeforeMapStarts");
            Console.WriteLine("    SpeculativeComputingTestHDFailureAfterMapStarts");
        }        
    }
}