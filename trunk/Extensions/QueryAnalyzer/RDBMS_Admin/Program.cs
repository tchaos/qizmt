using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    public partial class Program
    {
        static string CurrentDir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                ShowUsage();
                return;
            }
             
            string action = args[0].ToLower();          

            switch (action)
            {
                case "regressiontest":
                case "regressiontests":
                    RunRegressionTests(args);
                    break;

                case "killall":
                    KillAll(args);
                    break;

                case "stopall":
                    StopAll(args);
                    break;

                case "startall":
                    StartAll(args);
                    break;

                case "viewlog":
                case "viewlogs":
                    ViewLog(args);
                    break;

                case "clearlog":
                case "clearlogs":
                    ClearLog(args);
                    break;

                case "version": 
                case "ver":
                    Console.WriteLine("Version: " + (GetBuildDateTime().ToString()).Replace(":", ".").Replace(" ", ".").Replace("/", ".").Replace("AM", "A").Replace("PM", "P"));
                    break;

                case "examples":
                case "example":
                    GenerateExamples();
                    break;

                case "rindexfilteringstresstest":
                    RunRIndexFilteringStressTest(args);
                    break;

                case "rindexbasicstresstest":
                    GenerateRIndexBasicStressTest(args);
                    break;

                default:
                    Console.Error.WriteLine("Not valid: RDBMS_Admin {0}", action);
                    break;
            }
        }

        static void ShowUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("    RDBMS_admin <action> [<arguments>]");
            Console.WriteLine("Actions:");           
            Console.WriteLine("    killall                 kill all protocol services");
            Console.WriteLine("    stopall                 stop all protocol services");
            Console.WriteLine("    startall                start all protocol services");
            Console.WriteLine("    version                 get version of protocol service");
            Console.WriteLine("    viewlog                 view log entries");
            Console.WriteLine("    clearlog                clear logs entries");
            Console.WriteLine("    examples                generate built-in examples");
            Console.WriteLine("    rindexfilteringstresstest");
            Console.WriteLine("                            [maxPrimary] [maxAssociations]");
            Console.WriteLine("                            [maxSharedAssociations] [batchSize]");
            Console.WriteLine("                            [-v verbose]");
            Console.WriteLine("                            generate and run rindex filtering stress test");
            Console.WriteLine("    rindexbasicstresstest   generate a basic rindex stress test");
        }

        static void GetRawBuildInfo(out int bn, out int rv)
        {
            string execpath = CurrentDir + @"\QueryAnalyzer_Protocol.exe";            
            System.Reflection.Assembly asm = System.Reflection.Assembly.LoadFile(execpath);
            System.Reflection.AssemblyName an = asm.GetName();
            bn = an.Version.Build;
            rv = an.Version.Revision;           
        }

        static DateTime GetBuildDateTime()
        {
            int bn;
            int rv;
            GetRawBuildInfo(out bn, out rv);
            DateTime dt = new DateTime(2000, 1, 1, 0, 0, 0);
            dt += TimeSpan.FromDays(bn) + TimeSpan.FromSeconds(rv * 2);
            return dt;
        }
    }
}
