using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {

        public static readonly string[] RegressionTestsWildcards = new string[] {
            "regression_test_*.xml",
            "failing_regression_test_*.xml",
            "*managers_test_*.xml"
            };


        public static void RunRegressionTests(string[] args)
        {
            string RunTestsType = "normal";
            if (args.Length > 0)
            {
                RunTestsType = args[0].ToLower();
                if ("all" == RunTestsType || "managers" == RunTestsType)
                {
                    Console.WriteLine("Managers tests requirements:");
                    Console.WriteLine("  o  Cluster of at least 2 machines");
                    Console.WriteLine("  o  At least 100 GB free disk space per installation");
                    Console.WriteLine("  o  At least 2 GB of RAM per machine");
                    Console.WriteLine("  o  Must be able to have all MR.DFS data and jobs lost");
                    Console.WriteLine("  o  Must not run any other jobs or commands during tests");
                    Console.WriteLine();
                    System.Threading.Thread.Sleep(1000);
                }
                if ("all" == RunTestsType)
                {
                    if (args.Length <= 1 || "-f" != args[1])
                    {
                        Console.Error.WriteLine("Must use -f switch to run all: {0} regressiontest all -f", appname);
                        return;
                    }
                }
            }

            if ("-delete" == RunTestsType)
            {
                Console.WriteLine("Deleting...");
                DeleteRegressionTests();
                Console.WriteLine("Done");
                return;
            }

            if ("basic" == RunTestsType)
            {
                Console.WriteLine("Basic test run!");
            }

            bool IsTestTest = false;
            if ("-test" == RunTestsType)
            {
                IsTestTest = true;
                Console.WriteLine("TESTING: only running one test of each type!");
            }

            //string dfsxmlpath = Shell("qizmt metapath").Trim();
            string dfsxmlpath = Environment.CurrentDirectory + @"\" + dfs.DFSXMLNAME;

            //string SurrogateDir = (new System.IO.FileInfo(dfsxmlpath)).DirectoryName;
            string SurrogateDir = Environment.CurrentDirectory;
            string RegressionTestsDir = SurrogateDir + @"\RegressionTests";

            DateTime ImportStartTime = DateTime.Now;
            Console.WriteLine("[{0}]  Importing regression tests...", ImportStartTime.ToString());
            string tempjobsdir = RegressionTestsDir + @"\Jobs" + Guid.NewGuid().ToString().Substring(0, 6);
            try
            {
                System.IO.Directory.Delete(tempjobsdir, true);
            }
            catch
            {
            }
            System.IO.Directory.CreateDirectory(tempjobsdir);
            System.Text.RegularExpressions.Regex[] RegressionTestsRegexes
                = new System.Text.RegularExpressions.Regex[RegressionTestsWildcards.Length];
            for (int ir = 0; ir < RegressionTestsRegexes.Length; ir++)
            {
                string rtdirwc = RegressionTestsWildcards[ir];
                string rtdirsregex = Surrogate.WildcardRegexString(rtdirwc);
                System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(
                    rtdirsregex,
                    System.Text.RegularExpressions.RegexOptions.Compiled
                    | System.Text.RegularExpressions.RegexOptions.IgnoreCase
                    | System.Text.RegularExpressions.RegexOptions.Singleline);
                RegressionTestsRegexes[ir] = rex;
            }
            foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(RegressionTestsDir)).GetFiles())
            {
                for (int ir = 0; ir < RegressionTestsRegexes.Length; ir++)
                {
                    if (RegressionTestsRegexes[ir].IsMatch(fi.Name))
                    {
                        fi.CopyTo(tempjobsdir + @"\" + fi.Name, true);
                    }
                }
            }
            Shell("qizmt importdirmt \"" + tempjobsdir + "\"", true);
            try
            {
                System.IO.Directory.Delete(tempjobsdir, true);
            }
            catch
            {
            }
            Console.WriteLine("    Import completed; duration: {0}", (DateTime.Now - ImportStartTime).ToString());

            Console.WriteLine();
            DateTime RunStartTime = DateTime.Now;
            Console.WriteLine("[{0}]  Running tests...", RunStartTime.ToString());

            List<string> AllTestResults = new List<string>();

            {
                string TestType = "normal";
                if (IsTestTest || "basic" == RunTestsType || "all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Qizmt exec regression_test_testDriver.xml";
                    if (IsTestTest)
                    {
                        tcmd += " -test";
                    }
                    else if ("basic" == RunTestsType)
                    {
                        tcmd += " -basic";
                    }
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if (IsTestTest || "basic" == RunTestsType || "all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Qizmt exec regression_test_testDriver2.xml";
                    if (IsTestTest)
                    {
                        tcmd += " -test";
                    }
                    else if ("basic" == RunTestsType)
                    {
                        tcmd += " -basic";
                    }
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe clustercheck \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if (IsTestTest || "all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe CriticalSection";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe ClearLogs \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe DSpaceHosts \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe DSpaceFormatMetaOnlySwitch \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe DspaceGetFilePermission";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe DspaceGetbinaryFilePermission";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe LocalJobHost \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe LocalJobAddRefNonParticipatingCluster \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe AddRemoveMachineClearCache \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe PerfmonAdminCommandLock \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe PacketSniffAdminCommandLock \"" + dfsxmlpath + "\"";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "Regressiontest.exe KillallProxy";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe mrdebug";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe RemoveSurrogate ... incluster";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe RemoveSurrogate ... isolated";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe RemoveMachine ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe RemoveMachine2to1 ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe EnableReplication ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe EnableReplicationWithCache ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe CacheWithRedundancy ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe Replication ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe ReplicationFailover ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if (IsTestTest || "all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe MetaPath ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe ServiceCommands ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe CacheNoFailover ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe deploy ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers"; // Should this be a managers test?
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe killall ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers"; // Should this be a managers test?
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe kill ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe DfsUpdateStressTest ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("basic" == RunTestsType || "all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe rsorted ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe rhashsorted ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe MetaDataComplexity ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managerslargedata";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe MetaDataComplexityLargeSort ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe ReduceFinalizeThrow ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe ReduceInitializeThrow ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "managers";
                if ("all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe ReplicationChecks ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                if ("basic" == RunTestsType || "all" == RunTestsType || TestType == RunTestsType)
                {
                    string tcmd = "RegressionTest2.exe SortedCache ...";
                    Console.WriteLine(tcmd);
                    string tresult = Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            Console.WriteLine("    Tests completed; duration: {0}", (DateTime.Now - RunStartTime).ToString());

            Console.WriteLine();
            Console.WriteLine("TOTAL RESULTS:");
            Console.WriteLine("--STARTRESULTS--");
            Dictionary<string, bool> TestResultFound = new Dictionary<string, bool>();
            foreach (string tresult in AllTestResults)
            {
                foreach (string trline in tresult.Split('\n'))
                {
                    bool passed;
                    if (trline.StartsWith("[PASSED] - "))
                    {
                        passed = true;
                    }
                    else if (trline.StartsWith("[FAILED] - "))
                    {
                        passed = false;
                    }
                    else
                    {
                        continue;
                    }
                    string testname = trline.Substring(11).TrimEnd();
                    {
                        // Only print a test here once,
                        // sometimes the same one is printed twice in test results.
                        if (TestResultFound.ContainsKey(testname))
                        {
                            continue;
                        }
                        TestResultFound.Add(testname, true);
                    }
                    {
                        Console.Write('[');
                        if (passed)
                        {
                            if (isdspace)
                            {
                                Console.Write("\u00012PASSED\u00010");
                            }
                            else
                            {
                                ConsoleColor fg = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.Cyan;
                                Console.Write("PASSED");
                                Console.ForegroundColor = fg;
                            }
                        }
                        else
                        {
                            if (isdspace)
                            {
                                Console.Write("\u00014FAILED\u00010");
                            }
                            else
                            {
                                ConsoleColor fg = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.Write("FAILED");
                                Console.ForegroundColor = fg;
                            }
                        }
                        Console.WriteLine("] - {0}", testname);
                    }
                }
            }
            Console.WriteLine("--ENDRESULTS--");

            Console.WriteLine();
            /*
            DateTime DeleteStartTime = DateTime.Now;
            Console.WriteLine("[{0}]  Deleting regression tests...", DeleteStartTime.ToString());
            DeleteRegressionTests();
            Console.WriteLine("    Delete completed; duration: {0}", (DateTime.Now - DeleteStartTime).ToString());
             * */
            Console.WriteLine("Shortcut to delete regression tests from MR.DFS, type: {0} regressiontest -delete", appname);

            Console.WriteLine();
            Console.WriteLine("[{0}]  Done", DateTime.Now.ToString());

        }


        public static void DeleteRegressionTests()
        {
            foreach (string rtdirwc in RegressionTestsWildcards)
            {
                Shell("Qizmt delete \"" + rtdirwc + "\"", true);
            }
        }


    }

}