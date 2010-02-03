using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    public partial class Program
    {

        public static void RunRegressionTests(string[] args)
        {

            DateTime RunStartTime = DateTime.Now;
            Console.WriteLine("[{0}]  Running tests...", RunStartTime.ToString());

            List<string> AllTestResults = new List<string>();

            {
                string TestType = "normal";
                //////// TestType
                {
                    string tcmd = "QueryAnalyzer_ADONET_RegressionTests.exe";
                    Console.WriteLine(tcmd);
                    string tresult = Exec.Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                //////// TestType
                {
                    string tcmd = "RDBMS_DBCORE_RegressionTests.exe";
                    Console.WriteLine(tcmd);
                    string tresult = Exec.Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            {
                string TestType = "normal";
                //////// TestType
                {
                    string tcmd = "QueryAnalyzer_RegressionTests.exe";
                    Console.WriteLine(tcmd);
                    string tresult = Exec.Shell(tcmd, true);
                    Console.WriteLine(tresult);
                    AllTestResults.Add(tresult);
                }
            }

            Console.WriteLine("    Tests completed; duration: {0}", (DateTime.Now - RunStartTime).ToString());

            const bool isdspace = false;
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
            Console.WriteLine("[{0}]  Done", DateTime.Now.ToString());


        }

    }
}