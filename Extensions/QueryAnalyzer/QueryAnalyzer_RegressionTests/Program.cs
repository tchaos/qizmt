using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static bool IsQaDebug = false;

        static void Main(string[] args)
        {
            string whichtest = null;
            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                if ("-attach" == args[iarg])
                {
                    System.Diagnostics.Debugger.Launch();
                }
                else if ("-qadebug" == args[iarg])
                {
                    IsQaDebug = true;
                }
                else
                {
                    whichtest = args[iarg];
                }
            }
            if (null != whichtest)
            {
                Console.WriteLine("Test for: {0}", whichtest);
            }

            if (!(true
                && 3 == CountOccurences("foofo", "o")
                && 2 == CountOccurences("foofo", "f")
                && 2 == CountOccurences("allwef\ngoataaa\nhello goat\nfoo", "goat")
                && 0 == CountOccurences("345673423\nn3nn3n3\nh3hh3h3\n\n", "hello world")
                ))
            {
                throw new Exception("CountOccurences is bork");
            }

            string TableName = "RegressionTest_QueryAnalyzer_" + Guid.NewGuid().ToString().Replace("-", "");

            List<KeyValuePair<string, bool>> AllTests = new List<KeyValuePair<string, bool>>();

            {
                string testname = "CREATE TABLE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    CreateTable(TableName, null != whichtest || 0 != string.Compare(whichtest, testname, true));
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    throw new Exception("Cannot continue tests if " + testname + " fails");
                }
                Console.WriteLine();
            }

            {
                string testname = "INSERT IMPORT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    InsertImport(TableName, null != whichtest || 0 != string.Compare(whichtest, testname, true));
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    throw new Exception("Cannot continue tests if " + testname + " fails");
                }
                Console.WriteLine();
            }

            {
                string testname = "INSERT IMPORTLINES";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    InsertImportLines(TableName, null != whichtest || 0 != string.Compare(whichtest, testname, true));
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    throw new Exception("Cannot continue tests if " + testname + " fails");
                }
                Console.WriteLine();
            }

            {
                string testname = "INSERT VALUES";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    InsertValues(TableName, null != whichtest || 0 != string.Compare(whichtest, testname, true));
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    throw new Exception("Cannot continue tests if " + testname + " fails");
                }
                Console.WriteLine();
            }

            {
                string testname = "SELECT";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        Select(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "SELECT (columns)";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        SelectColumns(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "Aggregate GROUP BY";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        AggregateGroupBy(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "GROUP BY ORDER BY";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        GroupByOrderBy(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "WHERE NOT <expression>";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        WhereNOTexpr(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "WHERE operators";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        WhereOperators(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "WHERE functions";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        WhereFunctions(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "WHERE IS [NOT] NULL";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        WhereISXNULL(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "UNION ALL (sub select)";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        UnionAllSubSelect(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "UPDATE TABLE";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        UpdateTable(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "DELETE FROM";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        DeleteFrom(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "SELECT SYS TABLE";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        SelectSysTable(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "BATCHEXECUTE";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        BatchExecute(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "SHELL";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        ShellTest(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            {
                string testname = "INSERT INTO...SELECT";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        InsertIntoXSelect(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            // Note: TRUNCATE TABLE must come directly before DROP TABLE!
            {
                string testname = "TRUNCATE TABLE";
                if (null == whichtest || 0 == string.Compare(whichtest, testname, true))
                {
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        TruncateTable(TableName);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
            }

            // Note: DROP TABLE must come directly last!
            {
                string testname = "DROP TABLE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DropTable(TableName, null != whichtest || 0 != string.Compare(whichtest, testname, true));
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }


            // - Display final output -
            Console.WriteLine("--STARTRESULTS--");
            foreach (KeyValuePair<string, bool> test in AllTests)
            {
                DSpace_LogResult(test.Key, test.Value);
            }
            Console.WriteLine("--ENDRESULTS--");

#if DEBUG
            //Console.ReadKey();
#endif

        }


        public static string QaExec(string query)
        {
            //return Exec.Shell("dspace " + (IsQaDebug ? "-debug " : "") + "exec RDBMS_QueryAnalyzer.DBCORE \"" + RDBMS_DBCORE.Qa.QlArgsEscape(query) + "\"");
            return (new RDBMS_DBCORE.Qa.QueryAnalyzer()).Exec(RDBMS_DBCORE.Qa.QlArgsEscape(query));
        }


        public static void DSpace_LogResult(string name, bool passed)
        {
            if (passed)
            {
                Console.Write("[");
                ConsoleColor fc = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.Write("PASSED");
                Console.ForegroundColor = fc;
                Console.WriteLine("] - " + name);
            }
            else
            {
                Console.Write("[");
                ConsoleColor fc = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write("FAILED");
                Console.ForegroundColor = fc;
                Console.WriteLine("] - " + name);
            }
        }


        public static int CountOccurences(string input, string what)
        {
            int count = 0;
            int i = 0;
            for (; ; )
            {
                i = input.IndexOf(what, i);
                if (-1 == i)
                {
                    break;
                }
                i += what.Length;
                count++;
            }
            return count;
        }


    }
}
