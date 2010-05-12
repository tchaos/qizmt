using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_DBCORE_RegressionTests
{
    public partial class Program
    {
        static void Main(string[] args)
        {
            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                if ("-attach" == args[iarg])
                {
                    System.Diagnostics.Debugger.Launch();
                }
                else if ("-qadebug" == args[iarg])
                {
                    //IsQaDebug = true;
                }
                else
                {
                    //whichtest = args[iarg];
                }
            }

            List<KeyValuePair<string, bool>> AllTests = new List<KeyValuePair<string, bool>>();

            {
                string testname = "DbFunctions_ARITHMETIC";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ARITHMETIC();
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

            {
                string testname = "DbFunctions_IIF";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_IIF();
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

            {
                string testname = "DbFunctions_DATEPART";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_DATEPART();
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

            {
                string testname = "DbFunctions_CAST";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_CAST();
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
            {
                string testname = "DbFunctions_FORMAT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_FORMAT();
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
            {
                string testname = "DbFunctions_DATEADD";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_DATEADD();
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

            {
                string testname = "DbFunctions_DATEDIFF";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_DATEDIFF();
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

            {
                string testname = "DbFunctions_NULLIF";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_NULLIF();
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

            {
                string testname = "DbFunctions_NVL2";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_NVL2();
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

            {
                string testname = "DbFunctions_NVL";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_NVL();
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

            {
                string testname = "DbFunctions_SYSDATE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_SYSDATE();
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

            {
                string testname = "DbFunctions_NEXT_DAY";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_NEXT_DAY();
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

            {
                string testname = "DbFunctions_MONTHS_BETWEEN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_MONTHS_BETWEEN();
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

            {
                string testname = "DbFunctions_LAST_DAY";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LAST_DAY();
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

            {
                string testname = "DbFunctions_ADD_MONTHS";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ADD_MONTHS();
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

            {
                string testname = "DbFunctions_REPLACE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_REPLACE();
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

            {
                string testname = "DbFunctions_RPAD";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_RPAD();
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

            {
                string testname = "DbFunctions_LPAD";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LPAD();
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

            {
                string testname = "DbFunctions_INSTR";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_INSTR();
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

            {
                string testname = "DbFunctions_CONCAT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_CONCAT();
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

            {
                string testname = "DbFunctions_MOD";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_MOD();
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

            {
                string testname = "DbFunctions_COMPARE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_COMPARE();
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

            {
                string testname = "DbFunctions_ISNULL";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ISNULL();
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

            {
                string testname = "DbFunctions_ISNOTNULL";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ISNOTNULL();
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

            {
                string testname = "DbFunctions_ABS";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ABS();
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

            {
                string testname = "DbFunctions_ROUND";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ROUND();
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

            {
                string testname = "DbFunctions_LEFT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LEFT();
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

            {
                string testname = "DbFunctions_ACOS";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ACOS();
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

            {
                string testname = "DbFunctions_ASIN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ASIN();
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

            {
                string testname = "DbFunctions_ATAN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ATAN();
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

            {
                string testname = "DbFunctions_ATN2";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ATN2();
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

            {
                string testname = "DbFunctions_CEILING";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_CEILING();
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

            {
                string testname = "DbFunctions_COS";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_COS();
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

            {
                string testname = "DbFunctions_COT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_COT();
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

            {
                string testname = "DbFunctions_DEGREES";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_DEGREES();
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

            {
                string testname = "DbFunctions_ISLIKE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_ISLIKE();
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

            {
                string testname = "DbFunctions_EXP";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_EXP();
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

            {
                string testname = "DbFunctions_GREATER";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_GREATER();
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

            {
                string testname = "DbFunctions_LESSER";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LESSER();
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

            {
                string testname = "DbFunctions_EQUAL";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_EQUAL();
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

            {
                string testname = "DbFunctions_GREATEREQUAL";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_GREATEREQUAL();
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

            {
                string testname = "DbFunctions_LESSEREQUAL";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LESSEREQUAL();
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

            {
                string testname = "DbFunctions_NOTEQUAL";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_NOTEQUAL();
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

            {
                string testname = "DbFunctions_FLOOR";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_FLOOR();
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

            {
                string testname = "DbFunctions_LOG";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LOG();
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

            {
                string testname = "DbFunctions_LOG10";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LOG10();
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

            {
                string testname = "DbFunctions_PI";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_PI();
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

            {
                string testname = "DbFunctions_POWER";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_POWER();
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

            {
                string testname = "DbFunctions_RADIANS";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_RADIANS();
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

            {
                string testname = "DbFunctions_RAND";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_RAND();
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

            {
                string testname = "DbFunctions_SIGN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_SIGN();
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

            {
                string testname = "DbFunctions_SIN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_SIN();
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

            {
                string testname = "DbFunctions_SQUARE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_SQUARE();
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

            {
                string testname = "DbFunctions_SQRT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_SQRT();
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
            {
                string testname = "DbFunctions_TAN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_TAN();
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

            {
                string testname = "DbFunctions_RIGHT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_RIGHT();
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

            {
                string testname = "DbFunctions_CHARINDEX";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_CHARINDEX();
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
            {
                string testname = "DbFunctions_LEN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LEN();
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
            {
                string testname = "DbFunctions_LOWER";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LOWER();
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
            {
                string testname = "DbFunctions_UPPER";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_UPPER();
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
            {
                string testname = "DbFunctions_SUBSTRING";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_SUBSTRING();
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
            {
                string testname = "DbFunctions_RTRIM";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_RTRIM();
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
            {
                string testname = "DbFunctions_LTRIM";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_LTRIM();
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
            {
                string testname = "DbFunctions_SPACE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_SPACE();
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
            {
                string testname = "DbFunctions_REVERSE";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_REVERSE();
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
            {
                string testname = "DbFunctions_PATINDEX";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbFunctions_PATINDEX();
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
            {
                string testname = "DbAggregators_MIN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_MIN();
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
            {
                string testname = "DbAggregators_AVG";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_AVG();
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
            {
                string testname = "DbAggregators_STD";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_STD();
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
            {
                string testname = "DbAggregators_STD_SAMP";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_STD_SAMP();
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
            {
                string testname = "DbAggregators_VAR_POP";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_VAR_POP();
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
            {
                string testname = "DbAggregators_VAR_SAMP";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_VAR_SAMP();
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
            {
                string testname = "DbAggregators_COUNT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_COUNT();
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
            {
                string testname = "DbAggregators_FIRST";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_FIRST();
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
            {
                string testname = "DbAggregators_LAST";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_LAST();
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
            {
                string testname = "DbAggregators_MAX";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_MAX();
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
            {
                string testname = "DbAggregators_SUM";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_SUM();
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
            {
                string testname = "DbAggregators_CHOOSERND";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_CHOOSERND();
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
            {
                string testname = "DbAggregators_BIT_AND";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_BIT_AND();
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
            {
                string testname = "DbAggregators_BIT_OR";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_BIT_OR();
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
            {
                string testname = "DbAggregators_BIT_XOR";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_BIT_XOR();
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
            {
                string testname = "DbAggregators_COUNTDISTINCT";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    DbAggregators_COUNTDISTINCT();
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
            //Console.Read();
#endif
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
    }
}
