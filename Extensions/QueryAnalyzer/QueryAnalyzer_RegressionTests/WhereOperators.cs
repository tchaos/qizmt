using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void WhereOperators(string TableName)
        {

            {
                Console.WriteLine("<=");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE ABS(num)<=9999 ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find expected value <=, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find expected value <=");
                }
            }

            {
                Console.WriteLine(">=");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE ABS(num)>=9999 ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find expected value >=, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find expected value >=");
                }
            }

            {
                Console.WriteLine("<>");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE ABS(num)<>9999");
                if (-1 != outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Found unexpected value <>, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Found unexpected value <>");
                }
            }

            {
                Console.WriteLine("LIKE");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str LIKE '%world'");
                if (-1 == outputSelect1.IndexOf("hello world"))
                {
                    Console.Error.WriteLine("Did not find expected value LIKE, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find expected value LIKE");
                }
            }

        }

    }
}
