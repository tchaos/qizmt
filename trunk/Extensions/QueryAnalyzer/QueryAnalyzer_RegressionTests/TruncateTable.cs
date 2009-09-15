using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void TruncateTable(string TableName)
        {

            {
                Console.WriteLine("Ensure table exists...");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 7278821 ORDER BY x, num;");
                if (-1 == outputSelect1.IndexOf("{514068AA-DFCA-4891-AE93-38A76B83B53D}"))
                {
                    Console.Error.WriteLine("Did not find value; select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find value");
                }
            }

            {
                Console.WriteLine("Truncating table...");
                Console.WriteLine(QaExec("TRUNCATE TABLE " + TableName).Trim());
            }

            {
                Console.WriteLine("Ensure table exists and is empty...");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 7278821 ORDER BY x, num;");
                if (-1 != outputSelect1.IndexOf("{514068AA-DFCA-4891-AE93-38A76B83B53D}"))
                {
                    Console.Error.WriteLine("Table is not empty; select output: {0}", outputSelect1.Trim());
                    throw new Exception("Table is not empty");
                }
            }

        }

    }
}
