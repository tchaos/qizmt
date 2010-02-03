using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void SelectSysTable(string TableName)
        {

            {
                Console.WriteLine("Selecting from Sys.Tables...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM sys.tables WHERE table = '" + TableName + "' ORDER BY table;");
                if (-1 == outputSelect1.IndexOf(TableName))
                {
                    Console.Error.WriteLine("Did not find current table (" + TableName + "), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find current table (" + TableName + ")");
                }
            }

        }

    }
}
