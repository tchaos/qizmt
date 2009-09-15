using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void DropTable(string TableName, bool nocheck)
        {

            {
                Console.WriteLine("Ensure table exists...");
                QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 7278821 ORDER BY x, num;"); // Note: no rows due to truncate.
            }

            {
                Console.WriteLine("Dropping table...");
                Console.WriteLine(QaExec("DROP TABLE " + TableName).Trim());
            }

            if (nocheck)
            {
                return;
            }

            {
                Console.WriteLine("Ensure table does NOT exist...");
                bool ok = false;
                try
                {
                    string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 7278821 ORDER BY x, num;");
                    Console.Error.WriteLine("Did not catch exception; table may still exist after DROP: {0}", outputSelect1.Trim());
                }
                catch
                {
                    ok = true;
                }
                if (!ok)
                {
                    throw new Exception("Did not catch exception; table may still exist after DROP");
                }
            }

        }

    }
}
