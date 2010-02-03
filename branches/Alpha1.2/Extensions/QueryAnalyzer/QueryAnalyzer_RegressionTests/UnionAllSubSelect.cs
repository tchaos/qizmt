using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void UnionAllSubSelect(string TableName)
        {

            {
                Console.WriteLine("Inserting unique value...");

                QaExec("INSERT INTO " + TableName + " VALUES(3564588,'{2805CEDF-F0DA-4614-9F76-5FB1280B4283}',3564588,3564588,3564588);");

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{2805CEDF-F0DA-4614-9F76-5FB1280B4283}' ORDER BY x;");
                if (1 != CountOccurences(outputSelect1, "{2805CEDF-F0DA-4614-9F76-5FB1280B4283}"))
                {
                    Console.Error.WriteLine("Expected to find inserted value once, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find inserted value once");
                }

            }

            {
                Console.WriteLine("UNION ALL sub selecting from table twice, ensuring exactly 2 copies of value...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM (select * from " + TableName + " Union All select * from " + TableName + ") WHERE str = '{2805CEDF-F0DA-4614-9F76-5FB1280B4283}' ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{2805CEDF-F0DA-4614-9F76-5FB1280B4283}"))
                {
                    Console.Error.WriteLine("Expected to find inserted value twice, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find inserted value twice");
                }
            }

        }

    }
}
