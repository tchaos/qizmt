using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void InsertIntoXSelect(string TableName)
        {

            Console.WriteLine("Insert guaranteed unique GUID...");
            QaExec("INSERT INTO " + TableName + " VALUES(33436178,'{8A50F469-3CF1-47ac-BBFB-145D3A27B4FE}',33436178,33436178,33436178);");

            {
                Console.WriteLine("Ensure GUID found only once...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{8A50F469-3CF1-47ac-BBFB-145D3A27B4FE}' ORDER BY num;");
                if (1 != CountOccurences(outputSelect1, "{8A50F469-3CF1-47ac-BBFB-145D3A27B4FE}"))
                {
                    Console.Error.WriteLine("GUID count error; select output: {0}", outputSelect1.Trim());
                    throw new Exception("GUID count error");
                }
            }

            {
                Console.WriteLine("INSERT INTO ThisTable SELECT * FROM ThisTable; ...");
                QaExec("INSERT INTO " + TableName + " SELECT * FROM " + TableName + " ORDER BY num;");
            }

            {
                Console.WriteLine("Ensure GUID found exactly twice now that table was duplicated...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{8A50F469-3CF1-47ac-BBFB-145D3A27B4FE}' ORDER BY num;");
                if (2 != CountOccurences(outputSelect1, "{8A50F469-3CF1-47ac-BBFB-145D3A27B4FE}"))
                {
                    Console.Error.WriteLine("GUID count error; select output: {0}", outputSelect1.Trim());
                    throw new Exception("GUID count error");
                }
            }

        }

    }
}
