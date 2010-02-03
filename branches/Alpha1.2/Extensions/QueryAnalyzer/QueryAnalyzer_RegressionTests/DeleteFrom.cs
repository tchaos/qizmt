using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void DeleteFrom(string TableName)
        {

            Console.WriteLine("Inserting dummy row...");
            QaExec("INSERT INTO " + TableName + " VALUES(25627,'{201D873D-EBE5-42dc-BE2D-79675B66C11C}',25627,25627,25627);");

            {
                Console.WriteLine("Inserting value (to delete next)...");

                QaExec("INSERT INTO " + TableName + " VALUES(522269,'{122D32F8-06A1-44cf-87E0-DD439B1CFF41}',522269,522269,522269);");

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{122D32F8-06A1-44cf-87E0-DD439B1CFF41}' ORDER BY x;");
                if (1 != CountOccurences(outputSelect1, "{122D32F8-06A1-44cf-87E0-DD439B1CFF41}"))
                {
                    Console.Error.WriteLine("Expected to find inserted value once, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find inserted value once");
                }

            }

            {
                Console.WriteLine("Deleting the value...");

                QaExec("DELETE FROM " + TableName + " WHERE STR='{122D32F8-06A1-44cf-87E0-DD439B1CFF41}';");

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{122D32F8-06A1-44cf-87E0-DD439B1CFF41}'");
                if (0 != CountOccurences(outputSelect1, "{122D32F8-06A1-44cf-87E0-DD439B1CFF41}"))
                {
                    Console.Error.WriteLine("Expected not to find deleted value, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected not to find deleted value");
                }

            }

            {
                Console.WriteLine("Ensuring previously inserted dummy row exists...");

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{201D873D-EBE5-42dc-BE2D-79675B66C11C}' ORDER BY x;");
                if (1 != CountOccurences(outputSelect1, "{201D873D-EBE5-42dc-BE2D-79675B66C11C}"))
                {
                    Console.Error.WriteLine("Expected to find previously inserted dummy value once, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find previously inserted dummy value once");
                }
            }

        }

    }
}
