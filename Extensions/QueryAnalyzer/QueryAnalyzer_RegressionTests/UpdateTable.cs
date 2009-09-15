using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void UpdateTable(string TableName)
        {

            Console.WriteLine("Inserting dummy row...");
            QaExec("INSERT INTO " + TableName + " VALUES(131399,'{A0E2B5C6-773F-4c5e-88D7-FC38180586A4}',131399,131399,131399);");

            {
                Console.WriteLine("Inserting value (to update next)...");

                QaExec("INSERT INTO " + TableName + " VALUES(131399,'{18DF74F8-5808-4034-89B3-583EFCBAD945}',131399,131399,131399);");

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{18DF74F8-5808-4034-89B3-583EFCBAD945}' ORDER BY x;");
                if (1 != CountOccurences(outputSelect1, "{18DF74F8-5808-4034-89B3-583EFCBAD945}"))
                {
                    Console.Error.WriteLine("Expected to find inserted value once, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find inserted value once");
                }

            }

            {
                Console.WriteLine("Updating the value...");

                QaExec("UPDATE " + TableName + " SET STR='{44A915C7-1CAF-4674-9EE9-66849E721092}' WHERE STR='{18DF74F8-5808-4034-89B3-583EFCBAD945}';");

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE NUM = 131399 ORDER BY STR;");
                if (1 != CountOccurences(outputSelect1, "{44A915C7-1CAF-4674-9EE9-66849E721092}"))
                {
                    Console.Error.WriteLine("Expected to find updated value once, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find updated value once");
                }
                if (0 != CountOccurences(outputSelect1, "{18DF74F8-5808-4034-89B3-583EFCBAD945}"))
                {
                    Console.Error.WriteLine("Did not expect to find originally inserted value (it should have been updated), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not expect to find originally inserted value (it should have been updated)");
                }

            }

            {
                Console.WriteLine("Updating other fields...");

                QaExec("UPDATE " + TableName + " SET num=131399999,x=131399991 WHERE NUM >= 131399 AND NUM < 131400;");

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE NUM = 131399999 ORDER BY STR;");
                if (-1 == outputSelect1.IndexOf("131399999"))
                {
                    Console.Error.WriteLine("Expected to find updated other field, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find updated other field");
                }

            }

            {
                Console.WriteLine("Ensuring previously inserted dummy row exists...");

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{A0E2B5C6-773F-4c5e-88D7-FC38180586A4}' ORDER BY x;");
                if (1 != CountOccurences(outputSelect1, "{A0E2B5C6-773F-4c5e-88D7-FC38180586A4}"))
                {
                    Console.Error.WriteLine("Expected to find previously inserted dummy value once, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find previously inserted dummy value once");
                }
            }

        }

    }
}
