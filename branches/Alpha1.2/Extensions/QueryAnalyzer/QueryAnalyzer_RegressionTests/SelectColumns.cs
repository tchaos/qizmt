using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void SelectColumns(string TableName)
        {

            {
                Console.WriteLine("Selecting Str with negative integer...");
                string outputSelect1 = QaExec("SELECT TOP 999 Str FROM " + TableName + " WHERE num = -9999 ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value");
                }
            }

            {
                Console.WriteLine("Selecting x,y,str,z and expecting negative long integer at the top...");
                string outputSelect1 = QaExec("SELECT TOP 1 x,y,str,z FROM " + TableName + " WHERE x = 42 ORDER BY y;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value at the top, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value at the top");
                }
            }

            {
                Console.WriteLine("Selecting Num,St where string is 'hello world'...");
                string outputSelect1 = QaExec("SELECT TOP 1 Num,Str FROM " + TableName + " WHERE str = 'hello world' ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("hello world"))
                {
                    Console.Error.WriteLine("Did not find hello world, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find hello world");
                }
            }

            {
                Console.WriteLine("Selecting Str with 2 conditions (WHERE x AND y)");
                string outputSelect1 = QaExec("SELECT TOP 1 Str FROM " + TableName + " WHERE x = 7278821 AND num = 53 ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("{1D42BCBD-441E-4e84-A51E-F32AA1B4F8B5}"))
                {
                    Console.Error.WriteLine("Did not find value; select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find value");
                }
            }

            {
                Console.WriteLine("Selecting Str,Str and expecting one string found twice");
                string outputSelect1 = QaExec("SELECT TOP 1 Str,Str FROM " + TableName + " WHERE x = 7278821 AND num = 53 ORDER BY num;");
                if (2 != CountOccurences(outputSelect1, "{1D42BCBD-441E-4e84-A51E-F32AA1B4F8B5}"))
                {
                    Console.Error.WriteLine("Did not find value twice; select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find value twice");
                }
            }

        }

    }
}
