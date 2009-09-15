using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void Select(string TableName)
        {

            {
                Console.WriteLine("Selecting negative integer...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE num = -9999 ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value");
                }
            }

            {
                Console.WriteLine("Selecting without WHERE...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find expected value");
                }
            }

            {
                Console.WriteLine("Selecting without TOP...");
                string outputSelect1 = QaExec("SELECT * FROM " + TableName + " ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find expected value");
                }
            }

            {
                Console.WriteLine("Selecting and expecting negative integer at the top...");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 42 ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value at the top, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value at the top");
                }
            }

            {
                Console.WriteLine("Selecting and expecting negative long integer at the top...");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 42 ORDER BY y;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value at the top, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value at the top");
                }
            }

            {
                Console.WriteLine("Selecting and expecting negative double at the top...");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 42 ORDER BY z;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value at the top, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value at the top");
                }
            }

            {
                Console.WriteLine("Selecting with positive integer with sign specified (+)...");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = +42 ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value at the top, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value at the top");
                }
            }

            {
                Console.WriteLine("Selecting where string is 'hello world'...");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE str = 'hello world' ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("hello world"))
                {
                    Console.Error.WriteLine("Did not find negative value at the top, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value at the top");
                }
            }

            {
                Console.WriteLine("Selecting where string is '' (empty)...");
                QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE str = '' ORDER BY num;");
            }

            {
                Console.WriteLine("Selecting where string is '''' (one single quote)...");
                QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE str = '''' ORDER BY num;");
            }

            {
                Console.WriteLine("Selecting with 2 conditions (WHERE x AND y)");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 7278821 AND num = 53 ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("{1D42BCBD-441E-4e84-A51E-F32AA1B4F8B5}"))
                {
                    Console.Error.WriteLine("Did not find value; select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find value");
                }
            }

            {
                Console.WriteLine("Selecting with ORDER BY two columns");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE x = 7278821 ORDER BY x, num;");
                if (-1 == outputSelect1.IndexOf("{514068AA-DFCA-4891-AE93-38A76B83B53D}"))
                {
                    Console.Error.WriteLine("Did not find value; select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find value");
                }
            }

            {
                Console.WriteLine("Selecting various formatting...");
                QaExec("SELECT TOP 3 * FROM " + TableName + " WHERE x=42 ORDER BY num;");
                QaExec("SELECT TOP 3 * FROM " + TableName + " WHERE x= -42 ORDER BY num;");
                QaExec("SELECT TOP 2 * FROM " + TableName + " WHERE str='hello world' ORDER BY num;");
                QaExec("  Select  TOP    2  *  FROM    " + TableName + "  WHERE str   ='hello world'  order  by  Num;   ");
                QaExec("SELECT TOP 3 * FROM " + TableName + " WHERE x= -42 AND x=-42 ORDER BY num;");
                QaExec("SELECT TOP 3 * FROM " + TableName + " WHERE x= +42 AND x=+42 ORDER BY num , x;");
            }

        }

    }
}
