using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void WhereFunctions(string TableName)
        {

            {
                Console.WriteLine("WHERE ABS(negative_integer)...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE ABS(num)=9999 ORDER BY x;");
                if (-1 == outputSelect1.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value (1), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value (1)");
                }
                string outputSelect2 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE ABS(num) = 9999 ORDER BY x;");
                if (-1 == outputSelect2.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value (2), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value (2)");
                }
                string outputSelect4 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE  ABS  (  num  )  =  9999  ORDER BY x;");
                if (-1 == outputSelect4.IndexOf("{48C7D626-9790-40eb-A8CA-073F514ED7D6}"))
                {
                    Console.Error.WriteLine("Did not find negative value (4), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find negative value (4)");
                }
            }

            {
                Console.WriteLine("WHERE LEFT('hello world',1)...");
                string outputSelect1 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE LEFT(str,1)='h' ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("hello world"))
                {
                    Console.Error.WriteLine("Did not find string left (1), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find string left (1)");
                }
                string outputSelect2 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE LEFT(str,1) = 'h' ORDER BY num;");
                if (-1 == outputSelect2.IndexOf("hello world"))
                {
                    Console.Error.WriteLine("Did not find string left (2), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find string left (2)");
                }
                string outputSelect4 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE  LEFT  (  str  ,  1  )  =  'h'  ORDER BY num;");
                if (-1 == outputSelect4.IndexOf("hello world"))
                {
                    Console.Error.WriteLine("Did not find string left (4), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find string left (4)");
                }
                string outputSelect5 = QaExec("SELECT TOP 1 * FROM " + TableName + " WHERE  LEFT  (  str  ,  1  )  =  'h'");
            }

            

        }

    }
}
