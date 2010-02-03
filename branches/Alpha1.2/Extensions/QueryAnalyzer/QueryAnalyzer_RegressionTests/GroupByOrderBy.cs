using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void GroupByOrderBy(string TableName)
        {

            Console.WriteLine("Inserting values...");
            QaExec("INSERT INTO " + TableName + " VALUES(11999275,'aa{BC54AFDD-34AE-445b-820A-BFCBC716FF77}',399111,-11223344,3991);");
            QaExec("INSERT INTO " + TableName + " VALUES(11999276,'BB{745B9042-77B2-4633-B765-59A1B0DA7194}',399111, 0       ,3991);");
            QaExec("INSERT INTO " + TableName + " VALUES(11999277,'cc{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}',399111, 11223344,3991);");

            {
                Console.WriteLine("MIN(num) GROUP BY ORDER BY num");
                string outputSelect1 = QaExec("SELECT MIN(num) FROM " + TableName + " WHERE x = 399111 GROUP BY x ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("11999275"))
                {
                    Console.Error.WriteLine("Expected MIN(num), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected MIN(num)");
                }
                if (-1 != outputSelect1.IndexOf("aa{BC54AFDD-34AE-445b-820A-BFCBC716FF77}")
                    || -1 != outputSelect1.IndexOf("BB{745B9042-77B2-4633-B765-59A1B0DA7194}")
                    || -1 != outputSelect1.IndexOf("cc{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}")
                    || -1 != outputSelect1.IndexOf("11999276")
                    || -1 != outputSelect1.IndexOf("11999277")
                    || -1 != outputSelect1.IndexOf("399111")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }

            {
                Console.WriteLine("MAX(str) GROUP BY ORDER BY num");
                string outputSelect1 = QaExec("SELECT MAX(str) FROM " + TableName + " WHERE x = 399111 GROUP BY x ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("cc{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}"))
                {
                    Console.Error.WriteLine("Expected MAX(str), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected MAX(str)");
                }
                if (-1 != outputSelect1.IndexOf("aa{BC54AFDD-34AE-445b-820A-BFCBC716FF77}")
                    || -1 != outputSelect1.IndexOf("BB{745B9042-77B2-4633-B765-59A1B0DA7194}")
                    || -1 != outputSelect1.IndexOf("11999275")
                    || -1 != outputSelect1.IndexOf("11999276")
                    || -1 != outputSelect1.IndexOf("11999277")
                    || -1 != outputSelect1.IndexOf("399111")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }

            {
                Console.WriteLine("x,MIN(num),MAX(str),x GROUP BY ORDER BY num");
                string outputSelect1 = QaExec("SELECT x,MIN(num),MAX(str),x FROM " + TableName + " WHERE x = 399111 GROUP BY x ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("cc{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}")
                    || -1 == outputSelect1.IndexOf("11999275"))
                {
                    Console.Error.WriteLine("Expected x,MIN(num),MAX(str),x, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected x,MIN(num),MAX(str),x");
                }
                if (-1 != outputSelect1.IndexOf("aa{BC54AFDD-34AE-445b-820A-BFCBC716FF77}")
                    || -1 != outputSelect1.IndexOf("BB{745B9042-77B2-4633-B765-59A1B0DA7194}")
                    || -1 != outputSelect1.IndexOf("11999276")
                    || -1 != outputSelect1.IndexOf("11999277")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }

            {
                Console.WriteLine(" x ,  Min  (  num  )  ,  Max  (  str  )  ,  x   GROUP BY ORDER BY num");
                string outputSelect1 = QaExec("SELECT   x ,  Min  (  num  )  ,  Max  (  str  )  ,  x    FROM " + TableName + " WHERE x = 399111 GROUP BY x ORDER BY num;");
                if (-1 == outputSelect1.IndexOf("cc{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}")
                    || -1 == outputSelect1.IndexOf("11999275"))
                {
                    Console.Error.WriteLine("Expected x ,  Min  (  num  )  ,  Max  (  str  )  ,  x , select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected x ,  Min  (  num  )  ,  Max  (  str  )  ,  x ");
                }
                if (-1 != outputSelect1.IndexOf("aa{BC54AFDD-34AE-445b-820A-BFCBC716FF77}")
                    || -1 != outputSelect1.IndexOf("BB{745B9042-77B2-4633-B765-59A1B0DA7194}")
                    || -1 != outputSelect1.IndexOf("11999276")
                    || -1 != outputSelect1.IndexOf("11999277")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }


        }

    }
}
