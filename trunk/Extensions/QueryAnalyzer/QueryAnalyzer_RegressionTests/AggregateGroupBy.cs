using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void AggregateGroupBy(string TableName)
        {

            Console.WriteLine("Inserting values...");
            QaExec("INSERT INTO " + TableName + " VALUES(11388275,'a{BC54AFDD-34AE-445b-820A-BFCBC716FF77}',312111,-11223344,3111);");
            QaExec("INSERT INTO " + TableName + " VALUES(11388276,'B{745B9042-77B2-4633-B765-59A1B0DA7194}',312111, 0       ,3111);");
            QaExec("INSERT INTO " + TableName + " VALUES(11388277,'c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}',312111, 11223344,3111);");

            {
                Console.WriteLine("MIN(num) GROUP BY");
                string outputSelect1 = QaExec("SELECT MIN(num) FROM " + TableName + " WHERE x = 312111 GROUP BY x;");
                if (-1 == outputSelect1.IndexOf("11388275"))
                {
                    Console.Error.WriteLine("Expected MIN(num), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected MIN(num)");
                }
                if (-1 != outputSelect1.IndexOf("a{BC54AFDD-34AE-445b-820A-BFCBC716FF77}")
                    || -1 != outputSelect1.IndexOf("B{745B9042-77B2-4633-B765-59A1B0DA7194}")
                    || -1 != outputSelect1.IndexOf("c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}")
                    || -1 != outputSelect1.IndexOf("11388276")
                    || -1 != outputSelect1.IndexOf("11388277")
                    || -1 != outputSelect1.IndexOf("312111")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }

            {
                Console.WriteLine("MAX(str) GROUP BY");
                string outputSelect1 = QaExec("SELECT MAX(str) FROM " + TableName + " WHERE x = 312111 GROUP BY x;");
                if (-1 == outputSelect1.IndexOf("c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}"))
                {
                    Console.Error.WriteLine("Expected MAX(str), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected MAX(str)");
                }
                if (-1 != outputSelect1.IndexOf("a{BC54AFDD-34AE-445b-820A-BFCBC716FF77}")
                    || -1 != outputSelect1.IndexOf("B{745B9042-77B2-4633-B765-59A1B0DA7194}")
                    || -1 != outputSelect1.IndexOf("11388275")
                    || -1 != outputSelect1.IndexOf("11388276")
                    || -1 != outputSelect1.IndexOf("11388277")
                    || -1 != outputSelect1.IndexOf("312111")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }

            {
                Console.WriteLine("x,MIN(num),MAX(str),x GROUP BY");
                string outputSelect1 = QaExec("SELECT x,MIN(num),MAX(str),x FROM " + TableName + " WHERE x = 312111 GROUP BY x;");
                if (-1 == outputSelect1.IndexOf("c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}")
                    || -1 == outputSelect1.IndexOf("11388275"))
                {
                    Console.Error.WriteLine("Expected x,MIN(num),MAX(str),x, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected x,MIN(num),MAX(str),x");
                }
                if (-1 != outputSelect1.IndexOf("a{BC54AFDD-34AE-445b-820A-BFCBC716FF77}")
                    || -1 != outputSelect1.IndexOf("B{745B9042-77B2-4633-B765-59A1B0DA7194}")
                    || -1 != outputSelect1.IndexOf("11388276")
                    || -1 != outputSelect1.IndexOf("11388277")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }

            {
                Console.WriteLine(" x ,  Min  (  num  )  ,  Max  (  str  )  ,  x   GROUP BY");
                string outputSelect1 = QaExec("SELECT   x ,  Min  (  num  )  ,  Max  (  str  )  ,  x    FROM " + TableName + " WHERE x = 312111 GROUP BY x;");
                if (-1 == outputSelect1.IndexOf("c{512441F2-0AFE-42d3-A4BA-9DD26A9E553A}")
                    || -1 == outputSelect1.IndexOf("11388275"))
                {
                    Console.Error.WriteLine("Expected x ,  Min  (  num  )  ,  Max  (  str  )  ,  x , select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected x ,  Min  (  num  )  ,  Max  (  str  )  ,  x ");
                }
                if (-1 != outputSelect1.IndexOf("a{BC54AFDD-34AE-445b-820A-BFCBC716FF77}")
                    || -1 != outputSelect1.IndexOf("B{745B9042-77B2-4633-B765-59A1B0DA7194}")
                    || -1 != outputSelect1.IndexOf("11388276")
                    || -1 != outputSelect1.IndexOf("11388277")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }


        }

    }
}
