using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void GroupByFunctions(string TableName)
        {

            Console.WriteLine("Inserting values...");
            QaExec("INSERT INTO " + TableName + " VALUES(34123314,'a{9C7F771C-8600-4c44-AA15-934FF8106A5C}',228925,-11223344,5511);");
            QaExec("INSERT INTO " + TableName + " VALUES(34123315,'B{20BF6E94-116D-470b-84E7-3E4EB907C660}',228925, 0       ,5511);");
            QaExec("INSERT INTO " + TableName + " VALUES(34123316,'c{E71C13DF-8823-4001-962F-FEB311E79342}',228925, 11223344,5511);");

            {
                Console.WriteLine("GROUP BY ABS(x)");
                string outputSelect1 = QaExec("SELECT MAX(num) FROM " + TableName + " WHERE x = 228925 GROUP BY ABS(ABS(Y)),ABS(Y);");
                if (-1 == outputSelect1.IndexOf("34123315") || -1 == outputSelect1.IndexOf("34123316"))
                {
                    Console.Error.WriteLine("Invalid GROUP BY ABS(x), select output: {0}", outputSelect1.Trim());
                    throw new Exception("Invalid GROUP BY ABS(x)");
                }
                if (-1 != outputSelect1.IndexOf("34123314")
                    )
                {
                    Console.Error.WriteLine("Unexpected values, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Unexpected values");
                }
            }


        }

    }
}
