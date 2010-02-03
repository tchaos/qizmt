using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void InsertValues(string TableName, bool nocheck)
        {

            Console.WriteLine("Inserting values with different formatting...");
            QaExec("INSERT INTO " + TableName + " VALUES(51,'{368161C8-9E07-4e25-8D7B-16EE03DA4D54}',7278821,31,31);");
            QaExec("INSERT INTO " + TableName + " VALUES(52, '{B87F9250-84B3-4356-8135-99E39DC5A29C}' ,7278821 ,32,32);");
            QaExec("INSERT INTO " + TableName + " VALUES( 53 , '{1D42BCBD-441E-4e84-A51E-F32AA1B4F8B5}' , 7278821 , 33 , 33 );");
            QaExec("INSERT INTO " + TableName + " VALUES ( 54 , '{36833B97-5DFF-4643-9E4C-158E156B100D}' , 7278821  , 34  , 34  ) ; ");
            QaExec(" Insert  inTo  " + TableName + " values   ( 55 , '{E845F6BC-50AE-4a7a-A238-77F4B92D3500}' , 7278821  , 35   ,  35  ) ; ");

            Console.WriteLine("Inserting negative integer...");
            QaExec("insert into " + TableName + " values(-31,'{514068AA-DFCA-4891-AE93-38A76B83B53D}',7278821,-36,-36);");

            Console.WriteLine("Inserting positive integer with sign specified (+)...");
            QaExec("insert into " + TableName + " values(+61,'{4BB19E5F-1EA8-4e6c-AC4D-BF962B4D595A}',7278821,+37,+37);");

            if (nocheck)
            {
                return;
            }

            {
                Console.WriteLine("Ensuring values inserted into table...");
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE x = 7278821 ORDER BY NUM;");
                if (-1 == outputSelect1.IndexOf("{368161C8-9E07-4e25-8D7B-16EE03DA4D54}")
                    || -1 == outputSelect1.IndexOf("{B87F9250-84B3-4356-8135-99E39DC5A29C}")
                    || -1 == outputSelect1.IndexOf("{1D42BCBD-441E-4e84-A51E-F32AA1B4F8B5}")
                    || -1 == outputSelect1.IndexOf("{36833B97-5DFF-4643-9E4C-158E156B100D}")
                    || -1 == outputSelect1.IndexOf("{E845F6BC-50AE-4a7a-A238-77F4B92D3500}")
                    || -1 == outputSelect1.IndexOf("{4BB19E5F-1EA8-4e6c-AC4D-BF962B4D595A}")
                    || -1 == outputSelect1.IndexOf("{514068AA-DFCA-4891-AE93-38A76B83B53D}"))
                {
                    Console.Error.WriteLine("Did not find expected inserted values; SELECT output: {0}", outputSelect1.Trim());
                    throw new Exception("Did not find expected inserted values");
                }
            }

        }

    }
}
