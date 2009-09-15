using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void CreateTable(string TableName, bool nocheck)
        {

            string output = "";
            try
            {
                Console.WriteLine("Creating table...");
                Console.WriteLine(QaExec(" CREATE  TABLE  " + TableName + "  ( Num Int , Str Char ( 40 ) , X Int, Y Long, Z Double ) ; ").Trim());

                if (nocheck)
                {
                    return;
                }

                Console.WriteLine("Ensuring table created...");
                output = QaExec("SELECT TOP 0 * FROM " + TableName + " WHERE num=0 ORDER BY num"); // Ensures no exception.

            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Failure: {0} - {1}", e.Message, output.Trim());
                throw;
            }

        }

    }
}
