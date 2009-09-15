using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void WhereISXNULL(string TableName)
        {

            {
                Console.WriteLine("Inserting row (with nulls and non-nulls to check)...");

                QaExec("INSERT INTO " + TableName + " VALUES(45444588,null,45444588,Null,126262646818);"); // str and y are null

                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE num = 45444588");
                if (1 != CountOccurences(outputSelect1, "126262646818"))
                {
                    Console.Error.WriteLine("Expected to find inserted value once, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find inserted value once");
                }

            }


            {
                Console.WriteLine("IS NULL...");
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE num is null");
                    if (-1 != outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select num is null");
                    }
                }
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str is null");
                    if (-1 == outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select str is nul");
                    }
                }
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE x is null");
                    if (-1 != outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select x is null");
                    }
                }
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE y is null");
                    if (-1 == outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select y is null");
                    }
                }

            }


            {
                Console.WriteLine("IS NOT NULL...");
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE num is not null");
                    if (-1 == outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select num is not null");
                    }
                }
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str is not null");
                    if (-1 != outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select str is not nul");
                    }
                }
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE x is not null");
                    if (-1 == outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select x is not null");
                    }
                }
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE y is not null");
                    if (-1 != outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select y is not null");
                    }
                }

            }

            {
                Console.WriteLine("More complex expressions...");
                {
                    string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE num=99 OR y IS NULL OR Left(str,1) = 'x' OR str Is Not Null");
                    if (-1 == outputSelect1.IndexOf("126262646818"))
                    {
                        Console.Error.WriteLine("Did not find expected value, select output: {0}", outputSelect1.Trim());
                        throw new Exception("Did not find expected value  select y is null");
                    }
                }
            }

        }

    }
}
