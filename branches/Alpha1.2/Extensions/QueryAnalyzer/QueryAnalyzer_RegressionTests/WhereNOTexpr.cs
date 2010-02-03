using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_RegressionTests
{
    public partial class Program
    {
        static void WhereNOTexpr(string TableName)
        {

            {
                Console.WriteLine("Inserting row...");

                QaExec("INSERT INTO " + TableName + " VALUES(2815455,'{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}',2815455,2815455,2815455);");

            }

            Console.WriteLine("WHERE NOT <expression>...");
            {
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}' AND NOT num=100000");
                if (-1 == outputSelect1.IndexOf("{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}"))
                {
                    Console.Error.WriteLine("Expected to find inserted value  str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}' AND NOT num=100000, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find inserted value  str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}' AND NOT num=100000");
                }
            }
            {
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE NOT str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}'");
                if (-1 != outputSelect1.IndexOf("{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}"))
                {
                    Console.Error.WriteLine("Expected not to find inserted value  NOT str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}', select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected not to find inserted value  NOT str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}'");
                }
            }
            {
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}' AND NOT num=2815455");
                if (-1 != outputSelect1.IndexOf("{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}"))
                {
                    Console.Error.WriteLine("Expected not to find inserted value  str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}' AND NOT num=2815455, select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected not to find inserted value  str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}' AND NOT num=2815455");
                }
            }
            {
                string outputSelect1 = QaExec("SELECT TOP 999 * FROM " + TableName + " WHERE (NOT num=100000 OR str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}')   AND (num=2815455)");
                if (-1 == outputSelect1.IndexOf("{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}"))
                {
                    Console.Error.WriteLine("Expected to find inserted value  NOT num=100000 OR str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}', select output: {0}", outputSelect1.Trim());
                    throw new Exception("Expected to find inserted value  NOT num=100000 OR str = '{225C4BAC-856F-4479-B7A8-FB1A6EBAB3FB}'");
                }
            }

        }

    }
}
