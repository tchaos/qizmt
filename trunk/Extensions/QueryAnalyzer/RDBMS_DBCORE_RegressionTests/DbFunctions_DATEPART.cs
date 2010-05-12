using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RDBMS_DBCORE;
using MySpace.DataMining.DistributedObjects;

namespace RDBMS_DBCORE_RegressionTests
{
    public partial class Program
    {
        public static void DbFunctions_DATEPART()
        {
            DbFunctionTools tools = new DbFunctionTools();

            DateTime input = new DateTime(1999, 3, 14, 13, 1, 33);

            DbValue valInput = tools.AllocValue(input);
            List<DbValue> args = new List<DbValue>();
            args.Add(valInput);
            DbFunctionArguments fargs = new DbFunctionArguments(args);

            {
                Console.WriteLine("Testing DbFunctions.DATEPART_YEAR...");

                DbValue valOutput = DbFunctions.DATEPART_YEAR(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input.Year;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEPART_YEAR has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEPART_MONTH...");

                DbValue valOutput = DbFunctions.DATEPART_MONTH(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input.Month;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEPART_MONTH has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEPART_DAY...");

                DbValue valOutput = DbFunctions.DATEPART_DAY(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input.Day;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEPART_DAY has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEPART_HOUR...");

                DbValue valOutput = DbFunctions.DATEPART_HOUR(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input.Hour;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEPART_HOUR has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEPART_MINUTE...");

                DbValue valOutput = DbFunctions.DATEPART_MINUTE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input.Minute;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEPART_MINUTE has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEPART_SECOND...");

                DbValue valOutput = DbFunctions.DATEPART_SECOND(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input.Second;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEPART_SECOND has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

           
        }
    }
}
