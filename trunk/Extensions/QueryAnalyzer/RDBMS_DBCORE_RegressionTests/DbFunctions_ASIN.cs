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
        public static void DbFunctions_ASIN()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            //Double.
            {
                Console.WriteLine("Testing DbFunctions.ASIN(Double)...");

                double input = rnd.NextDouble();

                if (input < 0.5)
                {
                    input = input * -1d;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ASIN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Asin(input);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ASIN(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.ASIN(Int32)...");

                int input = rnd.Next(Int32.MinValue, Int32.MaxValue);

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ASIN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Asin((double)input);

                if (Double.IsNaN(output) && Double.IsNaN(expected))
                {
                    Console.WriteLine("Expected results received.");
                }
                else
                {
                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ASIN(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.ASIN(Long)...");

                long input = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ASIN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Asin((double)input);

                if (Double.IsNaN(output) && Double.IsNaN(expected))
                {
                    Console.WriteLine("Expected results received.");
                }
                else
                {
                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ASIN(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }
            }
        }
    }
}
