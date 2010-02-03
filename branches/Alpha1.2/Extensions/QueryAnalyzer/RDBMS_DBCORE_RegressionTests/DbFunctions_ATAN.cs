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
        public static void DbFunctions_ATAN()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            //Double.
            {
                Console.WriteLine("Testing DbFunctions.ATAN(Double)...");

                double input = rnd.NextDouble();

                if (input < 0.5)
                {
                    input = input * -1d;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ATAN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Atan(input);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ATAN(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.ATAN(Int32)...");

                int input = rnd.Next(Int32.MinValue, Int32.MaxValue);

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ATAN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Atan((double)input);

                if (Double.IsNaN(output) && Double.IsNaN(expected))
                {
                    Console.WriteLine("Expected results received.");
                }
                else
                {
                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ATAN(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.ATAN(Long)...");

                long input = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ATAN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Atan((double)input);

                if (Double.IsNaN(output) && Double.IsNaN(expected))
                {
                    Console.WriteLine("Expected results received.");
                }
                else
                {
                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ATAN(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
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
