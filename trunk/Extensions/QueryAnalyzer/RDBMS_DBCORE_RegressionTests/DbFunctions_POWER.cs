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
        public static void DbFunctions_POWER()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            
            {
                Console.WriteLine("Testing DbFunctions.POWER(Int32, Int32)...");

                int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                int pow = rnd.Next(Int32.MinValue, Int32.MaxValue);

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                args.Add(tools.AllocValue(pow));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.POWER(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Pow((double)input, (double)pow);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.POWER(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.POWER(Double, Double)...");

                double input = rnd.NextDouble();
                double pow = rnd.NextDouble();

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                args.Add(tools.AllocValue(pow));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.POWER(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Pow(input, pow);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.POWER(Double, Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.POWER(Long, Long)...");

                long input = DateTime.Now.Ticks;
                long pow = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                args.Add(tools.AllocValue(pow));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.POWER(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Pow((double)input, (double)pow);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.POWER(Long, Long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.POWER(Int32, Long)...");

                int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                long pow = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                args.Add(tools.AllocValue(pow));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.POWER(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Pow((double)input, (double)pow);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.POWER(Int32, Long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.POWER(Int32, Double)...");

                int input = rnd.Next();
                double pow = rnd.NextDouble();

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                args.Add(tools.AllocValue(pow));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.POWER(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Pow((double)input, pow);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.POWER(Int32, Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
