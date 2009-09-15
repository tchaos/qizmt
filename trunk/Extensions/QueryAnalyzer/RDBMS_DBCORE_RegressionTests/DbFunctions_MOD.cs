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
        public static void DbFunctions_MOD()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            //Double.
            {
                Console.WriteLine("Testing DbFunctions.MOD(Double, Double)...");

                double input0 = rnd.NextDouble();
                double input1 = rnd.NextDouble();

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input0));
                args.Add(tools.AllocValue(input1));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MOD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = input0 % input1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MOD(Double, Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.MOD(Int32, Int32)...");

                int input0 = rnd.Next();
                int input1 = rnd.Next();

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input0));
                args.Add(tools.AllocValue(input1));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MOD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input0 % input1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MOD(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.MOD(Long, Long)...");

                long input0 = DateTime.Now.Ticks;
                long input1 = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input0));
                args.Add(tools.AllocValue(input1));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MOD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                long expected = input0 % input1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MOD(Long, Long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.MOD(Int32, Double)...");

                int input0 = rnd.Next();
                double input1 = rnd.NextDouble();

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input0));
                args.Add(tools.AllocValue(input1));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MOD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = (double)input0 % input1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MOD(Int32, Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.MOD(Int32, Long)...");

                int input0 = rnd.Next();
                long input1 = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input0));
                args.Add(tools.AllocValue(input1));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MOD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                long expected = (long)input0 % input1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MOD(Int32, Long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
