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
        public static void DbAggregators_COUNT()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 20;

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_COUNT(Double)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                for (int i = 0; i < fargs.Length; i++)
                {
                    double input = rnd.NextDouble();
                    if (input < 0.5)
                    {
                        input = input * -1d;
                    }
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.COUNT(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = rowcount;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_COUNT(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_COUNT(Int32)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                for (int i = 0; i < fargs.Length; i++)
                {
                    int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.COUNT(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = rowcount;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_COUNT(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_COUNT(Int64)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];

                for (int i = 0; i < fargs.Length; i++)
                {
                    long input = DateTime.Now.Ticks;
                    if (input % 2 == 0)
                    {
                        input = input * -1;
                    }
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.COUNT(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = rowcount;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_COUNT(Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_COUNT(char(n))...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];               
                for (int i = 0; i < fargs.Length; i++)
                {
                    int strlen = rnd.Next(1, 100);
                    mstring input = Utils.GenString(strlen);
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.COUNT(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = rowcount;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_COUNT(char(n)) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
