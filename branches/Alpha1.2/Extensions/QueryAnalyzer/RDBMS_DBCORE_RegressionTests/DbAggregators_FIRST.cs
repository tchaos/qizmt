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
        public static void DbAggregators_FIRST()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 20;

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_FIRST(Double)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                double expected = 0;

                for (int i = 0; i < fargs.Length; i++)
                {
                    double input = rnd.NextDouble();
                    if (input < 0.5)
                    {
                        input = input * -1d;
                    }
                    if (i == 0)
                    {
                        expected = input;
                    }

                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.FIRST(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                if (expected != output)
                {
                    throw new Exception("DbAggregators_FIRST(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_FIRST(Int32)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                int expected = Int32.MaxValue;

                for (int i = 0; i < fargs.Length; i++)
                {
                    int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                    if (i == 0)
                    {
                        expected = input;
                    }
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.FIRST(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                if (expected != output)
                {
                    throw new Exception("DbAggregators_FIRST(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_FIRST(Int64)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                long expected = Int64.MaxValue;

                for (int i = 0; i < fargs.Length; i++)
                {
                    long input = DateTime.Now.Ticks;
                    if (input % 2 == 0)
                    {
                        input = input * -1;
                    }
                    if (i == 0)
                    {
                        expected = input;
                    }
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.FIRST(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                if (expected != output)
                {
                    throw new Exception("DbAggregators_FIRST(Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_FIRST(char(n))...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                string expected = null;

                for (int i = 0; i < fargs.Length; i++)
                {
                    int strlen = rnd.Next(1, 100);
                    mstring input = Utils.GenString(strlen);
                    if (i == 0)
                    {
                        expected = input.ToString();
                    }
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.FIRST(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                if (expected != output.ToString())
                {
                    throw new Exception("DbAggregators_FIRST(char(n)) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
