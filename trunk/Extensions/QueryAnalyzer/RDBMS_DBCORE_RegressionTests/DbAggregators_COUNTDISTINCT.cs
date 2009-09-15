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
        public static void DbAggregators_COUNTDISTINCT()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 20;

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_COUNTDISTINCT(Double)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                Dictionary<double, short> dict = new Dictionary<double, short>();
                double repeat = 0;
                for (int i = 0; i < fargs.Length; i++)
                {                   
                    double input = rnd.NextDouble();
                    if (input < 0.5)
                    {
                        input = input * -1d;
                    }
                    if(i == 0)
                    {
                        repeat = input;
                    }

                    if (i % 3 == 1)
                    {
                        input = repeat;
                    }

                    dict[input] = 0;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.COUNTDISTINCT(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = dict.Count;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_COUNTDISTINCT(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_COUNTDISTINCT(Int32)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                Dictionary<int, short> dict = new Dictionary<int, short>();
                int repeat = 0;
                for (int i = 0; i < fargs.Length; i++)
                {
                    int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                    if (i == 0)
                    {
                        repeat = input;
                    }

                    if (i % 3 == 1)
                    {
                        input = repeat;
                    }
                    dict[input] = 0;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.COUNTDISTINCT(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = dict.Count;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_COUNTDISTINCT(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_COUNTDISTINCT(Int64)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                Dictionary<long, short> dict = new Dictionary<long, short>();
                long repeat = 0;
                for (int i = 0; i < fargs.Length; i++)
                {
                    long input = DateTime.Now.Ticks;
                    if (input % 2 == 0)
                    {
                        input = input * -1;
                    }
                    if (i == 0)
                    {
                        repeat = input;
                    }

                    if (i % 3 == 1)
                    {
                        input = repeat;
                    }
                    dict[input] = 0;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.COUNTDISTINCT(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = dict.Count;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_COUNTDISTINCT(Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_COUNTDISTINCT(char(n))...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                Dictionary<string, short> dict = new Dictionary<string, short>();
                mstring repeat = mstring.Prepare();
                for (int i = 0; i < fargs.Length; i++)
                {
                    int strlen = rnd.Next(1, 100);
                    mstring input = Utils.GenString(strlen);
                    if (i == 0)
                    {
                        repeat = input;
                    }

                    if (i % 3 == 1)
                    {
                        input = repeat;
                    }
                    dict[input.ToString()] = 0;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.COUNTDISTINCT(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = dict.Count;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_COUNTDISTINCT(char(n)) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
