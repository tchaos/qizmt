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
        public static void DbAggregators_CHOOSERND()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 20;

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_CHOOSERND(Double)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                Dictionary<double, int> dict = new Dictionary<double, int>();

                for (int i = 0; i < fargs.Length; i++)
                {
                    double input = rnd.NextDouble();
                    if (input < 0.5)
                    {
                        input = input * -1d;
                    }
                    dict[input] = 1;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.CHOOSERND(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                if (!dict.ContainsKey(output))
                {
                    throw new Exception("DbAggregators_CHOOSERND(Double) has failed.  Value not found: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_CHOOSERND(Int32)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                Dictionary<int, int> dict = new Dictionary<int, int>();

                for (int i = 0; i < fargs.Length; i++)
                {
                    int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                    dict[input] = 1;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.CHOOSERND(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                if (!dict.ContainsKey(output))
                {
                    throw new Exception("DbAggregators_CHOOSERND(Int32) has failed.  Value not found: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_CHOOSERND(Int64)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                Dictionary<long, int> dict = new Dictionary<long, int>();

                for (int i = 0; i < fargs.Length; i++)
                {
                    long input = DateTime.Now.Ticks;
                    if (input % 2 == 0)
                    {
                        input = input * -1;
                    }
                    dict[input] = 1;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.CHOOSERND(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                if (!dict.ContainsKey(output))
                {
                    throw new Exception("DbAggregators_CHOOSERND(Int64) has failed.  Value not found: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_CHOOSERND(char(n))...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                Dictionary<string, int> dict = new Dictionary<string, int>();

                for (int i = 0; i < fargs.Length; i++)
                {
                    int strlen = rnd.Next(1, 100);
                    mstring input = Utils.GenString(strlen);
                    dict[input.ToString()] = 1;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.CHOOSERND(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                if (!dict.ContainsKey(output.ToString()))
                {
                    throw new Exception("DbAggregators_CHOOSERND(char(n)) has failed.  Value not found: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
