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
        public static void DbAggregators_STD_SAMP()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 20;
            double[] values = new double[rowcount];

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_STD_SAMP(Double)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                for (int i = 0; i < fargs.Length; i++)
                {
                    double input = rnd.NextDouble();
                    if (input < 0.5)
                    {
                        input = input * -1d;
                    }
                    values[i] = input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.STD_SAMP(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = std(values, true);
                if (expected != output)
                {
                    throw new Exception("DbAggregators_STD_SAMP(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_STD_SAMP(Int32)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                for (int i = 0; i < fargs.Length; i++)
                {
                    int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                    values[i] = (double)input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.STD_SAMP(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = std(values, true);
                if (expected != output)
                {
                    throw new Exception("DbAggregators_STD_SAMP(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_STD_SAMP(Int64)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                for (int i = 0; i < fargs.Length; i++)
                {
                    long input = DateTime.Now.Ticks;
                    if (input % 2 == 0)
                    {
                        input = input * -1;
                    }
                    values[i] = (double)input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.STD_SAMP(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = std(values, true);
                if (expected != output)
                {
                    throw new Exception("DbAggregators_STD_SAMP(Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
