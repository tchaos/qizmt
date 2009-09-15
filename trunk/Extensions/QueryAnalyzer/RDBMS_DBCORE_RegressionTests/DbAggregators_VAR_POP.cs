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
        private static double var(double[] values, bool sample)
        {
            double sum = 0;
            foreach (double v in values)
            {
                sum += v;
            }

            double avg = sum / (double)values.Length;

            double dev = 0;
            foreach (double v in values)
            {
                dev += Math.Pow(v - avg, 2);
            }
            return dev / (double)(sample ? values.Length - 1 : values.Length);
        }

        public static void DbAggregators_VAR_POP()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 20;
            double[] values = new double[rowcount];

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_VAR_POP(Double)...");

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
                DbValue valOutput = DbAggregators.VAR_POP(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = var(values, false);
                if (expected != output)
                {
                    throw new Exception("DbAggregators_VAR_POP(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_VAR_POP(Int32)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                for (int i = 0; i < fargs.Length; i++)
                {
                    int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                    values[i] = (double)input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.VAR_POP(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = var(values, false);
                if (expected != output)
                {
                    throw new Exception("DbAggregators_VAR_POP(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_VAR_POP(Int64)...");

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
                DbValue valOutput = DbAggregators.VAR_POP(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = var(values, false);
                if (expected != output)
                {
                    throw new Exception("DbAggregators_VAR_POP(Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
