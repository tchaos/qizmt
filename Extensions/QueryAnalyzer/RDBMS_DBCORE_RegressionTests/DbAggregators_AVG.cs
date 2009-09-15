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
        public static void DbAggregators_AVG()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 20;

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_AVG(Double)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                double sum = 0;

                for (int i = 0; i < fargs.Length; i++)
                {
                    double input = rnd.NextDouble();
                    if (input < 0.5)
                    {
                        input = input * -1d;
                    }
                    sum += input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.AVG(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = sum / (double)rowcount;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_AVG(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_AVG(Int32)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                double sum = 0;

                for (int i = 0; i < fargs.Length; i++)
                {
                    int input = rnd.Next(Int32.MinValue, Int32.MaxValue);                   
                    sum += (double)input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.AVG(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = sum / (double)rowcount;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_AVG(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_AVG(Int64)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                double sum = 0;

                for (int i = 0; i < fargs.Length; i++)
                {
                    long input = DateTime.Now.Ticks;
                    if (input % 2 == 0)
                    {
                        input = input * -1;
                    }
                    sum += (double)input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.AVG(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = sum / (double)rowcount;
                if (expected != output)
                {
                    throw new Exception("DbAggregators_AVG(Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
