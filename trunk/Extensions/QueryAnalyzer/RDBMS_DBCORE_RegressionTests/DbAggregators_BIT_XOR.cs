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
        public static void DbAggregators_BIT_XOR()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 3;

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_BIT_XOR(Double)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                double[] values = new double[rowcount];

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
                DbValue valOutput = DbAggregators.BIT_XOR(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                byte[] expected = bitop(values, 3);

                if (!compareBytes(expected, bs))
                {
                    throw new Exception("DbAggregators_BIT_XOR(Double) has failed.");
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_BIT_XOR(Int32)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                int[] values = new int[rowcount];

                for (int i = 0; i < fargs.Length; i++)
                {
                    int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                    values[i] = input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.BIT_XOR(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                byte[] expected = bitop(values, 3);
                if (!compareBytes(expected, bs))
                {
                    throw new Exception("DbAggregators_BIT_XOR(Int32) has failed.");
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_BIT_XOR(Int64)...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                long[] values = new long[rowcount];

                for (int i = 0; i < fargs.Length; i++)
                {
                    long input = DateTime.Now.Ticks;
                    if (input % 2 == 0)
                    {
                        input = input * -1;
                    }
                    values[i] = input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.BIT_XOR(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);
                byte[] expected = bitop(values, 3);
                if (!compareBytes(expected, bs))
                {
                    throw new Exception("DbAggregators_BIT_XOR(Int64) has failed.");
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_BIT_XOR(char(n))...");

                DbFunctionArguments[] fargs = new DbFunctionArguments[rowcount];
                mstring[] values = new mstring[rowcount];

                for (int i = 0; i < fargs.Length; i++)
                {
                    int strlen = 30;
                    mstring input = Utils.GenString(strlen);
                    values[i] = input;
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(input));
                    fargs[i] = new DbFunctionArguments(args);
                }
                DbValue valOutput = DbAggregators.BIT_XOR(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);
                byte[] expected = bitop(values, 4);
                if (!compareBytes(expected, bs))
                {
                    throw new Exception("DbAggregators_BIT_XOR(char(n)) has failed.");
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
