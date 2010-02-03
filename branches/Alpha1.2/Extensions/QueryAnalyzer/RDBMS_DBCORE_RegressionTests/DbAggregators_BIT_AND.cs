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
        private static void bitop(byte[] x, byte[] y, int whatop)
        {
            for (int ib = 0; ib < x.Length; ib++)
            {
                if (whatop == 1)
                {
                    x[ib] = (byte)(x[ib] & y[ib]);
                }
                else if (whatop == 2)
                {
                    x[ib] = (byte)(x[ib] | y[ib]);
                }
                else
                {
                    x[ib] = (byte)(x[ib] ^ y[ib]);
                }
            }
        }

        private static byte[] bitop(double[] values, int whatop)
        {           
            byte[] buf = new byte[9];
            byte[] result = new byte[9];
            for(int i = 0; i < values.Length; i++)
            {
                if (i == 0)
                {
                    Entry.DoubleToBytes(values[i], result, 0);
                    continue;
                }
                Entry.DoubleToBytes(values[i], buf, 0);
                bitop(result, buf, whatop);
            }
            return result;
        }

        private static byte[] bitop(int[] values, int whatop)
        {
            byte[] buf = new byte[4];
            byte[] result = new byte[4];
            for (int i = 0; i < values.Length; i++)
            {
                int x = 0;
                if (i == 0)
                {
                    x = Entry.ToInt32((UInt32)values[i]);
                    Entry.ToBytes(x, result, 0);
                    continue;
                }
                x = Entry.ToInt32((UInt32)values[i]);
                Entry.ToBytes(x, buf, 0);
                bitop(result, buf, whatop);
            }
            return result;
        }

        private static byte[] bitop(long[] values, int whatop)
        {
            byte[] buf = new byte[8];
            byte[] result = new byte[8];
            for (int i = 0; i < values.Length; i++)
            {
                long x = 0;
                if (i == 0)
                {
                    x = Entry.ToInt64((UInt64)values[i]);
                    Entry.LongToBytes(x, result, 0);
                    continue;
                }
                x = Entry.ToInt64((UInt64)values[i]);
                Entry.LongToBytes(x, buf, 0);
                bitop(result, buf, whatop);
            }
            return result;
        }

        private static byte[] bitop(mstring[] values, int whatop)
        {
            byte[] buf = null;
            byte[] result = null;
            for (int i = 0; i < values.Length; i++)
            {
                if (i == 0)
                {
                    ByteSlice bs = values[i].ToByteSliceUTF16();
                    result = bs.ToBytes();
                    continue;
                }

                {
                    ByteSlice bs = values[i].ToByteSliceUTF16();
                    buf = bs.ToBytes();
                } 
                bitop(result, buf, whatop);
            }
            return result;
        }

        private static bool compareBytes(byte[] b1, ByteSlice b2)
        {
            if (b1.Length != b2.Length - 1)  //include null front byte
            {
                return false;
            }

            for (int i = 0; i < b1.Length; i++)
            {
                if (b1[i] != b2[i + 1])
                {
                    return false;
                }
            }
            return true;
        }

        public static void DbAggregators_BIT_AND()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
            const int rowcount = 20;

            //Double.
            {
                Console.WriteLine("Testing DbAggregators_BIT_AND(Double)...");

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
                DbValue valOutput = DbAggregators.BIT_AND(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                byte[] expected = bitop(values, 1);

                if (!compareBytes(expected, bs))
                {
                    throw new Exception("DbAggregators_BIT_AND(Double) has failed.");
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbAggregators_BIT_AND(Int32)...");

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
                DbValue valOutput = DbAggregators.BIT_AND(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                byte[] expected = bitop(values, 1);
                if (!compareBytes(expected, bs))
                {
                    throw new Exception("DbAggregators_BIT_AND(Int32) has failed.");
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            
            {
                Console.WriteLine("Testing DbAggregators_BIT_AND(Int64)...");

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
                DbValue valOutput = DbAggregators.BIT_AND(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);
                byte[] expected = bitop(values, 1);
                if (!compareBytes(expected, bs))
                {
                    throw new Exception("DbAggregators_BIT_AND(Int64) has failed.");
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            
            {
                Console.WriteLine("Testing DbAggregators_BIT_AND(char(n))...");

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
                DbValue valOutput = DbAggregators.BIT_AND(tools, new DbAggregatorArguments(fargs));
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);
                byte[] expected = bitop(values, 1);
                if (!compareBytes(expected, bs))
                {
                    throw new Exception("DbAggregators_BIT_AND(char(n)) has failed.");
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
