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
        public static void DbFunctions_NOTEQUAL()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            {
                Console.WriteLine("Testing DbFunctions.EQUAL(char(n), char(n))...");

                mstring s1 = Utils.GenString(rnd.Next(1, 200));
                mstring s2 = Utils.GenString(rnd.Next(1, 200));

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(s1));
                args.Add(tools.AllocValue(s2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NOTEQUAL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = (s1.ToString().CompareTo(s2.ToString()) != 0 ? 1 : 0);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NOTEQUAL(char(n), char(n)) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NOTEQUAL(Int32, Int32)...");

                int x = rnd.Next(Int32.MinValue, Int32.MaxValue);
                int y = rnd.Next(Int32.MinValue, Int32.MaxValue);

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NOTEQUAL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = (x != y ? 1 : 0);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NOTEQUAL(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NOTEQUAL(Double, Double)...");

                double x = rnd.NextDouble();
                double y = rnd.NextDouble();

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NOTEQUAL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = (x != y ? 1 : 0);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NOTEQUAL(Double, Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NOTEQUAL(Long, Long)...");

                long x = DateTime.Now.Ticks;
                long y = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NOTEQUAL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = (x != y ? 1 : 0);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NOTEQUAL(Long, Long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NOTEQUAL(Double, Int32)...");

                double x = rnd.NextDouble();
                int y = rnd.Next();

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NOTEQUAL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = (x != y ? 1 : 0);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NOTEQUAL(Double, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NOTEQUAL(Double, Long)...");

                double x = rnd.NextDouble();
                long y = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NOTEQUAL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = (x != y ? 1 : 0);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NOTEQUAL(Double, Long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
