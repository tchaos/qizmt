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
        public static void DbFunctions_ATN2()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            //Double.
            {
                Console.WriteLine("Testing DbFunctions.ATN2(Double, Double)...");

                double y = rnd.NextDouble();
                if (y < 0.5)
                {
                    y = y * -1d;
                }
                double x = rnd.NextDouble();
                if (x < 0.5)
                {
                    x = x * -1d;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(y));
                args.Add(tools.AllocValue(x));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ATN2(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Atan2(y, x);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ATN2(Double, Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.ATN2(Int32, Int32)...");

                int y = rnd.Next(Int32.MinValue, Int32.MaxValue);
                int x = rnd.Next(Int32.MinValue, Int32.MaxValue);

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(y));
                args.Add(tools.AllocValue(x));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ATN2(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Atan2((double)y, (double)x);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ATN2(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.ATN2(Long, Long)...");

                long y = DateTime.Now.Ticks;
                long x = DateTime.Now.Ticks;

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(y));
                args.Add(tools.AllocValue(x));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ATN2(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Atan2((double)y, (double)x);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ATN2(Long, Long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
