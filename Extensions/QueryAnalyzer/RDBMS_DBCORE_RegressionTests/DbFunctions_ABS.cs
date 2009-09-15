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
        public static void DbFunctions_ABS()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();
      
            //Int32.
            {
                Console.WriteLine("Testing DbFunctions.ABS(Int32)...");

                int input = rnd.Next(Int32.MinValue, Int32.MaxValue);
                DbValue valInput = tools.AllocValue(input);
                List<DbValue> args = new List<DbValue>();
                args.Add(valInput);
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ABS(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = Math.Abs(input);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ABS(Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            //Int64.
            {
                Console.WriteLine("Testing DbFunctions.ABS(Int64)...");

                long input = DateTime.Now.Ticks - 1;
                if (input % 2 == 0)
                {
                    input = input * -1L;
                }

                DbValue valInput = tools.AllocValue(input);
                List<DbValue> args = new List<DbValue>();
                args.Add(valInput);
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ABS(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                byte[] buf = bs.ToBytes();
                long output = Entry.ToInt64(Entry.BytesToULong(buf, 1));

                long expected = Math.Abs(input);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ABS(Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            //Double.
            {
                Console.WriteLine("Testing DbFunctions.ABS(Double)...");

                double input = rnd.NextDouble();

                if (input < 0.5)
                {
                    input = input * -1d;
                }
               
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ABS(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                byte[] buf = bs.ToBytes();
                double output = Entry.BytesToDouble(buf, 1);

                double expected = Math.Abs(input);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ABS(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
