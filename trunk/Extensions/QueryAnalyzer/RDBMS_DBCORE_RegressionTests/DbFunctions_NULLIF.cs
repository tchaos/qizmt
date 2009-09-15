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
        public static void DbFunctions_NULLIF()
        {
            DbFunctionTools tools = new DbFunctionTools();

            {
                Console.WriteLine("Testing DbFunctions.NULLIF(DateTime, DateTime)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(DateTime.Parse("12/1/2000 10:00:00 AM")));
                args.Add(tools.AllocValue(DateTime.Parse("12/1/2000 10:00:00 AM")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NULLIF(tools, fargs);                
                ByteSlice bs = valOutput.Eval();
                bool output = Types.IsNullValue(bs);
                bool expected = true;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NULLIF(DateTime, DateTime) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NULLIF(DateTime, DateTime)...");

                List<DbValue> args = new List<DbValue>();
                DateTime dt = DateTime.Parse("12/1/2000 10:00:00 AM");
                args.Add(tools.AllocValue(dt));
                args.Add(tools.AllocValue(DateTime.Parse("12/2/2000 10:00:00 AM")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NULLIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NULLIF(DateTime, DateTime) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NULLIF(Int32, Int32)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(10));
                args.Add(tools.AllocValue(10));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NULLIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                bool output = Types.IsNullValue(bs);
                bool expected = true;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NULLIF(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NULLIF(Int32, Int32)...");

                List<DbValue> args = new List<DbValue>();
                int x = 10;
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(11));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NULLIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = x;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NULLIF(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NULLIF(Int32, long)...");

                List<DbValue> args = new List<DbValue>();
                int x = 10;
                long y = 10;
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NULLIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                bool output = Types.IsNullValue(bs);
                bool expected = true;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NULLIF(Int32, long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NULLIF(Int32, long)...");

                List<DbValue> args = new List<DbValue>();
                int x = 10;
                long y = 11;
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NULLIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);
                int expected = x;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NULLIF(Int32, long) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
