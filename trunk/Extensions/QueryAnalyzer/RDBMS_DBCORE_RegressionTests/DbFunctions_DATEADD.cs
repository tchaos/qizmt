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
        public static void DbFunctions_DATEADD()
        {
            DbFunctionTools tools = new DbFunctionTools();

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(year)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("year");
                args.Add(tools.AllocValue(datepart));
                int number = -10;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddYears(number);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(year) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(quarter)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("qq");
                args.Add(tools.AllocValue(datepart));
                int number = 5;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddMonths(number * 3);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(quarter) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(month)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("m");
                args.Add(tools.AllocValue(datepart));
                int number = 10;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddMonths(number);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(month) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(day)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("day");
                args.Add(tools.AllocValue(datepart));
                int number = -9;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddDays(number);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(day) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(week)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("wk");
                args.Add(tools.AllocValue(datepart));
                int number = 22;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddDays(number * 7);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(week) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(hour)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("hour");
                args.Add(tools.AllocValue(datepart));
                int number = -99;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddHours(number);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(hour) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(minute)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("mi");
                args.Add(tools.AllocValue(datepart));
                int number = 80;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddMinutes(number);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(minute) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(second)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("s");
                args.Add(tools.AllocValue(datepart));
                int number = 900;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddSeconds(number);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(second) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEADD(millisecond)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("millisecond");
                args.Add(tools.AllocValue(datepart));
                int number = 900;
                args.Add(tools.AllocValue(number));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);
                DateTime expected = dt.AddMilliseconds(number);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEADD(millisecond) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
