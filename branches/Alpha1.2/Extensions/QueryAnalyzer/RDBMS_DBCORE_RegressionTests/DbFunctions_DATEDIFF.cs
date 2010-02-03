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
        public static void DbFunctions_DATEDIFF()
        {
            DbFunctionTools tools = new DbFunctionTools();
           
            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(year)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("year");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2010, 12, 2, 10, 0, 0);
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = -10;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(year) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(quarter)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("qq");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2000, 7, 1, 10, 0, 0);
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2002, 8, 30, 12, 0, 0);
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = 8;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(quarter) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(month)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("month");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2000, 7, 1, 10, 0, 0);
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2002, 8, 15, 12, 0, 0);
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);
                double expected = 25;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(month) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(day)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("d");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2000, 7, 1, 10, 0, 0);
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2002, 8, 20, 12, 0, 0);
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = 0;
                {
                    DateTime xstartdate = new DateTime(2000, 7, 1);
                    DateTime xenddate = new DateTime(2002, 8, 20);
                    TimeSpan sp = xenddate - xstartdate;
                    expected = sp.TotalDays;
                }                

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(day) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(week)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("week");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2000, 7, 1, 10, 0, 0); 
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2002, 8, 20, 12, 0, 0); 
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = 0;
                {
                    DateTime xstartdate = new DateTime(2000, 7, 1);
                    DateTime xenddate = new DateTime(2002, 8, 20); 
                    TimeSpan sp = xenddate - xstartdate;
                    expected = (int)sp.TotalDays / 7;
                }

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(week) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(hour)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("hh");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2000, 7, 1, 10, 30, 1); 
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2002, 8, 20, 12, 20, 3);
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = 0;
                {
                    DateTime xstartdate = new DateTime(2000, 7, 1, 10, 0, 0);
                    DateTime xenddate = new DateTime(2002, 8, 20, 12, 0, 0);
                    TimeSpan sp = xenddate - xstartdate;
                    expected = sp.TotalHours;
                }

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(hour) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(minute)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("minute");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2000, 7, 1, 10, 30, 1); 
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2002, 8, 20, 12, 20, 3);
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = 0;
                {
                    DateTime xstartdate = new DateTime(2000, 7, 1, 10, 30, 0);
                    DateTime xenddate = new DateTime(2002, 8, 20, 12, 20, 0);
                    TimeSpan sp = xenddate - xstartdate;
                    expected = sp.TotalMinutes;
                }

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(minute) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(second)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("ss");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2000, 7, 1, 10, 30, 1);
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2002, 8, 20, 12, 20, 3); 
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = 0;
                {
                    DateTime xstartdate = new DateTime(2000, 7, 1, 10, 30, 1);
                    DateTime xenddate = new DateTime(2002, 8, 20, 12, 20, 3); 
                    TimeSpan sp = xenddate - xstartdate;
                    expected = sp.TotalSeconds;
                }

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(second) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.DATEDIFF(millisecond)...");

                List<DbValue> args = new List<DbValue>();
                mstring datepart = mstring.Prepare("ms");
                args.Add(tools.AllocValue(datepart));
                DateTime startdate = new DateTime(2000, 7, 1, 10, 30, 1, 2); 
                args.Add(tools.AllocValue(startdate));
                DateTime enddate = new DateTime(2002, 8, 20, 12, 20, 3, 5); 
                args.Add(tools.AllocValue(enddate));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DATEDIFF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = 0;
                TimeSpan sp = enddate - startdate;
                expected = sp.TotalMilliseconds;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DATEDIFF(millisecond) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
