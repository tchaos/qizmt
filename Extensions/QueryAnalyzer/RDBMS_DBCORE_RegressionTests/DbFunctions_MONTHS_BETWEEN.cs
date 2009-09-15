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
        public static void DbFunctions_MONTHS_BETWEEN()
        {
            DbFunctionTools tools = new DbFunctionTools();

            {
                Console.WriteLine("Testing DbFunctions.MONTHS_BETWEEN...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(DateTime.Parse("1/31/2000 10:00:00 AM")));
                args.Add(tools.AllocValue(DateTime.Parse("1/3/2000 10:00:00 AM")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MONTHS_BETWEEN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = (double)(31 - 3) / (double)31;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MONTHS_BETWEEN has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
           
            {
                Console.WriteLine("Testing DbFunctions.MONTHS_BETWEEN...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(DateTime.Parse("1/31/2000 10:00:00 AM")));
                args.Add(tools.AllocValue(DateTime.Parse("4/30/1999 10:00:00 AM")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MONTHS_BETWEEN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = 9;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MONTHS_BETWEEN has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.MONTHS_BETWEEN...");

                List<DbValue> args = new List<DbValue>();
                DateTime d0 = DateTime.Parse("1/31/2000 10:00:00 AM");
                DateTime d1 = DateTime.Parse("4/1/1999 10:00:00 AM");
                args.Add(tools.AllocValue(d0));
                args.Add(tools.AllocValue(d1));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MONTHS_BETWEEN(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                TimeSpan sp = d0 - d1;
                double expected = sp.TotalDays / 31d;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MONTHS_BETWEEN has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
