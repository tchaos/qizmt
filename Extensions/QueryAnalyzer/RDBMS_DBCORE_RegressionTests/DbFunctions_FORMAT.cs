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
        public static void DbFunctions_FORMAT()
        {
            DbFunctionTools tools = new DbFunctionTools();

            {
                Console.WriteLine("Testing DbFunctions.FORMAT('MM')...");

                List<DbValue> args = new List<DbValue>();
                mstring formatstr = mstring.Prepare("MM");
                args.Add(tools.AllocValue(formatstr));               
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.FORMAT(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);
                mstring expected = mstring.Prepare(dt.ToString(formatstr.ToString()));

                if (expected != output)
                {
                    throw new Exception("DbFunctions.FORMAT('MM') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.FORMAT('dd')...");

                List<DbValue> args = new List<DbValue>();
                mstring formatstr = mstring.Prepare("dd");
                args.Add(tools.AllocValue(formatstr));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.FORMAT(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);
                mstring expected = mstring.Prepare(dt.ToString(formatstr.ToString()));

                if (expected != output)
                {
                    throw new Exception("DbFunctions.FORMAT('dd') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.FORMAT('t')...");

                List<DbValue> args = new List<DbValue>();
                mstring formatstr = mstring.Prepare("t");
                args.Add(tools.AllocValue(formatstr));
                DateTime dt = new DateTime(2000, 9, 14, 12, 0, 0);
                args.Add(tools.AllocValue(dt));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.FORMAT(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);
                mstring expected = mstring.Prepare(dt.ToString(formatstr.ToString()));

                if (expected != output)
                {
                    throw new Exception("DbFunctions.FORMAT('t') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
