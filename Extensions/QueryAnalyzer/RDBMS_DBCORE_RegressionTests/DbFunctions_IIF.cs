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
        public static void DbFunctions_IIF()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            // int
            {
                Console.WriteLine("Testing DbFunctions.IIF(1, 'First', 'Second')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(1));
                args.Add(tools.AllocValue(mstring.Prepare("First")));
                args.Add(tools.AllocValue(mstring.Prepare("Second")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.IIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("First");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.IIF(1, 'First', 'Second') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.IIF(0, 'First', 'Second')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(0));
                args.Add(tools.AllocValue(mstring.Prepare("First")));
                args.Add(tools.AllocValue(mstring.Prepare("Second")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.IIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("Second");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.IIF(0, 'First', 'Second') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.IIF(42, 'First', 'Second')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(42));
                args.Add(tools.AllocValue(mstring.Prepare("First")));
                args.Add(tools.AllocValue(mstring.Prepare("Second")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.IIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("First");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.IIF(42, 'First', 'Second') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            // NULL
            {
                Console.WriteLine("Testing DbFunctions.IIF(NULL, 'First', 'Second')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocNullValue());
                args.Add(tools.AllocValue(mstring.Prepare("First")));
                args.Add(tools.AllocValue(mstring.Prepare("Second")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.IIF(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("Second");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.IIF(NULL, 'First', 'Second') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
