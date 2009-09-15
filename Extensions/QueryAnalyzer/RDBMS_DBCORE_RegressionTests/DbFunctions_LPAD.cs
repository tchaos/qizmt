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
        public static void DbFunctions_LPAD()
        {
            DbFunctionTools tools = new DbFunctionTools();
           
            {
                Console.WriteLine("Testing DbFunctions.LPAD(String,Int32, String)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("hello")));
                int len = 11;
                args.Add(tools.AllocValue(len));
                args.Add(tools.AllocValue(mstring.Prepare("xy")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.LPAD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("xyxyxyhello");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.LPAD(String,Int32, String) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.LPAD(String,Int32, String)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("hello")));
                int len = 10;
                args.Add(tools.AllocValue(len));
                args.Add(tools.AllocValue(mstring.Prepare("xy")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.LPAD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("xyxyxhello");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.LPAD(String,Int32, String) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.LPAD(String,Int32, String)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("hello")));
                int len = 5;
                args.Add(tools.AllocValue(len));
                args.Add(tools.AllocValue(mstring.Prepare("xy")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.LPAD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("hello");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.LPAD(String,Int32, String) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.LPAD(String,Int32, String)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("hello")));
                int len = 3;
                args.Add(tools.AllocValue(len));
                args.Add(tools.AllocValue(mstring.Prepare("xy")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.LPAD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("hel");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.LPAD(String,Int32, String) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }


        }
    }
}
