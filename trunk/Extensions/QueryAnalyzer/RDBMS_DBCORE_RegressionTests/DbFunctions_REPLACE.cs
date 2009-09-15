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
        public static void DbFunctions_REPLACE()
        {
            DbFunctionTools tools = new DbFunctionTools();
            {
                Console.WriteLine("Testing DbFunctions.REPLACE(String,String,String)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("apple is red")));
                args.Add(tools.AllocValue(mstring.Prepare("red")));
                args.Add(tools.AllocValue(mstring.Prepare("yellow")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.REPLACE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("apple is yellow");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.REPLACE(String,String,String) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.REPLACE(String,String,String)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("apple is red")));
                args.Add(tools.AllocValue(mstring.Prepare("black")));
                args.Add(tools.AllocValue(mstring.Prepare("yellow")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.REPLACE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("apple is red");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.REPLACE(String,String,String) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
