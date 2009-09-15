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
        public static void DbFunctions_LEFT()
        {
            DbFunctionTools tools = new DbFunctionTools();

            //String,Int32.
            {
                Console.WriteLine("Testing DbFunctions.LEFT(String,Int32)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("hello world")));
                args.Add(tools.AllocValue(3));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.LEFT(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("hel");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.LEFT(String,Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
