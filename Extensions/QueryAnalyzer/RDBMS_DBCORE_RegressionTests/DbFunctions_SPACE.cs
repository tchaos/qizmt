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
        public static void DbFunctions_SPACE()
        {
            DbFunctionTools tools = new DbFunctionTools();

            //String,Int32.
            {
                Console.WriteLine("Testing DbFunctions.SPACE(String)...");

                List<DbValue> args = new List<DbValue>();
                int len = 4;
                args.Add(tools.AllocValue(4));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.SPACE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("    ");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.SPACE(String) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
