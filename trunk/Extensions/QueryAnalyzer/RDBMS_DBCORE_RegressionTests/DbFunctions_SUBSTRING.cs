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
        public static void DbFunctions_SUBSTRING()
        {
            DbFunctionTools tools = new DbFunctionTools();

            //String,Int32.
            {
                Console.WriteLine("Testing DbFunctions.SUBSTRING(String, int, int)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("HELLO WORLD")));
                int si = 6;
                int len = 5;
                args.Add(tools.AllocValue(si));
                args.Add(tools.AllocValue(len));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.SUBSTRING(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                mstring expected = mstring.Prepare("WORLD");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.SUBSTRING(String, int, int) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
