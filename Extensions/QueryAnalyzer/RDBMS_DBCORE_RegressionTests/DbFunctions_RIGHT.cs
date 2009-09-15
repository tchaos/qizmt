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
        public static void DbFunctions_RIGHT()
        {
            DbFunctionTools tools = new DbFunctionTools();

            //String,Int32.
            {
                Console.WriteLine("Testing DbFunctions.RIGHT(String,Int32)...");

                mstring input = Utils.GenString(100);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                args.Add(tools.AllocValue(5));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.RIGHT(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                mstring output = tools.GetString(bs);

                string str = input.ToString();
                string expected = str.Substring(str.Length - 5, 5);

                if (expected != output.ToString())
                {
                    throw new Exception("DbFunctions.RIGHT(String,Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
