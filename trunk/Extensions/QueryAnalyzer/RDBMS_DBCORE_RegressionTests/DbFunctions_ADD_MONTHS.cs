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
        public static void DbFunctions_ADD_MONTHS()
        {
            DbFunctionTools tools = new DbFunctionTools();

            //String,Int32.
            {
                Console.WriteLine("Testing DbFunctions.ADD_MONTHS...");

                List<DbValue> args = new List<DbValue>();
                DateTime dt = DateTime.Now;
                int months = 9;
                args.Add(tools.AllocValue(dt));
                args.Add(tools.AllocValue(months));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ADD_MONTHS(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);

                DateTime expected = dt.AddMonths(months);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ADD_MONTHS has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
