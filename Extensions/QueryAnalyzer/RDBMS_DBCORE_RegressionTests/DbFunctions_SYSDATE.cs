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
        public static void DbFunctions_SYSDATE()
        {
            DbFunctionTools tools = new DbFunctionTools();

            //String,Int32.
            {
                Console.WriteLine("Testing DbFunctions.SYSDATE...");

                DbValue valOutput = DbFunctions.SYSDATE(tools, new DbFunctionArguments());
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);

                DateTime expected = DateTime.Now;
                TimeSpan sp = output - expected;

                if (sp.TotalMinutes > 5)
                {
                    throw new Exception("DbFunctions.SYSDATE has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
