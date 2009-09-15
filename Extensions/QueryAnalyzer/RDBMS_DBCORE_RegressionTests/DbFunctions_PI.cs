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
        public static void DbFunctions_PI()
        {
            DbFunctionTools tools = new DbFunctionTools();
            {
                Console.WriteLine("Testing DbFunctions.PI()...");
                
                DbFunctionArguments fargs = new DbFunctionArguments();

                DbValue valOutput = DbFunctions.PI(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.PI;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.PI() has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
