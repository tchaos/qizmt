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
        public static void DbFunctions_FLOOR()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            //Double.
            {
                Console.WriteLine("Testing DbFunctions.FLOOR(Double)...");

                double input = rnd.NextDouble();

                if (input < 0.5)
                {
                    input = input * -1d;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.FLOOR(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = Math.Floor(input);

                if (expected != output)
                {
                    throw new Exception("DbFunctions.FLOOR(Double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
