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
        public static void DbFunctions_NVL()
        {
            DbFunctionTools tools = new DbFunctionTools();

            {
                Console.WriteLine("Testing DbFunctions.NVL(DateTime)...");

                List<DbValue> args = new List<DbValue>();
                DateTime dt = DateTime.Parse("12/1/2000 10:00:00 AM");
                args.Add(tools.AllocValue(dt));
                args.Add(tools.AllocValue(DateTime.Parse("12/12/2000 10:00:00 AM")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NVL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);

                DateTime expected = DateTime.Parse("12/1/2000 10:00:00 AM");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NVL(DateTime) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NVL(DateTime)...");

                List<DbValue> args = new List<DbValue>();

                byte[] buf = new byte[9];
                buf[0] = 1; //is null
                args.Add(tools.AllocValue(ByteSlice.Prepare(buf), DbType.Prepare("DateTime", 9)));
                args.Add(tools.AllocValue(DateTime.Parse("1/1/2009 10:00:00 AM")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);
                DbValue valOutput = DbFunctions.NVL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);

                DateTime expected = DateTime.Parse("1/1/2009 10:00:00 AM");

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NVL(DateTime) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
