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
        public static void DbFunctions_NVL2()
        {
            DbFunctionTools tools = new DbFunctionTools();

            {
                Console.WriteLine("Testing DbFunctions.NVL2(DateTime)...");

                List<DbValue> args = new List<DbValue>();
                DateTime dt = DateTime.Parse("12/1/2000 10:00:00 AM");
                DateTime ifnull = DateTime.Parse("12/11/2000 10:00:00 AM");
                DateTime notnull = DateTime.Parse("12/14/2000 10:00:00 AM");
                args.Add(tools.AllocValue(dt));
                args.Add(tools.AllocValue(notnull));
                args.Add(tools.AllocValue(ifnull));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.NVL2(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);

                DateTime expected = notnull;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.NVL2(DateTime) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.NVL2(DateTime)...");

                List<DbValue> args = new List<DbValue>();

                byte[] buf = new byte[9];
                buf[0] = 1; //is null
                DateTime ifnull = DateTime.Parse("12/11/2000 10:00:00 AM");
                DateTime notnull = DateTime.Parse("12/14/2000 10:00:00 AM");
                args.Add(tools.AllocValue(ByteSlice.Prepare(buf), DbType.Prepare("DateTime", 9)));
                args.Add(tools.AllocValue(notnull));
                args.Add(tools.AllocValue(ifnull));
                DbFunctionArguments fargs = new DbFunctionArguments(args);
                DbValue valOutput = DbFunctions.NVL2(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                DateTime output = tools.GetDateTime(bs);

                DateTime expected = ifnull;
                if (expected != output)
                {
                    throw new Exception("DbFunctions.NVL2(DateTime) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
        }
    }
}
