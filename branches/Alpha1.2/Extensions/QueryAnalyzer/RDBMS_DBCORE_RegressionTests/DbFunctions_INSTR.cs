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
        public static void DbFunctions_INSTR()
        {
            DbFunctionTools tools = new DbFunctionTools();
           
            {
                Console.WriteLine("Testing DbFunctions.SUBSTR(String,String)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("apple is red")));
                args.Add(tools.AllocValue(mstring.Prepare("red")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.INSTR(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 9;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.SUBSTR(String,String) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.SUBSTR(String,String, Int32)...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("apple is red")));
                args.Add(tools.AllocValue(mstring.Prepare("red")));
                int startindex = 2;
                args.Add(tools.AllocValue(startindex));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.INSTR(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 9;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.SUBSTR(String,String, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
