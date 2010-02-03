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
        public static void DbFunctions_RAND()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

       
            {
                Console.WriteLine("Testing DbFunctions.RAND(Int32)...");

                int input = rnd.Next(Int32.MinValue, Int32.MaxValue);

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.RAND(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                Console.WriteLine("Expected results received: {0}", output);               
            }

            {
                Console.WriteLine("Testing DbFunctions.RAND()...");

                List<DbValue> args = new List<DbValue>();
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.RAND(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                Console.WriteLine("Expected results received: {0}", output);
            }
        }
    }
}
