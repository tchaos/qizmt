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
        public static void DbFunctions_CHARINDEX()
        {
            DbFunctionTools tools = new DbFunctionTools();

            //String,Int32.
            {
                Console.WriteLine("Testing DbFunctions.CHARINDEX(char(n), char(n)...");

                mstring word = mstring.Prepare("apple");
                mstring sentence = mstring.Prepare("Red is apple");
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(word));
                args.Add(tools.AllocValue(sentence));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.CHARINDEX(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 7;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.CHARINDEX(char(n), char(n) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing DbFunctions.CHARINDEX(char(n), char(n), Int32...");

                mstring word = mstring.Prepare("apple");
                mstring sentence = mstring.Prepare("Red is apple, or more apples.");
                int startIndex = 8;
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(word));
                args.Add(tools.AllocValue(sentence));
                args.Add(tools.AllocValue(startIndex));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.CHARINDEX(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 22;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.CHARINDEX(char(n), char(n), Int32 has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
