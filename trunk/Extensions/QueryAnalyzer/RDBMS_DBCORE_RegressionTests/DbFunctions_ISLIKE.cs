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
        public static void DbFunctions_ISLIKE()
        {
            DbFunctionTools tools = new DbFunctionTools();

            {
                Console.WriteLine("Testing positive DbFunctions.ISLIKE(String,'%xxx')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("red apple")));
                args.Add(tools.AllocValue(mstring.Prepare("%apple")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'%xxx') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing case-insensitive positive DbFunctions.ISLIKE(String,'%xxx')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("red apple")));
                args.Add(tools.AllocValue(mstring.Prepare("%Apple")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'%xxx') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'%xxx')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("red aple")));
                args.Add(tools.AllocValue(mstring.Prepare("%apple")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 0;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'%xxx') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing positive DbFunctions.ISLIKE(String,'xx_x')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("apple")));
                args.Add(tools.AllocValue(mstring.Prepare("app_e")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'xx_x') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'xx_x')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("applle")));
                args.Add(tools.AllocValue(mstring.Prepare("app_e")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 0;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'xx_x') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing positive DbFunctions.ISLIKE(String,'xx[a-d]xx')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("comater")));
                args.Add(tools.AllocValue(mstring.Prepare("com[a-d]ter")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'xx[a-d]xx') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'xx[a-d]xx')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("comxter")));
                args.Add(tools.AllocValue(mstring.Prepare("com[a-d]ter")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 0;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'xx[a-d]xx') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing positive DbFunctions.ISLIKE(String,'xx[^a-d]xx')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("comxter")));
                args.Add(tools.AllocValue(mstring.Prepare("com[^a-d]ter")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'xx[a-d]xx') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'xx[^a-d]xx')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("comater")));
                args.Add(tools.AllocValue(mstring.Prepare("com[^a-d]ter")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 0;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'xx[a-d]xx') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            //Using Wildcard Characters As Literals
            {
                Console.WriteLine("Testing positive DbFunctions.ISLIKE(String,'x[%]')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("5%")));
                args.Add(tools.AllocValue(mstring.Prepare("5[%]")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'x[%]') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'x[%]')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("54")));
                args.Add(tools.AllocValue(mstring.Prepare("5[%]")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 0;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'x[%]') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing positive DbFunctions.ISLIKE(String,'[_]n')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("_n")));
                args.Add(tools.AllocValue(mstring.Prepare("[_]n")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'x[%]') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'[_]n')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("n")));
                args.Add(tools.AllocValue(mstring.Prepare("[_]n")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 0;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'[_]n') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing positive DbFunctions.ISLIKE(String,'[-acdf]')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("-")));
                args.Add(tools.AllocValue(mstring.Prepare("[-acdf]")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'[-acdf]') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'[-acdf]')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("b")));
                args.Add(tools.AllocValue(mstring.Prepare("[-acdf]")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 0;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'[-acdf]') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing positive DbFunctions.ISLIKE(String,'[ [ ]')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("[")));
                args.Add(tools.AllocValue(mstring.Prepare("[ [ ]")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'[ [ ]') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'[ [ ]')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("p")));
                args.Add(tools.AllocValue(mstring.Prepare("[ [ ]")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 0;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'[ [ ]') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            {
                Console.WriteLine("Testing negative DbFunctions.ISLIKE(String,'[*]')...");

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(mstring.Prepare("*")));
                args.Add(tools.AllocValue(mstring.Prepare("[*]")));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ISLIKE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = 1;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ISLIKE(String,'[*]') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

        }
    }
}
