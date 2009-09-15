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
        public static void DbFunctions_ISNULL()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            {
                Console.WriteLine("Testing DbFunctions.ISNULL(char(n))...");

                {
                    mstring s1 = Utils.GenString(rnd.Next(1, 200));

                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(s1));
                    DbFunctionArguments fargs = new DbFunctionArguments(args);

                    DbValue valOutput = DbFunctions.ISNULL(tools, fargs);
                    ByteSlice bs = valOutput.Eval();
                    bool output = tools.GetInt(bs) != 0;

                    bool expected = false;

                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ISNULL(char(n)='" + s1.ToString() + "') has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }

                {

                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(ByteSlice.Prepare(new byte[] { 1 }), DbTypeID.CHARS));
                    DbFunctionArguments fargs = new DbFunctionArguments(args);

                    DbValue valOutput = DbFunctions.ISNULL(tools, fargs);
                    ByteSlice bs = valOutput.Eval();
                    bool output = tools.GetInt(bs) != 0;

                    bool expected = true;

                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ISNULL(char(n)=NULL) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }

            }

            {
                Console.WriteLine("Testing DbFunctions.ISNULL(Int32)...");

                {
                    int x = rnd.Next(Int32.MinValue, Int32.MaxValue);

                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(x));
                    DbFunctionArguments fargs = new DbFunctionArguments(args);

                    DbValue valOutput = DbFunctions.ISNULL(tools, fargs);
                    ByteSlice bs = valOutput.Eval();
                    bool output = tools.GetInt(bs) != 0;

                    bool expected = false;

                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ISNULL(Int32=" + x.ToString() + ") has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }

                {

                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(ByteSlice.Prepare(new byte[] { 1 }), DbTypeID.INT));
                    DbFunctionArguments fargs = new DbFunctionArguments(args);

                    DbValue valOutput = DbFunctions.ISNULL(tools, fargs);
                    ByteSlice bs = valOutput.Eval();
                    bool output = tools.GetInt(bs) != 0;

                    bool expected = true;

                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ISNULL(Int32=NULL) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }

            }

            {
                Console.WriteLine("Testing DbFunctions.ISNULL(Double)...");

                {
                    double x = rnd.NextDouble();

                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(x));
                    DbFunctionArguments fargs = new DbFunctionArguments(args);

                    DbValue valOutput = DbFunctions.ISNULL(tools, fargs);
                    ByteSlice bs = valOutput.Eval();
                    bool output = tools.GetInt(bs) != 0;

                    bool expected = false;

                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ISNULL(Double=" + x.ToString() + ") has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }

                {
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(ByteSlice.Prepare(new byte[] { 1 }), DbTypeID.DOUBLE));
                    DbFunctionArguments fargs = new DbFunctionArguments(args);

                    DbValue valOutput = DbFunctions.ISNULL(tools, fargs);
                    ByteSlice bs = valOutput.Eval();
                    bool output = tools.GetInt(bs) != 0;

                    bool expected = true;

                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ISNULL(Double=NULL) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }

            }

            {
                Console.WriteLine("Testing DbFunctions.ISNULL(Long)...");

                {
                    long x = DateTime.Now.Ticks;

                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(x));
                    DbFunctionArguments fargs = new DbFunctionArguments(args);

                    DbValue valOutput = DbFunctions.ISNULL(tools, fargs);
                    ByteSlice bs = valOutput.Eval();
                    bool output = tools.GetInt(bs) != 0;

                    bool expected = false;

                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ISNULL(Long=" + x.ToString() + ") has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }

                {
                    List<DbValue> args = new List<DbValue>();
                    args.Add(tools.AllocValue(ByteSlice.Prepare(new byte[] { 1 }), DbTypeID.LONG));
                    DbFunctionArguments fargs = new DbFunctionArguments(args);

                    DbValue valOutput = DbFunctions.ISNULL(tools, fargs);
                    ByteSlice bs = valOutput.Eval();
                    bool output = tools.GetInt(bs) != 0;

                    bool expected = true;

                    if (expected != output)
                    {
                        throw new Exception("DbFunctions.ISNULL(Long=NULL) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Expected results received.");
                    }
                }

            }

        }
    }
}
