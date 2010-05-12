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
        public static void DbFunctions_ARITHMETIC()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            //Int32.
            {
                Console.WriteLine("Testing DbFunctions.ADD(Int32, Int32)...");

                int input1 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                int input2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input1 + input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ADD(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.SUB(Int32, Int32)...");

                int input1 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                int input2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.SUB(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input1 - input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.SUB(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.MUL(Int32, Int32)...");

                int input1 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                int input2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MUL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input1 * input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MUL(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.DIV(Int32, Int32)...");

                int input1 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                int input2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DIV(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = input1 / input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DIV(Int32, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            // Int32, Int64
            {
                Console.WriteLine("Testing DbFunctions.ADD(Int32, Int64)...");

                int input1 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                long input2 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                long expected = input1 + input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ADD(Int32, Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            //Int64.
            {
                Console.WriteLine("Testing DbFunctions.ADD(Int64, Int64)...");

                long input1 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                long input2 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                long expected = input1 + input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ADD(Int64, Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.SUB(Int64, Int64)...");

                long input1 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                long input2 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.SUB(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                long expected = input1 - input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.SUB(Int64, Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.MUL(Int64, Int64)...");

                long input1 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                long input2 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MUL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                long expected = input1 * input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MUL(Int64, Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.DIV(Int64, Int64)...");

                long input1 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                long input2 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DIV(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                long expected = input1 / input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DIV(Int64, Int64) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            // Int64, Int32
            {
                Console.WriteLine("Testing DbFunctions.SUB(Int64, Int32)...");

                long input1 = (long)rnd.Next(Int32.MinValue, Int32.MaxValue) * rnd.Next();
                int input2 = rnd.Next(Int32.MinValue, Int32.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.SUB(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                long output = tools.GetLong(bs);

                long expected = input1 - input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.SUB(Int64, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            //double.
            {
                Console.WriteLine("Testing DbFunctions.ADD(double, double)...");

                double input1 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                double input2 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.ADD(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = input1 + input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.ADD(double, double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.SUB(double, double)...");

                double input1 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                double input2 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.SUB(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = input1 - input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.SUB(double, double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.MUL(double, double)...");

                double input1 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                double input2 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MUL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = input1 * input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MUL(double, double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            {
                Console.WriteLine("Testing DbFunctions.DIV(double, double)...");

                double input1 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                double input2 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DIV(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = input1 / input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DIV(double, double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            // double, Int32
            {
                Console.WriteLine("Testing DbFunctions.MUL(double, Int32)...");

                double input1 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                int input2 = rnd.Next(int.MinValue, int.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.MUL(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = input1 * input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.MUL(double, Int32) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }

            // Int64, double
            {
                Console.WriteLine("Testing DbFunctions.DIV(Int64, double)...");

                long input1 = rnd.Next(int.MinValue, int.MaxValue) * rnd.Next();
                double input2 = rnd.NextDouble() * rnd.Next(int.MinValue, int.MaxValue);
                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(input1));
                args.Add(tools.AllocValue(input2));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.DIV(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                double output = tools.GetDouble(bs);

                double expected = input1 / input2;

                if (expected != output)
                {
                    throw new Exception("DbFunctions.DIV(Int64, double) has failed.  Expected result: " + expected.ToString() + ", but received: " + output.ToString());
                }
                else
                {
                    Console.WriteLine("Expected results received.");
                }
            }
            
        }
    }
}
