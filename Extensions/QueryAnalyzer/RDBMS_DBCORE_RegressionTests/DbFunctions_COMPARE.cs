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
        public static void DbFunctions_COMPARE()
        {
            DbFunctionTools tools = new DbFunctionTools();
            Random rnd = new Random();

            const int NUM_RUNS_EACH = 4;

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(char(n), char(n)) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                mstring x = Utils.GenString(rnd.Next(1, 200));
                mstring y = Utils.GenString(rnd.Next(1, 200));
                if (1 == it)
                {
                    y = x;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = string.Compare(x.ToString(), y.ToString(), StringComparison.OrdinalIgnoreCase);

                if (!_SameCompareRank(expected, output))
                {
                    if (0 == expected || 0 == output)
                    {
                        throw new Exception("DbFunctions.COMPARE(char(n), char(n)) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                    }
                    Console.WriteLine("Warning: C# StringComparison.OrdinalIgnoreCase and DbFunctions.COMPARE do not agree on string ordering: C# says " + _CompareRankToString(expected) + " but I say " + _CompareRankToString(output) + " for:\r\n\t'{0}'\r\n\t'{1}'", x.ToString(), y.ToString());
                    System.Threading.Thread.Sleep(1000 * 3);
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(Int32, Int32) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                int x = rnd.Next(Int32.MinValue, Int32.MaxValue);
                int y = rnd.Next(Int32.MinValue, Int32.MaxValue);
                if (1 == it)
                {
                    y = x;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = x.CompareTo(y);

                if (!_SameCompareRank(expected, output))
                {
                    throw new Exception("DbFunctions.COMPARE(Int32, Int32) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(Double, Double) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                double x = rnd.NextDouble();
                double y = rnd.NextDouble();
                if (1 == it)
                {
                    y = x;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = x.CompareTo(y);

                if (!_SameCompareRank(expected, output))
                {
                    throw new Exception("DbFunctions.COMPARE(Double, Double) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(Long, Long) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                long x = unchecked((long)rnd.Next() * (long)rnd.Next());
                long y = unchecked((long)rnd.Next() * (long)rnd.Next());
                if (1 == it)
                {
                    y = x;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = x.CompareTo(y);

                if (!_SameCompareRank(expected, output))
                {
                    throw new Exception("DbFunctions.COMPARE(Long, Long) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(Double, Int32) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                double x = rnd.NextDouble();
                int y = rnd.Next();
                if (1 == it)
                {
                    //y = x;
                    x = y;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = x.CompareTo((double)y);

                if (!_SameCompareRank(expected, output))
                {
                    throw new Exception("DbFunctions.COMPARE(Double, Int32) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(Double, Long) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                double x = rnd.NextDouble();
                long y = unchecked((long)rnd.Next() * (long)rnd.Next());
                if (1 == it)
                {
                    //y = x;
                    x = y;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = x.CompareTo((double)y);

                if (!_SameCompareRank(expected, output))
                {
                    throw new Exception("DbFunctions.COMPARE(Double, Long) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(DateTime, DateTime) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                DateTime x = DateTime.Now;
                DateTime y = x.AddDays(rnd.Next(-365, 365));
                if (1 == it)
                {
                    y = x;
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = x.CompareTo(y);

                if (!_SameCompareRank(expected, output))
                {
                    throw new Exception("DbFunctions.COMPARE(DateTime, DateTime) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(DateTime, Char(n)) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                DateTime x = DateTime.Now;
                DateTime dt = x.AddDays(rnd.Next(-365, 365));
                mstring y = mstring.Prepare(dt.ToString());

                if (1 == it)
                {
                    y = mstring.Prepare(x.ToString());
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = x.CompareTo(DateTime.Parse(y.ToString()));

                if (!_SameCompareRank(expected, output))
                {
                    throw new Exception("DbFunctions.COMPARE(DateTime, Char(n)) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }

            for (int it = 0; it < NUM_RUNS_EACH; it++)
            {
                Console.WriteLine("Testing DbFunctions.COMPARE(Char(n), DateTime) ({0} of {1})...", it + 1, NUM_RUNS_EACH);

                DateTime y = DateTime.Now;
                DateTime dt = y.AddDays(rnd.Next(-365, 365));
                mstring x = mstring.Prepare(dt.ToString());

                if (1 == it)
                {
                    x = mstring.Prepare(y.ToString());
                }

                List<DbValue> args = new List<DbValue>();
                args.Add(tools.AllocValue(x));
                args.Add(tools.AllocValue(y));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue valOutput = DbFunctions.COMPARE(tools, fargs);
                ByteSlice bs = valOutput.Eval();
                int output = tools.GetInt(bs);

                int expected = DateTime.Parse(x.ToString()).CompareTo(y);

                if (!_SameCompareRank(expected, output))
                {
                    throw new Exception("DbFunctions.COMPARE(Char(n), DateTime) has failed.  Expected result: " + _CompareRankToString(expected) + ", but received: " + _CompareRankToString(output));
                }
                else
                {
                    Console.WriteLine("Expected results received: {0}", _CompareRankToString(expected));
                }
            }
        }

        static bool _SameCompareRank(int a, int b)
        {
            return false
                || (a > 0 && b > 0)
                || (a < 0 && b < 0)
                || (a == 0 && b == 0)
                ;
        }

        static string _CompareRankToString(int a)
        {
            if (a < 0)
            {
                return "<0";
            }
            if (a > 0)
            {
                return ">0";
            }
            return "=0";
        }

    }
}
