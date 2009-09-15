using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_DBCORE
{

    public partial class DbExec
    {

        delegate DbValue DbScalarFunctionDelegate(DbFunctionTools tools, DbFunctionArguments args);
        static Dictionary<string, DbScalarFunctionDelegate> dbscalarfuncs = null;

        delegate DbValue DbAggregatorDelegate(DbFunctionTools tools, DbAggregatorArguments args);
        static Dictionary<string, DbAggregatorDelegate> dbaggregators = null;


        static void EnsureDbFunctions()
        {
            if (null == dbscalarfuncs)
            {
                System.Reflection.MethodInfo[] dbfuncmethods = typeof(DbFunctions).GetMethods(
                    System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                dbscalarfuncs = new Dictionary<string, DbScalarFunctionDelegate>(
                    dbfuncmethods.Length,
                    new _CaseInsensitiveEqualityComparer_7791());
#if DEBUG_WriteDbFunctions
                System.IO.StreamWriter _swdebug = new System.IO.StreamWriter(@"c:\DbFunctions.txt");
#endif
                foreach (System.Reflection.MethodInfo mi in dbfuncmethods)
                {
#if DEBUG_WriteDbFunctions
                    _swdebug.WriteLine(mi.Name);
#endif
                    Delegate dg = Delegate.CreateDelegate(typeof(DbScalarFunctionDelegate), mi);
                    dbscalarfuncs[mi.Name] = (DbScalarFunctionDelegate)dg;
                }
#if DEBUG_WriteDbFunctions
                _swdebug.Close();
#endif
            }
        }

        static void EnsureDbAggregators()
        {
            if (null == dbaggregators)
            {
                System.Reflection.MethodInfo[] dbaggrmethods = typeof(DbAggregators).GetMethods(
                    System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                dbaggregators = new Dictionary<string, DbAggregatorDelegate>(
                    dbaggrmethods.Length,
                    new _CaseInsensitiveEqualityComparer_7791());
#if DEBUG_WriteDbFunctions
                System.IO.StreamWriter _swdebug = new System.IO.StreamWriter(@"c:\DbAggregators.txt");
#endif
                foreach (System.Reflection.MethodInfo mi in dbaggrmethods)
                {
#if DEBUG_WriteDbFunctions
                    _swdebug.WriteLine(mi.Name);
#endif
                    Delegate dg = Delegate.CreateDelegate(typeof(DbAggregatorDelegate), mi);
                    dbaggregators[mi.Name] = (DbAggregatorDelegate)dg;
                }
#if DEBUG_WriteDbFunctions
                _swdebug.Close();
#endif
            }
        }


        // Returns null if no such function.
        public static DbValue TryExecDbScalarFunction(string name, DbFunctionTools tools, DbFunctionArguments args)
        {
            EnsureDbFunctions();

            if (!dbscalarfuncs.ContainsKey(name))
            {
                return null;
            }

            DbScalarFunctionDelegate dbf = dbscalarfuncs[name];
            return dbf(tools, args);

        }

        public static DbValue ExecDbScalarFunction(string name, DbFunctionTools tools, DbFunctionArguments args)
        {
            DbValue result = TryExecDbScalarFunction(name, tools, args);
            if (null == result)
            {
                throw new DbExecException("No such scalar function named '" + name + "'");
            }
            return result;
        }


        // Returns null if no such function.
        public static DbValue TryExecDbAggregator(string name, DbFunctionTools tools, DbAggregatorArguments args)
        {
            EnsureDbAggregators();

            if (!dbaggregators.ContainsKey(name))
            {
                return null;
            }

            DbAggregatorDelegate dbagg = dbaggregators[name];
            return dbagg(tools, args);

        }

        public static DbValue ExecDbAggregator(string name, DbFunctionTools tools, DbAggregatorArguments args)
        {
            DbValue result = TryExecDbAggregator(name, tools, args);
            if (null == result)
            {
                throw new DbExecException("No such aggregate function named '" + name + "'");
            }
            return result;
        }


        public class _CaseInsensitiveEqualityComparer_7791 : IEqualityComparer<string>
        {
            bool IEqualityComparer<string>.Equals(string x, string y)
            {
                return 0 == string.Compare(x, y, StringComparison.OrdinalIgnoreCase);
            }

            int IEqualityComparer<string>.GetHashCode(string obj)
            {
                unchecked
                {
                    int result = 8385;
                    int cnt = obj.Length;
                    for (int i = 0; i < cnt; i++)
                    {
                        result <<= 4;
                        char ch = obj[i];
                        if (ch >= 'A' && ch <= 'Z')
                        {
                            ch = (char)('a' + (ch - 'A'));
                        }
                        result += ch;
                    }
                    return result;
                }
            }
        }

    }

    public class DbExecException : Exception
    {
        public DbExecException(string str, Exception innerException)
            : base(str, innerException)
        {
        }

        public DbExecException(string str)
            : base(str)
        {
        }
    }

}
