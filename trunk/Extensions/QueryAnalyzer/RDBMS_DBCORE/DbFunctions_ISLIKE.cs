using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{

    // Important: only call Eval* ONCE per DbValue, or it will re-evaluate.
    public partial class DbFunctions
    {

        public static DbValue ISLIKE(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "ISLIKE";

            args.EnsureCount(FunctionName, 2);

            int ismatch = 0;
            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocValue(ismatch);
            }
            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
            }

            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (arg1type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 1, "pattern CHAR(n), got " + arg1type.Name.ToUpper());
            }

            string x = tools.GetString(arg0).ToString();
            string y = tools.GetString(arg1).ToString();
            string pattern = GetRegexString(y);            

            if (pattern != null)
            {
                System.Text.RegularExpressions.Regex regx = new System.Text.RegularExpressions.Regex(pattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);                
                if(regx.IsMatch(x))
                {
                    ismatch = 1;
                }
            }

            return tools.AllocValue(ismatch);
        }

        internal static int FindClosingBracket(string str, int startIndex)
        {
            for (int i = startIndex; i < str.Length; i++)
            {
                if (str[i] == ']')
                {
                    if (i == startIndex || str[i - 1] != '\\')
                    {
                        return i;
                    }
                }
            }
            return -1;
        }

        internal static string GetRegexString(string sqlpattern)
        {
            string rpattern = "";
            int prevpos = 0;
            int curpos = 0;
            for (; curpos < sqlpattern.Length; )
            {
                if (sqlpattern[curpos] == '[')
                {
                    rpattern += System.Text.RegularExpressions.Regex.Escape(sqlpattern.Substring(prevpos, curpos - prevpos)).Replace("%", ".*").Replace('_', '.');

                    int closingBracket = FindClosingBracket(sqlpattern, curpos + 1);
                    if (closingBracket == -1)
                    {
                        return null;
                    }

                    rpattern += "[" + sqlpattern.Substring(curpos + 1, closingBracket - curpos - 1).Replace("[", @"\[") + "]";
                    prevpos = closingBracket + 1;
                    curpos = prevpos;
                }
                else
                {
                    curpos++;
                }
            }
            if (prevpos < sqlpattern.Length)
            {
                rpattern += System.Text.RegularExpressions.Regex.Escape(sqlpattern.Substring(prevpos, curpos - prevpos)).Replace("%", ".*").Replace('_', '.');
            }
            return rpattern;
        }
    }

}

