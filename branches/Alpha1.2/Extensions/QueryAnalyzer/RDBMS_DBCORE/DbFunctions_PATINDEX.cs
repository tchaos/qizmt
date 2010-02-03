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

        public static DbValue PATINDEX(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "PATINDEX";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
            }
            if (arg1type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg1type.Name.ToUpper());
            }

            string pat = tools.GetString(arg0).ToString();
            string str = tools.GetString(arg1).ToString();
            pat = pat.Trim('%');

            string rpat = GetRegexString(pat);

            System.Text.RegularExpressions.Regex regx = new System.Text.RegularExpressions.Regex(rpat);
            System.Text.RegularExpressions.Match match = regx.Match(str);
            int indx = -1;
            if (match != null)
            {
                indx = match.Index;
            }

            return tools.AllocValue(indx);
        }
    }
}

