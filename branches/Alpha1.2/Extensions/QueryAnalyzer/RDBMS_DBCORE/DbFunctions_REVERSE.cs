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

        public static DbValue REVERSE(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "REVERSE";

            args.EnsureCount(FunctionName, 1);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
            }

            mstring x = tools.GetString(arg0);

            mstring r = mstring.Prepare();

            for (int i = x.Length-1; i >= 0; i--)
            {
                r = r.AppendM(x.SubstringM(i, 1));
            }

            return tools.AllocValue(r);
        }
    }
}

