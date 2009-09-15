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

        public static DbValue LOWER(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "LOWER";

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
            mstring lower = x.ToLowerM();
            return tools.AllocValue(lower);
        }
    }
}

