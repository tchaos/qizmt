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
        public static DbValue NEXT_DAY(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "NEXT_DAY";

            args.EnsureCount(FunctionName, 1);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.DATETIME)
            {
                args.Expected(FunctionName, 0, "input DATETIME, got " + arg0type.Name.ToUpper());
            }

            DateTime dt = tools.GetDateTime(arg0);
            dt = dt.AddDays(1);
            return tools.AllocValue(dt);
        }
    }
}

