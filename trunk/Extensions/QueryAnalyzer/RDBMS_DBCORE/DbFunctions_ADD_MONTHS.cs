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
        public static DbValue ADD_MONTHS(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "ADD_MONTHS";

            args.EnsureCount(FunctionName, 2);

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
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (Types.IsNullValue(arg1))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg1type.ID != DbTypeID.INT)
            {
                args.Expected(FunctionName, 0, "input INT, got " + arg1type.Name.ToUpper());
            }

            DateTime dt = tools.GetDateTime(arg0);
            int months = tools.GetInt(arg1);
            dt = dt.AddMonths(months);            
            return tools.AllocValue(dt);
        }
    }
}

