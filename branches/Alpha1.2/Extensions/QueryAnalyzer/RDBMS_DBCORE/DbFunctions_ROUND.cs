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
        public static DbValue ROUND(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "ROUND";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.DOUBLE)
            {
                args.Expected(FunctionName, 0, "input DOUBLE, got " + arg0type.Name.ToUpper());
            }

            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (arg1type.ID != DbTypeID.INT)
            {
                args.Expected(FunctionName, 1, "digits INT, got " + arg1type.Name.ToUpper());
            }

            int digits = tools.GetInt(arg1);
            double x = tools.GetDouble(arg0);
            x = Math.Round(x, digits);
            return tools.AllocValue(x);
        }
    }
}

