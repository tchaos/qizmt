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
        public static DbValue NVL(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "NVL";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);

            if (arg0type.ID != arg1type.ID)
            {
                args.Expected(FunctionName, 0, "input " + arg0type.Name.ToString() + ", got " + arg1type.Name.ToUpper());
            }
            if (Types.IsNullValue(arg0))
            {
                return args[1];
            }
            else
            {
                return args[0];
            }
        }
    }
}

