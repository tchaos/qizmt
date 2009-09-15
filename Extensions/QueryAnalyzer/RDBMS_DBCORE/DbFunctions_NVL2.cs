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
        public static DbValue NVL2(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "NVL2";

            args.EnsureCount(FunctionName, 3);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            DbType arg2type;
            ByteSlice arg2 = args[2].Eval(out arg2type);

            if (arg0type.ID != arg1type.ID)
            {
                args.Expected(FunctionName, 0, "input " + arg0type.Name.ToString() + ", got " + arg1type.Name.ToUpper());
            }
            if (arg0type.ID != arg2type.ID)
            {
                args.Expected(FunctionName, 0, "input " + arg0type.Name.ToString() + ", got " + arg2type.Name.ToUpper());
            }

            if (Types.IsNullValue(arg0))
            {
                return args[2];
            }
            else
            {
                return args[1];
            }
        }
    }
}

