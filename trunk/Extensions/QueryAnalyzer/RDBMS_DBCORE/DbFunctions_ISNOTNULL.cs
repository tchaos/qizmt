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
        public static DbValue ISNOTNULL(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "ISNOTNULL";

            args.EnsureCount(FunctionName, 1);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            bool bresult = !Types.IsNullValue(arg0);
            return tools.AllocValue(bresult ? 1 : 0);
        }
    }
}

