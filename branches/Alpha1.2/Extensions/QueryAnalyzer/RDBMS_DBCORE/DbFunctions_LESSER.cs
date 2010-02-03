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
        public static DbValue LESSER(DbFunctionTools tools, DbFunctionArguments args)
        {
            DbValue dbvcompare = COMPARE(tools, args);
            DbType typecompare;
            ByteSlice bsresult = dbvcompare.Eval(out typecompare);
            if (DbTypeID.INT != typecompare.ID)
            {
                return tools.AllocValue(bsresult, typecompare);
            }
            int compare = tools.GetInt(bsresult);
            return tools.AllocValue(compare < 0 ? 1 : 0);
        }
    }
}

