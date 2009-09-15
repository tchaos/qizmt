using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{

    // Important: only call Eval* ONCE per DbValue, or it will re-evaluate.
    public partial class DbAggregators
    {
        public static DbValue SUM(DbFunctionTools tools, DbAggregatorArguments args)
        {
            string AggregatorName = "SUM";

            double sumd = 0;
            int sumi = 0;
            long suml = 0;
            DbType arg0type = DbType.PrepareNull();
            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                args[iarg].EnsureCount(AggregatorName, 1);                
                ByteSlice arg0 = args[iarg][0].Eval(out arg0type);
                if (!Types.IsNullValue(arg0))   //ignore null
                {                    
                    switch (arg0type.ID)
                    {
                        case DbTypeID.INT:
                            sumi += tools.GetInt(arg0);
                            break;

                        case DbTypeID.LONG:
                            suml += tools.GetLong(arg0);
                            break;

                        case DbTypeID.DOUBLE:
                            sumd += tools.GetDouble(arg0);
                            break;

                        default:
                            args[iarg].Expected(AggregatorName, 0, "input INT, LONG or DOUBLE, got " + arg0type.Name.ToUpper());
                            return null; // Doesn't reach here.
                    }
                }
            }

            if (args.Length > 0)
            {
                switch (arg0type.ID)
                {
                    case DbTypeID.INT:
                        return tools.AllocValue(sumi);
                        break;

                    case DbTypeID.LONG:
                        return tools.AllocValue(suml);
                        break;

                    case DbTypeID.DOUBLE:
                        return tools.AllocValue(sumd);
                        break;
                }
            }

            return tools.AllocValue(sumi);
        }
    }
}

