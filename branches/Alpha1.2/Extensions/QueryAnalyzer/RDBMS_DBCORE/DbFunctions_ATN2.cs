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
        public static DbValue ATN2(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "ATN2";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);

            if (Types.IsNullValue(arg0) || Types.IsNullValue(arg1))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }

            double d1 = 0;
            double d2 = 0;
            switch (arg0type.ID)
            {
                case DbTypeID.INT:
                    {
                        int x = tools.GetInt(arg0);
                        d1 = (double)x;
                    }
                    break;

                case DbTypeID.LONG:
                    {
                        long x = tools.GetLong(arg0);
                        d1 = (double)x;
                    }
                    break;

                case DbTypeID.DOUBLE:
                    {
                        d1 = tools.GetDouble(arg0);
                    }
                    break;

                default:
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got " + arg0type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }
            switch (arg1type.ID)
            {
                case DbTypeID.INT:
                    {
                        int x = tools.GetInt(arg1);
                        d2 = (double)x;
                    }
                    break;

                case DbTypeID.LONG:
                    {
                        long x = tools.GetLong(arg1);
                        d2 = (double)x;
                    }
                    break;

                case DbTypeID.DOUBLE:
                    {
                        d2 = tools.GetDouble(arg1);
                    }
                    break;

                default:
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got " + arg0type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }

            d1 = Math.Atan2(d1, d2);
            return tools.AllocValue(d1);
        }
    }
}

