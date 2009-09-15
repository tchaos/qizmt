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
        public static DbValue POWER(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "POWER";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }

            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            double b = 0;
            double p = 0;

            switch (arg0type.ID)
            {
                case DbTypeID.INT:
                    {
                        int x = tools.GetInt(arg0);
                        b = (double)x;                        
                    }
                    break;

                case DbTypeID.LONG:
                    {
                        long x = tools.GetLong(arg0);
                        b = (double)x;
                    }
                    break;

                case DbTypeID.DOUBLE:
                    {
                        b = tools.GetDouble(arg0);
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
                        p = (double)x;
                    }
                    break;

                case DbTypeID.LONG:
                    {
                        long x = tools.GetLong(arg1);
                        p = (double)x;
                    }
                    break;

                case DbTypeID.DOUBLE:
                    {
                        p = tools.GetDouble(arg1);
                    }
                    break;

                default:
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got " + arg1type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }

            double r = Math.Pow(b, p);
            return tools.AllocValue(r);
        }
    }
}

