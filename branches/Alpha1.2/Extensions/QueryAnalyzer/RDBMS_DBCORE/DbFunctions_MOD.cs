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

        public static DbValue MOD(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "MOD";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);           
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (Types.IsNullValue(arg1))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }

            int i0, i1 = 0;
            long l0, l1 = 0;
            double d0, d1 = 0;

            switch (arg1type.ID)
            {
                case DbTypeID.INT:
                    i1 = tools.GetInt(arg1);
                    break;

                case DbTypeID.LONG:
                    l1 = tools.GetLong(arg1);
                    break;

                case DbTypeID.DOUBLE:
                    d1 = tools.GetDouble(arg1);
                    break;

                default:
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got " + arg1type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }

            if (i1 == 0 && l1 == 0 && d1 == 0)
            {
                args.Expected(FunctionName, 0, "Division by zero");
                return null; 
            }

            switch (arg0type.ID)
            {
                case DbTypeID.INT:
                    {
                        i0 = tools.GetInt(arg0);
                        switch (arg1type.ID)
                        {
                            case DbTypeID.INT:
                                {
                                    int rem = i0 % i1;
                                    return tools.AllocValue(rem);
                                }
                                break;

                            case DbTypeID.LONG:
                                {
                                    long rem = (long)i0 % l1;
                                    return tools.AllocValue(rem);
                                }
                                break;

                            case DbTypeID.DOUBLE:
                                {
                                    double rem = (double)i0 % d1;
                                    return tools.AllocValue(rem);
                                }
                                break;
                        }
                    }   
                    break;

                case DbTypeID.LONG:
                    {
                        l0 = tools.GetLong(arg0);
                        switch (arg1type.ID)
                        {
                            case DbTypeID.INT:
                                {
                                    long rem = l0 % (long)i1;
                                    return tools.AllocValue(rem);
                                }
                                break;

                            case DbTypeID.LONG:
                                {
                                    long rem = l0 % l1;
                                    return tools.AllocValue(rem);
                                }
                                break;

                            case DbTypeID.DOUBLE:
                                {
                                    double rem = (double)l0 % d1;
                                    return tools.AllocValue(rem);
                                }
                                break;
                        }
                    }                   
                    break;

                case DbTypeID.DOUBLE:
                    {
                        d0 = tools.GetDouble(arg0);
                        switch (arg1type.ID)
                        {
                            case DbTypeID.INT:
                                {
                                    double rem = d0 % (double)i1;
                                    return tools.AllocValue(rem);
                                }
                                break;

                            case DbTypeID.LONG:
                                {
                                    double rem = d0 % (double)l1;
                                    return tools.AllocValue(rem);
                                }
                                break;

                            case DbTypeID.DOUBLE:
                                {
                                    double rem = d0 % d1;
                                    return tools.AllocValue(rem);
                                }
                                break;
                        }
                    }
                    
                    break;

                default:
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got " + arg0type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }

            return null; // Doesn't reach here.
        }
    }
}