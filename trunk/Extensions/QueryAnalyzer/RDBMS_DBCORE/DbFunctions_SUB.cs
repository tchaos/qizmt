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

        public static DbValue SUB(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "SUB";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocValue(arg0, arg0type); // Give a null, take a null.
            }
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (Types.IsNullValue(arg1))
            {
                return tools.AllocValue(arg1, arg1type); // Give a null, take a null.
            }

            switch (arg0type.ID)
            {
                case DbTypeID.INT:
                    {
                        int x = tools.GetInt(arg0);
                        switch (arg1type.ID)
                        {
                            case DbTypeID.INT:
                                {
                                    int y = tools.GetInt(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            case DbTypeID.LONG:
                                {
                                    long y = tools.GetLong(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            case DbTypeID.DOUBLE:
                                {
                                    double y = tools.GetDouble(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            default:
                                args.Expected(FunctionName, 1, "input INT, LONG or DOUBLE, got " + arg1type.Name.ToUpper());
                                return null; // Doesn't reach here.
                        }
                    }
                    break;

                case DbTypeID.LONG:
                    {
                        long x = tools.GetLong(arg0);
                        switch (arg1type.ID)
                        {
                            case DbTypeID.INT:
                                {
                                    int y = tools.GetInt(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            case DbTypeID.LONG:
                                {
                                    long y = tools.GetLong(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            case DbTypeID.DOUBLE:
                                {
                                    double y = tools.GetDouble(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            default:
                                args.Expected(FunctionName, 1, "input INT, LONG or DOUBLE, got " + arg1type.Name.ToUpper());
                                return null; // Doesn't reach here.
                        }
                    }
                    break;

                case DbTypeID.DOUBLE:
                    {
                        double x = tools.GetDouble(arg0);
                        switch (arg1type.ID)
                        {
                            case DbTypeID.INT:
                                {
                                    int y = tools.GetInt(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            case DbTypeID.LONG:
                                {
                                    long y = tools.GetLong(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            case DbTypeID.DOUBLE:
                                {
                                    double y = tools.GetDouble(arg1);
                                    return tools.AllocValue(x - y);
                                }
                                break;
                            default:
                                args.Expected(FunctionName, 1, "input INT, LONG or DOUBLE, got " + arg1type.Name.ToUpper());
                                return null; // Doesn't reach here.
                        }
                    }
                    break;

                default:
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got " + arg0type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }

        }

    }

}

