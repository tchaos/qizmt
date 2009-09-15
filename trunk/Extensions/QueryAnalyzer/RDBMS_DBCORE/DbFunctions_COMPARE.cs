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
        public static DbValue COMPARE(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "COMPARE";

            args.EnsureCount(FunctionName, 2);

            int result;

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);

            if (Types.IsNullValue(arg0))
            {
                result = int.MaxValue;
                return tools.AllocValue(result);
            }
            if (Types.IsNullValue(arg1))
            {
                result = int.MinValue;
                return tools.AllocValue(result);
            }

            int i0 = 0, i1 = 0;
            long l0 = 0, l1 = 0;
            double d0 = 0, d1 = 0;

            switch (arg0type.ID)
            {
                case DbTypeID.INT:
                    i0 = tools.GetInt(arg0);
                    break;

                case DbTypeID.LONG:
                    l0 = tools.GetLong(arg0);
                    break;

                case DbTypeID.DOUBLE:
                    d0 = tools.GetDouble(arg0);
                    break;

                case DbTypeID.CHARS:
                    {
                        if (arg1type.ID != DbTypeID.CHARS && arg1type.ID != DbTypeID.DATETIME)
                        {
                            args.Expected(FunctionName, 0, "input CHAR(n) or DATETIME, got " + arg1type.Name.ToUpper());
                            return null; // Doesn't reach here.
                        }

                        if (arg1type.ID == DbTypeID.CHARS)
                        {
                            for (int i = 1; ; i += 2)
                            {
                                bool b1end = (i + 2 > arg0.Length) || (arg0[i] == 0 && arg0[i + 1] == 0);
                                bool b2end = (i + 2 > arg1.Length) || (arg1[i] == 0 && arg1[i + 1] == 0);
                                if (b1end)
                                {
                                    if (b2end)
                                    {
                                        result = 0;
                                        break;
                                    }
                                    result = -1;
                                    break;
                                }
                                else if (b2end)
                                {
                                    result = 1;
                                    break;
                                }
                                int diff = Types.UTF16BytesToLowerChar(arg0[i], arg0[i + 1])
                                    - Types.UTF16BytesToLowerChar(arg1[i], arg1[i + 1]);
                                if (0 != diff)
                                {
                                    char ch0 = Types.UTF16BytesToChar(arg0[i], arg0[i + 1]);
                                    char ch1 = Types.UTF16BytesToChar(arg1[i], arg1[i + 1]);
                                    if (!Char.IsLetter(ch0) || !Char.IsLetter(ch1))
                                    {
                                        result = ch0 - ch1;
                                    }
                                    result = diff;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            string sdt = tools.GetString(arg0).ToString();
                            DateTime dt0 = DateTime.Parse(sdt);
                            DateTime dt1 = tools.GetDateTime(arg1);
                            result = dt0.CompareTo(dt1);
                                                      
                        }
                        return tools.AllocValue(result);  
                    }
                    break;

                case DbTypeID.DATETIME:
                    {
                        if (arg1type.ID != DbTypeID.CHARS && arg1type.ID != DbTypeID.DATETIME)
                        {
                            args.Expected(FunctionName, 0, "input CHAR(n) or DATETIME, got " + arg1type.Name.ToUpper());
                            return null; // Doesn't reach here.
                        }

                        if (arg1type.ID == DbTypeID.DATETIME)
                        {
                            DateTime dt0 = tools.GetDateTime(arg0);
                            DateTime dt1 = tools.GetDateTime(arg1);
                            result = dt0.CompareTo(dt1);
                        }
                        else
                        {                           
                            DateTime dt0 = tools.GetDateTime(arg0);
                            string sdt = tools.GetString(arg1).ToString();
                            DateTime dt1 = DateTime.Parse(sdt);
                            result = dt0.CompareTo(dt1);
                        }
                        return tools.AllocValue(result);    
                    }
                    break;

                default:
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got1 " + arg0type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }

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
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got2 " + arg1type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }

            switch (arg0type.ID)
            {
                case DbTypeID.INT:
                    switch (arg1type.ID)
                    {
                        case DbTypeID.INT:
                            result = i0.CompareTo(i1);
                            break;
                        case DbTypeID.LONG:
                            result = ((long)i0).CompareTo(l1);
                            break;
                        case DbTypeID.DOUBLE:
                            result = ((double)i0).CompareTo(d1);
                            break;
                        default:
                            return null; // Should never happen.
                    }
                    break;
                case DbTypeID.LONG:
                    switch (arg1type.ID)
                    {
                        case DbTypeID.INT:
                            result = l0.CompareTo((long)i1);
                            break;
                        case DbTypeID.LONG:
                            result = l0.CompareTo(l1);
                            break;
                        case DbTypeID.DOUBLE:
                            result = ((double)l0).CompareTo(d1);
                            break;
                        default:
                            return null; // Should never happen.
                    }
                    break;
                case DbTypeID.DOUBLE:
                    switch (arg1type.ID)
                    {
                        case DbTypeID.INT:
                            result = d0.CompareTo((double)i1);
                            break;
                        case DbTypeID.LONG:
                            result = d0.CompareTo((double)l1);
                            break;
                        case DbTypeID.DOUBLE:
                            result = d0.CompareTo(d1);
                            break;
                        default:
                            return null; // Should never happen.
                    }
                    break;
                default:
                    return null; // Should never happen.
            }

            return tools.AllocValue(result);
        }
    }
}

