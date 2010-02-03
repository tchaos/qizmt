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

        public static DbValue CAST(DbFunctionTools tools, DbFunctionArguments args)
        {
#if DEBUG
            //System.Diagnostics.Debugger.Launch();
#endif

            string FunctionName = "CAST";

            args.EnsureCount(FunctionName, 2);

            DbType type;
            ByteSlice bs = args[0].Eval(out type);
            
            mstring xas = tools.GetString(args[1]);
            if (!xas.StartsWith("AS "))
            {
#if DEBUG
                throw new Exception("Expected AS <type> in CAST, not: " + xas);
#endif
                throw new Exception("Expected AS <type> in CAST");
            }
            mstring msastype = xas.SubstringM(3);
            string sastype = msastype.ToString(); // Alloc.
            sastype = DbType.NormalizeName(sastype); // Alloc if char(n).
            DbType astype = DbType.Prepare(sastype); // Alloc if char(n).
            if (DbTypeID.NULL == astype.ID)
            {
                throw new Exception("Unknown AS type in CAST: " + sastype);
            }

            switch (astype.ID)
            {
                case DbTypeID.INT:
                    switch (type.ID)
                    {
                        case DbTypeID.INT: // as INT
                            if (astype.Size > type.Size)
                            {
                                throw new Exception("CAST: source value buffer too small");
                            }
                            return tools.AllocValue(ByteSlice.Prepare(bs, 0, astype.Size), astype);
                        case DbTypeID.LONG: // as INT
                            return tools.AllocValue((int)tools.GetLong(bs));
                        case DbTypeID.DOUBLE: // as INT
                            return tools.AllocValue((int)tools.GetDouble(bs));
                        case DbTypeID.DATETIME: // as INT
                            return tools.AllocValue((int)tools.GetDateTime(bs).Ticks);
                        case DbTypeID.CHARS: // as INT
                            {
                                int to = tools.GetString(bs).NextItemToInt32(' ');
                                return tools.AllocValue(to);
                            }
                        default:
                            throw new Exception("Cannot handle CAST value of type " + type.Name + " AS " + astype.Name);
                    }
                    break;
                case DbTypeID.LONG:
                    switch (type.ID)
                    {
                        case DbTypeID.INT: // as LONG
                            return tools.AllocValue((long)tools.GetInt(bs));
                        case DbTypeID.LONG: // as LONG
                            if (astype.Size > type.Size)
                            {
                                throw new Exception("CAST: source value buffer too small");
                            }
                            return tools.AllocValue(ByteSlice.Prepare(bs, 0, astype.Size), astype);
                        case DbTypeID.DOUBLE: // as LONG
                            return tools.AllocValue((long)tools.GetDouble(bs));
                        case DbTypeID.DATETIME: // as LONG
                            return tools.AllocValue((long)tools.GetDateTime(bs).Ticks);
                        case DbTypeID.CHARS: // as LONG
                            {
                                long to = tools.GetString(bs).NextItemToInt64(' ');
                                return tools.AllocValue(to);
                            }
                        default:
                            throw new Exception("Cannot handle CAST value of type " + type.Name + " AS " + astype.Name);
                    }
                    break;
                case DbTypeID.DOUBLE:
                    switch (type.ID)
                    {
                        case DbTypeID.INT: // as DOUBLE
                            return tools.AllocValue((double)tools.GetInt(bs));
                        case DbTypeID.LONG: // as DOUBLE
                            return tools.AllocValue((double)tools.GetLong(bs));
                        case DbTypeID.DOUBLE: // as DOUBLE
                            if (astype.Size > type.Size)
                            {
                                throw new Exception("CAST: source value buffer too small");
                            }
                            return tools.AllocValue(ByteSlice.Prepare(bs, 0, astype.Size), astype);
                        case DbTypeID.DATETIME: // as DOUBLE
                            return tools.AllocValue((double)tools.GetDateTime(bs).Ticks);
                        case DbTypeID.CHARS: // as DOUBLE
                            {
                                double to = tools.GetString(bs).NextItemToDouble(' ');
                                return tools.AllocValue(to);
                            }
                        default:
                            throw new Exception("Cannot handle CAST value of type " + type.Name + " AS " + astype.Name);
                    }
                    break;
                case DbTypeID.DATETIME:
                    switch (type.ID)
                    {
                        case DbTypeID.INT: // as DATETIME
                            return tools.AllocValue(new DateTime((long)tools.GetInt(bs)));
                        case DbTypeID.LONG: // as DATETIME
                            return tools.AllocValue(new DateTime((long)tools.GetLong(bs)));
                        case DbTypeID.DOUBLE: // as DATETIME
                            return tools.AllocValue(new DateTime((long)tools.GetDouble(bs)));
                        case DbTypeID.DATETIME: // as DATETIME
                            if (astype.Size > type.Size)
                            {
                                throw new Exception("CAST: source value buffer too small");
                            }
                            return tools.AllocValue(ByteSlice.Prepare(bs, 0, astype.Size), astype);
                        case DbTypeID.CHARS: // as DATETIME
                            {
                                mstring to = tools.GetString(bs);
                                return tools.AllocValue(DateTime.Parse(to.ToString()));
                            }
                        default:
                            throw new Exception("Cannot handle CAST value of type " + type.Name + " AS " + astype.Name);
                    }
                    break;
                case DbTypeID.CHARS:
                    switch (type.ID)
                    {
                        case DbTypeID.INT: // as CHAR(n)
                            {
                                mstring ms = mstring.Prepare(tools.GetInt(bs));
                                return tools.AllocValue(ms, astype.Size);
                            }
                        case DbTypeID.LONG: // as CHAR(n)
                            {
                                mstring ms = mstring.Prepare(tools.GetLong(bs));
                                return tools.AllocValue(ms, astype.Size);
                            }
                        case DbTypeID.DOUBLE: // as CHAR(n)
                            {
                                mstring ms = mstring.Prepare(tools.GetDouble(bs));
                                return tools.AllocValue(ms, astype.Size);
                            }
                        case DbTypeID.DATETIME: // as CHAR(n)
                            {
                                mstring ms = mstring.Prepare(tools.GetDateTime(bs).ToString());
                                return tools.AllocValue(ms, astype.Size);
                            }
                        case DbTypeID.CHARS: // as CHAR(n)
                            if (astype.Size > type.Size)
                            {
                                //throw new Exception("CAST: source value buffer too small");
                                return tools.AllocValue(tools.GetString(bs), astype.Size);
                            }
                            return tools.AllocValue(ByteSlice.Prepare(bs, 0, astype.Size), astype);
                        default:
                            throw new Exception("Cannot handle CAST value of type " + type.Name + " AS " + astype.Name);
                    }
                    break;
                default:
                    throw new Exception("Cannot handle CAST value of type " + type.Name + " AS " + astype.Name);
            }

        }
    }
}

