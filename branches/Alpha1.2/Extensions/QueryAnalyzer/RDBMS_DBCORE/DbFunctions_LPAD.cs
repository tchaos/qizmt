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
        private static DbValue _PAD(string functionName, DbFunctionTools tools, DbFunctionArguments args, bool isLeft)
        {
            args.EnsureCount(functionName, 3);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(functionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
            }
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (Types.IsNullValue(arg1))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg1type.ID != DbTypeID.INT)
            {
                args.Expected(functionName, 0, "input INT, got " + arg1type.Name.ToUpper());
            }
            DbType arg2type;
            ByteSlice arg2 = args[2].Eval(out arg2type);
            if (Types.IsNullValue(arg2))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg2type.ID != DbTypeID.CHARS)
            {
                args.Expected(functionName, 0, "input CHAR(n), got " + arg2type.Name.ToUpper());
            }

            mstring str = tools.GetString(arg0);
            int totallen = tools.GetInt(arg1);
            mstring padstr = tools.GetString(arg2);

            if (str.Length > totallen)
            {
                str = str.SubstringM(0, totallen);
            }
            else if(str.Length < totallen)
            {
                int delta = totallen - str.Length;
                int padlen = padstr.Length;
                mstring newstr = mstring.Prepare();

                for (int remain = delta; remain > 0; )
                {
                    newstr = newstr.AppendM(padstr);
                    remain -= padlen;
                }

                //if we go over, truncate.
                if (newstr.Length > delta)
                {
                    newstr = newstr.SubstringM(0, delta);
                }

                if (isLeft)
                {
                    str = newstr.AppendM(str);
                }
                else
                {
                    str = str.AppendM(newstr);
                }
            }
            return tools.AllocValue(str);            
        }

        public static DbValue LPAD(DbFunctionTools tools, DbFunctionArguments args)
        {
            return _PAD("LPAD", tools, args, true);
        }
    }
}

