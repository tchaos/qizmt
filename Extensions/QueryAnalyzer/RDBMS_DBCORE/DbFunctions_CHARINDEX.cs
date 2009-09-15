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

        public static DbValue CHARINDEX(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "CHARINDEX";

            args.EnsureMinCount(FunctionName, 2);

            int index = -1;
            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (Types.IsNullValue(arg0) || Types.IsNullValue(arg1))
            {
                return tools.AllocValue(index);
            }

            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
                return null;
            }
            if (arg1type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg1type.Name.ToUpper());
                return null;
            }

            int startIndex = 0;
            if (args.Length > 2)
            {
                DbType arg2type;
                ByteSlice arg2 = args[2].Eval(out arg2type);
                if (arg2type.ID != DbTypeID.INT)
                {
                    args.Expected(FunctionName, 0, "input INT, got " + arg2type.Name.ToUpper());
                    return null;
                }
                startIndex = tools.GetInt(arg2);
                if (startIndex < 0)
                {
                    startIndex = 0;
                }
            }
            
            mstring word = tools.GetString(arg0);
            mstring sentence = tools.GetString(arg1);

            if (startIndex > sentence.Length - 1)
            {
                index = -1;
            }
            else if (startIndex == 0)
            {
                index = sentence.IndexOf(word);
            }
            else
            {
                mstring partsentence = sentence.SubstringM(startIndex);
                int ix = partsentence.IndexOf(word);
                if (ix == -1)
                {
                    index = -1;
                }
                else
                {
                    index = startIndex + ix;
                }
            }
            return tools.AllocValue(index);
        }
    }
}

