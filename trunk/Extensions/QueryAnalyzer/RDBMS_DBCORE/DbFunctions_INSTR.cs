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

        public static DbValue INSTR(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "INSTR";

            args.EnsureMinCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
            }

            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (Types.IsNullValue(arg1))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg1type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg1type.Name.ToUpper());
            }

            int startIndex = 0;
            if (args.Length > 2)
            {
                DbType arg2type;
                ByteSlice arg2 = args[2].Eval(out arg2type);
                if (arg2type.ID != DbTypeID.INT)
                {
                    args.Expected(FunctionName, 0, "input INT, got " + arg2type.Name.ToUpper());
                }
                if (!Types.IsNullValue(arg2))
                {
                    startIndex = tools.GetInt(arg2);
                }
            }
            
            mstring sentence = tools.GetString(arg0);
            mstring word = tools.GetString(arg1);
            int index = -1;
            if (startIndex < sentence.Length)
            {
                if (startIndex > 0)
                {
                    sentence = sentence.SubstringM(startIndex);
                }
                index = sentence.IndexOf(word);
                if (index > -1)
                {
                    index += startIndex;
                }
            }
            return tools.AllocValue(index);
        }
    }
}

