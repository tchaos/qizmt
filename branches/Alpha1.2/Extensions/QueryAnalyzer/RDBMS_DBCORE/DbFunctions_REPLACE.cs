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

        public static DbValue REPLACE(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "REPLACE";

            args.EnsureCount(FunctionName, 3);

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
            DbType arg2type;
            ByteSlice arg2 = args[2].Eval(out arg2type);
            if (Types.IsNullValue(arg2))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg2type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg2type.Name.ToUpper());
            }

            mstring sentence = tools.GetString(arg0);
            mstring word = tools.GetString(arg1);
            mstring replacement = tools.GetString(arg2);
            sentence = sentence.ReplaceM(ref word, ref replacement);
            return tools.AllocValue(sentence);
        }
    }
}

