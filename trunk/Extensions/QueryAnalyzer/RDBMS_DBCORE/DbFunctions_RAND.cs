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
        public static DbValue RAND(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "RAND";
            
            Random rnd = null;
            
            if (args.Length == 1)
            {
                DbType arg0type;
                ByteSlice arg0 = args[0].Eval(out arg0type);
                if (Types.IsNullValue(arg0))
                {
                    rnd = new Random();
                }
                else
                {
                    if (arg0type.ID != DbTypeID.INT)
                    {
                        args.Expected(FunctionName, 0, "input INT, got " + arg0type.Name.ToUpper());
                    }

                    int seed = tools.GetInt(arg0);
                    rnd = new Random(seed);
                }
            }
            else
            {
                rnd = new Random();
            }
           
            return tools.AllocValue(rnd.NextDouble());
        }
    }
}

