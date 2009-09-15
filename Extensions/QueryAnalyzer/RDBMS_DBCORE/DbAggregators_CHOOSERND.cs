using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{

    // Important: only call Eval* ONCE per DbValue, or it will re-evaluate.
    public partial class DbAggregators
    {
        private static int anyRnd = -1;
        public static DbValue CHOOSERND(DbFunctionTools tools, DbAggregatorArguments args)
        {
            string AggregatorName = "CHOOSERND";

            if (args.Length == 0)
            {
                return tools.AllocNullValue();
            }
            if (anyRnd == -1)
            {
                Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
                anyRnd = rnd.Next();
            }
            int index = anyRnd % args.Length;
            args[index].EnsureCount(AggregatorName, 1);
            return args[index][0];
        }
    }
}

