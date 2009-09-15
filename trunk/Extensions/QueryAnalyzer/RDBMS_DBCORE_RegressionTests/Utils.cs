using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySpace.DataMining.DistributedObjects;

namespace RDBMS_DBCORE_RegressionTests
{
    public class Utils
    {
        public static mstring GenString(int len)
        {
            Random rnd = new Random();
            string str = "";
            for (int i = 0; i < len; i++)
            {
                int x = rnd.Next(33, 126);
                str += (char)x;
            }
            return mstring.Prepare(str);
        }
    }
}
