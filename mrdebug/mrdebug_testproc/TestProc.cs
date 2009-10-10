using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Configuration;
using System.Text.RegularExpressions;

namespace ConsoleApplication6
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; ; i++)
            {
                Console.WriteLine("*******a" + i);
                int z = addme(10);
                Console.WriteLine("*******b" + i);
            }
        }

        static int addme(int x)
        {
            int z = 10 + x;
            return z;
        }
    }
}
