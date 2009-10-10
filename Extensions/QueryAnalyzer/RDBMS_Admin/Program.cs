using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    public partial class Program
    {
        static void Main(string[] args)
        {

            string action = "";
            if (args.Length > 0)
            {
                action = args[0].ToLower();
            }

            switch (action)
            {
                case "regressiontest":
                case "regressiontests":
                    RunRegressionTests(args);
                    break;

                default:
                    Console.Error.WriteLine("Not valid: RDBMS_Admin {0}", action);
                    break;
            }

        }
    }
}
