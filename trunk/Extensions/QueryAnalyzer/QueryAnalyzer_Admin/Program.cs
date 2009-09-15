using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_Admin
{
    partial class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                ShowUsage();
                return;
            }

            string action = args[0].ToLower();

            switch (action)
            {
                case "killall":                    
                    KillAll(args);
                    break;

                default:
                    Console.Error.WriteLine("Invalid action");
                    break;
            }

            int k = 10;
        }

        private static void ShowUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("    QueryAnalyzer_Admin <action> [<arguments>]");
            Console.WriteLine("Actions:");
            Console.WriteLine("    killall <hosts>         kill all protocol services");
        }
    }
}
