
/*
 * Important: Preserve line numbers!
 * Use existing blank lines if needed.
 * Add functions in Program under the last one.
 * */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConsoleApplicationDebug
{

    class Program
    {

        static string[] _opts;
        public static bool HasOpt(string findname)
        {
            foreach (string opt in _opts)
            {
                if (0 == string.Compare(opt, findname, true))
                {
                    return true;
                }
            }
            return false;
        }



        static void Main(string[] args)
        {
            _opts = args;




            Console.WriteLine("*FIRST*");




            for (int i = 0; i < 10; i++)
            {





                Console.WriteLine("*******a" + i);





                int z = addme(10);





                Console.WriteLine("*******b" + i);





            }





            Console.WriteLine("{8339B895-EFA5-476b-865B-6FE4B77268E4}before hey");





            hey();





            Console.WriteLine("{8339B895-EFA5-476b-865B-6FE4B77268E4}after hey");





            if (HasOpt("UserThrow"))
            {





                throw new Exception("Requested thrown exception!");





            }





            if (HasOpt("DivideByZero"))
            {





                int zero = 1;
                zero--;
                int crash = 100 / zero;





            }





            AA zz;





            string goat = "foo bar";





            object o = "baz";





            int ix = 31;





            if (HasOpt("UserBreak"))
            {





                System.Diagnostics.Debugger.Break();





            }





            int j = 41;





            Console.WriteLine("*foo*");





            Console.WriteLine("*done*");





        }






        static int addme(int x)
        {





            int z = 10 + x;





            return z;





        }


        struct AA
        {





            public override string ToString()
            {





                if (HasOpt("StructThrowToString"))
                {





                    object o = null;
                    return o.ToString();





                }





                return "Called AA.ToString()!";





            }





        }







        static void hey()
        {





            Console.WriteLine("x");





            Console.WriteLine("y");





            Console.WriteLine("z"); Console.WriteLine("zz"); Console.WriteLine("zzz");





            for (int i = 0; i < 5; i++)
            {





                int i33 = 33;




            }





        }







    }





}

