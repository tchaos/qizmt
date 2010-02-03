using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

using MySpace.DataMining.AELight;

namespace RegressionTest2
{
    public partial class Program
    {

        static void GlobalCriticalSectionKillall(string[] args)
        {
            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                throw new Exception("Expected path to DFS.xml");
            }
            string dfsxmlpath = args[1];

            {


                Console.WriteLine("Running killall within global critical section...");
                IDisposable idLock = MySpace.DataMining.DistributedObjects.GlobalCriticalSection.GetLock();
                try
                {
                    Console.WriteLine("    Acquired: global critical section");
                    Exec.Shell("qizmt killall -f");
                    Console.WriteLine("    Qizmt killall completed");
                }
                finally
                {
                    try
                    {
                        // Don't care if this fails.
                        idLock.Dispose();
                        Console.WriteLine("    Released: global critical section");
                    }
                    catch
                    {
                        Console.WriteLine("    Global critical section release exception (this is OK)");
                    }
                }

                System.Threading.Thread.Sleep(1000 * 3);

                Console.WriteLine("Re-entering global critical section...");
                using (MySpace.DataMining.DistributedObjects.GlobalCriticalSection.GetLock())
                {
                    Console.WriteLine("    Acquired: global critical section");
                }
                Console.WriteLine("    Released: global critical section");

                Console.WriteLine("[PASSED] - " + string.Join(" ", args));

            }

        }


    }
}
