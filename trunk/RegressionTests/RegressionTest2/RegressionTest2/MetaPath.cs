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

        static void MetaPath(string[] args)
        {

            {
                string thisservicedir = Surrogate.FetchServiceNetworkPath(System.Net.Dns.GetHostName());
                string master = Surrogate.LocateMasterHost(thisservicedir);
                Surrogate.SetNewMasterHost(master);
                Surrogate.SetNewMetaLocation(thisservicedir);
            }
            // NetworkPathForHost works here due to the above.
            string internalpath = Surrogate.NetworkPathForHost(Surrogate.MasterHost) + @"\" + dfs.DFSXMLNAME;
            Console.WriteLine("Internal DFS.xml path: {0}", internalpath);

            string dfsxmlpath;
            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                //throw new Exception("Expected path to DFS.xml");
                dfsxmlpath = null;
            }
            else
            {
                dfsxmlpath = args[1];
                Console.WriteLine("Command-line path: {0}", dfsxmlpath);
            }

            string metapath = Exec.Shell("Qizmt metapath").Trim();
            Console.WriteLine("Qizmt metapath path: {0}", metapath);

            Console.WriteLine("Comparing...");
            if (null != dfsxmlpath)
            {
                if (!System.IO.File.Exists(dfsxmlpath))
                {
                    throw new Exception("Command-line path does not exist: " + dfsxmlpath);
                }
                if (!System.IO.File.Exists(metapath))
                {
                    throw new Exception("metapath path does not exist: " + metapath);
                }
                if (System.IO.File.ReadAllText(dfsxmlpath)
                    != System.IO.File.ReadAllText(metapath))
                {
                    throw new Exception("metapath failure: command-line and metapath are not the same");
                }
            }
            if (!System.IO.File.Exists(internalpath))
            {
                throw new Exception("Internal path does not exist: " + internalpath);
            }
            if (!System.IO.File.Exists(metapath))
            {
                throw new Exception("metapath path does not exist: " + metapath);
            }
            if (System.IO.File.ReadAllText(internalpath)
                != System.IO.File.ReadAllText(metapath))
            {
                throw new Exception("metapath failure: internal path and metapath are not the same");
            }

            Console.WriteLine("[PASSED] - " + string.Join(" ", args));

        }


    }
}
