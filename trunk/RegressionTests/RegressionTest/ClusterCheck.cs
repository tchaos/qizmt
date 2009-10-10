using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public class ClusterCheck
    {
        public static void TestClusterCheck(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Error: ClusterCheck command needs argument: <dfsXmlPath>");
                return;
            }

            string dfspath = args[1];

            if (!dfspath.StartsWith(@"\\"))
            {
                Console.Error.WriteLine("Argument: <dfsXmlPath> must be a network path");
                return;
            }

            bool verbose = false;
            if (args.Length > 2 && string.Compare(args[2], "verbose", true) == 0)
            {
                verbose = true;
            }

#if DEBUG
             verbose = true;
#endif

            string surrogate = dfspath.Substring(2, dfspath.IndexOf(@"\", 2) - 2).ToUpper();
            int si = 2 + surrogate.Length + 1;
            string dir = dfspath.Substring(si, dfspath.LastIndexOf(@"\") - si);

            System.Xml.XmlDocument dfs = new System.Xml.XmlDocument();
            string slavelist = null;
            bool cont = false;

            try
            {
                dfs.Load(dfspath);
                System.Xml.XmlNode node = dfs.SelectSingleNode("//SlaveList");
                if (node == null)
                {
                    Console.Error.WriteLine("SlaveList node is not found in dfs.xml");
                    return;
                }

                slavelist = node.InnerText.ToUpper();
                string[] parts = slavelist.Split(new char[] { ',', ';' });
                if (parts.Length < 2)
                {
                    Console.Error.WriteLine("Must have at least 2 hosts in SlaveList tag in dfs.xml");
                    return;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error loading dfs.xml: {0}", e.Message);
                return;
            }

            string dfsback = null;
            if (!DFSUtils.MakeFileBackup(dfspath, ref dfsback))
            {
                Console.Error.WriteLine("Error backing up dfs.xml");
                return;
            }

            string allhosts = slavelist;
            if (slavelist.IndexOf(surrogate) == -1)
            {
                allhosts += ";" + surrogate;
            }

            string exe = Exec.GetQizmtExe();

            //Participating surrogate.
            try
            {
                string sl = slavelist;
                if (sl.IndexOf(surrogate) == -1)
                {
                    sl += ";" + surrogate;
                    DFSUtils.ChangeDFSXMLSlaveList(dfs, dfspath, sl);
                }

                string result = Exec.Shell(exe + " clustercheck " + allhosts);

                if (verbose)
                {
                    Console.Write(result);
                }
                Console.WriteLine();
                Console.WriteLine("-");
                Console.WriteLine("Test case: Participating surrogate:");

                string expResult = @"Participating surrogate: \\" + surrogate.ToUpper();
                if (result.IndexOf(expResult) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error during participating surrogate test case: {0}", e.Message);
            }
            finally
            {
                cont = DFSUtils.UndoFileChanges(dfspath, dfsback);
            }

            if (!cont)
            {
                return;
            }

            //Non-particpating surrogate.
            try
            {
                if (slavelist.IndexOf(surrogate) > -1)
                {
                    string[] parts = slavelist.Split(new char[] { ',', ';' });
                    string sl = "";
                    foreach (string p in parts)
                    {
                        if (string.Compare(p, surrogate, true) != 0)
                        {
                            sl += ";" + p;
                        }
                    }
                    sl = sl.Trim(';');
                    DFSUtils.ChangeDFSXMLSlaveList(dfs, dfspath, sl);
                }

                string result = Exec.Shell(exe + " clustercheck " + allhosts);

                if (verbose)
                {
                    Console.Write(result);
                }
                Console.WriteLine();
                Console.WriteLine("-");
                Console.WriteLine("Test case: Non-participating surrogate:");

                string expResult = @"Non-participating surrogate: \\" + surrogate.ToUpper();
                if (result.IndexOf(expResult) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error during non-participating surrogate test case: {0}", e.Message);
            }
            finally
            {
                cont = DFSUtils.UndoFileChanges(dfspath, dfsback);
            }

            if (!cont)
            {
                return;
            }

            //Inaccessible host.
            try
            {
                string sl = allhosts + ";" + "BOGUSHOSTNAME";
                DFSUtils.ChangeDFSXMLSlaveList(dfs, dfspath, sl);

                string result = Exec.Shell(exe + " clustercheck " + sl);

                if (verbose)
                {
                    Console.Write(result);
                }
                Console.WriteLine();
                Console.WriteLine("-");
                Console.WriteLine("Test case: Inaccessible host:");

                string expResult = @"Inaccessible host: \\BOGUSHOSTNAME";
                if (result.IndexOf(expResult) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error during inaccessible host test case: {0}", e.Message);
            }
            finally
            {
                cont = DFSUtils.UndoFileChanges(dfspath, dfsback);
            }

            if (!cont)
            {
                return;
            }

            //Bad meta data backup.
            try
            {                
                {                    
                    System.Xml.XmlDocument thisdoc = new System.Xml.XmlDocument();
                    thisdoc.Load(dfspath);
                    System.Xml.XmlNode node = thisdoc.SelectSingleNode("//MetaBackup");
                    if (node == null)
                    {
                        node = thisdoc.CreateElement("MetaBackup");
                        thisdoc.DocumentElement.AppendChild(node);
                    }
                    node.InnerText = @"\\" + surrogate + @"\c$\" + Guid.NewGuid().ToString();
                    thisdoc.Save(dfspath);
                }

                string result = Exec.Shell(exe + " clustercheck " + allhosts);

                if (verbose)
                {
                    Console.Write(result);
                }
                Console.WriteLine();
                Console.WriteLine("-");
                Console.WriteLine("Test case: Bad meta data backup:");

                string expResult = @"Bad meta backup surrogate";
                if (result.IndexOf(expResult) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error during bad meta data backup test case: {0}", e.Message);
            }
            finally
            {
                cont = DFSUtils.UndoFileChanges(dfspath, dfsback);
            }

            if (!cont)
            {
                return;
            }

            //Broken surrogate.
            string slavedat = "";
            string slavedatback = "";
            string slave = "";
            {
                string[] parts = slavelist.Split(new char[] { ',', ';' });
                foreach (string p in parts)
                {
                    if (string.Compare(p, surrogate, true) != 0)
                    {
                        slave = p;
                        break;
                    }
                }

                if (slave == "")
                {
                    Console.Error.WriteLine("Cannot perform broken surrogate, uninstalled host, or orphaned worker tests: must have a host in the cluster besides the surrogate itself.");
                    return;
                }

                slavedat = @"\\" + slave + @"\" + dir + @"\slave.dat";

                if (!DFSUtils.MakeFileBackup(slavedat, ref slavedatback))
                {
                    Console.Error.WriteLine("Cannot perform broken surrogate, uninstalled host, or orphaned worker tests.  Error while backing up slave.dat: {0}", slavedat);
                    return;
                }
            }

            try
            {
                System.IO.File.WriteAllText(slavedat, "master=BOGUSHOSTNAME");
                string result = Exec.Shell(exe + " clustercheck " + allhosts);

                if (verbose)
                {
                    Console.Write(result);
                }
                Console.WriteLine();
                Console.WriteLine("-");
                Console.WriteLine("Test case: Broken surrogate:");

                string expResult = @"Broken surrogate: \\" + surrogate;
                if (result.IndexOf(expResult) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error during broken surrogate test case: {0}", e.Message);
            }
            finally
            {
                cont = DFSUtils.UndoFileChanges(slavedat, slavedatback);
            }

            if (!cont)
            {
                return;
            }

            //Uninstalled host.
            try
            {
                System.IO.File.Delete(slavedat);
                string result = Exec.Shell(exe + " clustercheck " + allhosts);

                if (verbose)
                {
                    Console.Write(result);
                }
                Console.WriteLine();
                Console.WriteLine("-");
                Console.WriteLine("Test case: Uninstalled host:");

                string expResult = @"Uninstalled host: \\" + slave;
                if (result.IndexOf(expResult) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error during uninstalled host test case: {0}", e.Message);
            }
            finally
            {
                cont = DFSUtils.UndoFileChanges(slavedat, slavedatback);
            }

            if (!cont)
            {
                return;
            }

            //Orphaned worker.
            {
                string[] parts = slavelist.Split(new char[] { ',', ';' });
                string sl = "";
                foreach (string p in parts)
                {
                    if (string.Compare(p, slave, true) != 0)
                    {
                        sl += ";" + p;
                    }
                }
                sl = sl.Trim(';');
                if (sl.Length == 0)
                {
                    Console.Error.WriteLine("Cannot perform orphaned worker test.  Must have at least 2 machines in Slavelist tag in dfs.xml.");
                }
                else
                {
                    try
                    {
                        DFSUtils.ChangeDFSXMLSlaveList(dfs, dfspath, sl);

                        string result = Exec.Shell(exe + " clustercheck " + allhosts);
                        if (verbose)
                        {
                            Console.Write(result);
                        }
                        Console.WriteLine();
                        Console.WriteLine("-");
                        Console.WriteLine("Test case: Orphaned worker:");

                        string expResult = @"Orphaned worker: \\" + slave;
                        if (result.IndexOf(expResult) > -1)
                        {
                            Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                        }
                        else
                        {
                            Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                        }
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine("Error during orphaned worker test case: {0}", e.Message);
                    }
                    finally
                    {
                        cont = DFSUtils.UndoFileChanges(dfspath, dfsback);
                    }
                }
            }

            if (!cont)
            {
                return;
            }

            System.IO.File.Delete(dfsback);
            System.IO.File.Delete(slavedatback);
        }
    }
}
