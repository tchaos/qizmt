using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    partial class Program
    {
        private static void DeleteRIndexes(string[] args)
        {
            string[] hosts = Utils.GetQizmtHosts();

            if (hosts.Length == 0)
            {
                Console.Error.WriteLine("No Qizmt host is found.");
                return;
            }

            const int MAX_TRIES = 10;
            Dictionary<string, StringBuilder> errs = new Dictionary<string, StringBuilder>(hosts.Length);
            string currentdir = CurrentDir.Replace(':', '$');
            int threadcount = hosts.Length;
            if (threadcount > 15)
            {
                threadcount = 15;
            }

            RDBMS_Admin.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string host)
                {
                    int triesremain = MAX_TRIES;
                    string root = @"\\" + host + @"\" + currentdir + @"\";
                    string[] indfiles = System.IO.Directory.GetFiles(root, "ind.*.ind");

                    foreach (string indfile in indfiles)
                    {
                        for (; ; )
                        {
                            try
                            {
                                System.IO.File.Delete(indfile);
                                lock (hosts)
                                {
                                    Console.Write('.');
                                }
                                break;
                            }
                            catch (Exception e)
                            {
                                if (--triesremain <= 0)
                                {
                                    lock (hosts)
                                    {
                                        if (!errs.ContainsKey(host))
                                        {
                                            errs.Add(host, new StringBuilder(1024));
                                        }
                                        StringBuilder sb = errs[host];
                                        sb.Append(Environment.NewLine).Append("File: ").
                                        Append(indfile).Append("; Delete error: ").Append(e.ToString());
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    
                    string sysindex = root + "sys.indexes";
                    if (System.IO.File.Exists(sysindex))
                    {
                        System.Xml.XmlDocument doc = new System.Xml.XmlDocument();
                        doc.Load(sysindex);
                        System.Xml.XmlNodeList indexes = doc.SelectNodes("//index");
                        for (int i = 0; i < indexes.Count; i++)
                        {
                            indexes[i].ParentNode.RemoveChild(indexes[i]);
                        }

                        triesremain = MAX_TRIES;
                        for (; ; )
                        {
                            try
                            {
                                doc.Save(sysindex);
                                lock (hosts)
                                {
                                    Console.Write('.');
                                }
                                break;
                            }
                            catch (Exception e)
                            {
                                if (--triesremain <= 0)
                                {
                                    lock (hosts)
                                    {
                                        if (!errs.ContainsKey(host))
                                        {
                                            errs.Add(host, new StringBuilder(1024));
                                        }
                                        StringBuilder sb = errs[host];
                                        sb.Append(Environment.NewLine).Append("File: ").
                                        Append(sysindex).Append("; Save error: ").Append(e.ToString());
                                    }
                                    break;
                                }
                            }
                        }
                    }    
                }
            ), hosts, threadcount);

            Console.WriteLine();

            if (errs.Count > 0)
            {
                Console.WriteLine("Errors encountered while trying to delete rindexes from these machines:");
                foreach (string host in errs.Keys)
                {
                    Console.WriteLine(host);
                    Console.WriteLine(errs[host].ToString());
                }
            }
            else
            {
                Console.WriteLine("Done");
            }
        }

        private static void VerifyRIndexes(string[] args)
        {
            string[] hosts = Utils.GetQizmtHosts();
            if (hosts.Length == 0)
            {
                Console.Error.WriteLine("No Qizmt host is found.");
                return;
            }

            bool verbose = args.Length > 1 && string.Compare(args[1], "-v", true) == 0;
            string currentdir = CurrentDir.Replace(':', '$');
            int threadcount = hosts.Length;
            if (threadcount > 15)
            {
                threadcount = 15;
            }

            bool identicalsysindexes = true;
            {
                Console.WriteLine();
                Console.WriteLine("Getting sysindexes state...");
                if (hosts.Length > 1)
                {
                    System.Xml.XmlDocument prevdoc = null;
                    for (int i = 0; i < hosts.Length; i++)
                    {
                        string sysindex = @"\\" + hosts[i] + @"\" + currentdir + @"\sys.indexes";
                        System.Xml.XmlDocument doc = null;
                        if (System.IO.File.Exists(sysindex))
                        {
                            doc = new System.Xml.XmlDocument();
                            doc.Load(sysindex);
                        }

                        if (i > 0)
                        {
                            //compare
                            if (!CompareSysIndexes(prevdoc, doc))
                            {
                                identicalsysindexes = false;
                                break;
                            }
                        }
                        prevdoc = doc;
                    }
                }
            }            
            
            Dictionary<string, List<string>> leaks = new Dictionary<string, List<string>>();
            Dictionary<string, Dictionary<string, int>> duprindexes = new Dictionary<string, Dictionary<string, int>>();
            Dictionary<string, Dictionary<string, int>> duptables = new Dictionary<string, Dictionary<string, int>>();
            Dictionary<string, Dictionary<string, int>> rindexeswithmissingfiles = new Dictionary<string, Dictionary<string, int>>();
            Dictionary<string, int> okrindexesscore = new Dictionary<string, int>();
            Dictionary<string, string> okrindexestypeinfo = new Dictionary<string, string>();
            RDBMS_Admin.ThreadTools<string>.Parallel(
               new Action<string>(
               delegate(string host)
               {
                   if (verbose)
                   {
                       lock (leaks)
                       {
                           Console.WriteLine();
                           Console.WriteLine("Getting info: {0}...", host);
                       }
                   }

                   string root = @"\\" + host + @"\" + currentdir;
                   string sysindex = root + @"\sys.indexes";
                   Dictionary<string, int> rindexes = new Dictionary<string, int>();
                   Dictionary<string, int> tables = new Dictionary<string, int>();

                   if (System.IO.File.Exists(sysindex))
                   {
                       System.Xml.XmlDocument doc = new System.Xml.XmlDocument();
                       doc.Load(sysindex);

                       System.Xml.XmlNodeList nodes = doc.SelectNodes("//index");
                       foreach (System.Xml.XmlNode node in nodes)
                       {
                           string rindexname = node["name"].InnerText.ToLower();
                           string tablename = node["table"]["name"].InnerText.ToLower();
                           bool pin = node["pin"].InnerText == "1";
                           bool ok = true;

                           if (!rindexes.ContainsKey(rindexname))
                           {
                               rindexes.Add(rindexname, 1);
                           }
                           else
                           {
                               ok = false;
                               lock (duprindexes)
                               {
                                   if (!duprindexes.ContainsKey(host))
                                   {
                                       duprindexes.Add(host, new Dictionary<string, int>());
                                   }
                                   if (!duprindexes[host].ContainsKey(rindexname))
                                   {
                                       duprindexes[host].Add(rindexname, 0);
                                   }
                                   duprindexes[host][rindexname]++;
                               }
                           }

                           if (!tables.ContainsKey(tablename))
                           {
                               tables.Add(tablename, 1);
                           }
                           else
                           {
                               ok = false;
                               lock (duptables)
                               {
                                   if (!duptables.ContainsKey(host))
                                   {
                                       duptables.Add(host, new Dictionary<string, int>());
                                   }
                                   if (!duptables[host].ContainsKey(tablename))
                                   {
                                       duptables[host].Add(tablename, 0);
                                   }
                                   duptables[host][tablename]++;
                               }
                           }

                           if (!System.IO.File.Exists(root + @"\ind.Index." + rindexname + ".ind"))
                           {
                               ok = false;
                               lock (rindexeswithmissingfiles)
                               {
                                   if (!rindexeswithmissingfiles.ContainsKey(host))
                                   {
                                       rindexeswithmissingfiles.Add(host, new Dictionary<string, int>());
                                   }
                                   if (!rindexeswithmissingfiles[host].ContainsKey(rindexname))
                                   {
                                       rindexeswithmissingfiles[host].Add(rindexname, 0);
                                   }
                                   rindexeswithmissingfiles[host][rindexname]++;
                               }
                           }

                           if (pin && !System.IO.File.Exists(root + @"\ind.Pin." + rindexname + ".ind"))
                           {
                               ok = false;
                               lock (rindexeswithmissingfiles)
                               {
                                   if (!rindexeswithmissingfiles.ContainsKey(host))
                                   {
                                       rindexeswithmissingfiles.Add(host, new Dictionary<string, int>());
                                   }
                                   if (!rindexeswithmissingfiles[host].ContainsKey(rindexname))
                                   {
                                       rindexeswithmissingfiles[host].Add(rindexname, 0);
                                   }
                                   rindexeswithmissingfiles[host][rindexname]++;
                               }
                           }

                           if (ok)
                           {
                               lock (okrindexesscore)
                               {
                                   if (!okrindexesscore.ContainsKey(rindexname))
                                   {
                                       okrindexesscore.Add(rindexname, 0);
                                   }
                                   okrindexesscore[rindexname]++;
                               }

                               lock (okrindexestypeinfo)
                               {
                                   if (!okrindexestypeinfo.ContainsKey(rindexname))
                                   {
                                       okrindexestypeinfo.Add(rindexname, GetRIndexTypeInfo(node));
                                   }
                               }
                           }
                       }
                   }

                   string[] indfiles = System.IO.Directory.GetFiles(root, "ind.*.ind");
                   foreach (string indfile in indfiles)
                   {
                       string[] parts = indfile.Split('.');
                       string rindexname = parts[2].ToLower();
                       if (!rindexes.ContainsKey(rindexname))
                       {
                           lock (leaks)
                           {
                               if (!leaks.ContainsKey(host))
                               {
                                   leaks.Add(host, new List<string>());
                               }
                               leaks[host].Add(indfile);
                           }
                       }
                   }
               }
           ), hosts, threadcount);

            if (identicalsysindexes)
            {
                Console.WriteLine();
                Console.WriteLine("State of sysindexes on cluster:");
                Console.WriteLine("All identical");
            }

            Console.WriteLine();
            Console.WriteLine("Healthy Hosts:");
            bool unhealthyhostfound = false;
            foreach (string host in hosts)
            {
                if (!(leaks.ContainsKey(host) || duprindexes.ContainsKey(host) ||
                    duptables.ContainsKey(host) || rindexeswithmissingfiles.ContainsKey(host)))
                {
                    Console.WriteLine();
                    Console.WriteLine(host);
                    Console.WriteLine("No leak");
                    Console.WriteLine("No duplicated rindex");
                    Console.WriteLine("No duplicated table");
                    Console.WriteLine("No rindex with missing meta files");
                }
                else
                {
                    unhealthyhostfound = true;
                }
            }

            Console.WriteLine();
            Console.WriteLine("Valid RIndexes:");
            foreach (string key in okrindexesscore.Keys)
            {
                if (okrindexesscore[key] == hosts.Length)
                {
                    Console.WriteLine(okrindexestypeinfo[key]);
                }
            }

            ConsoleColor oldColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;

            if (!identicalsysindexes)
            {
                Console.WriteLine();
                Console.WriteLine("State of sysindexes on cluster:");                
                Console.WriteLine("Not identical");
            }

            if (unhealthyhostfound)
            {
                Console.WriteLine();
                Console.WriteLine("Unhealthy Hosts:");
                foreach (string host in hosts)
                {
                    if (leaks.ContainsKey(host) || duprindexes.ContainsKey(host) || 
                        duptables.ContainsKey(host) || rindexeswithmissingfiles.ContainsKey(host))
                    {
                        Console.WriteLine();
                        Console.WriteLine(host);

                        if (leaks.ContainsKey(host))
                        {
                            Console.WriteLine("Leaks found");
                            if (verbose)
                            {
                                List<string> dups = leaks[host];
                                foreach (string dp in dups)
                                {
                                    Console.WriteLine("  " + dp);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("No leak");
                        }

                        if (duprindexes.ContainsKey(host))
                        {
                            Console.WriteLine("Duplicated rindex found");
                            if (verbose)
                            {
                                foreach (string dp in duprindexes[host].Keys)
                                {
                                    Console.WriteLine("  " + dp);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("No duplicated rindex");
                        }

                        if (duptables.ContainsKey(host))
                        {
                            Console.WriteLine("Duplicated table found");
                            if (verbose)
                            {
                                foreach (string dp in duptables[host].Keys)
                                {
                                    Console.WriteLine("  " + dp);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("No duplicated table");
                        }

                        if (rindexeswithmissingfiles.ContainsKey(host))
                        {
                            Console.WriteLine("Rindex with missing meta files found");
                            if (verbose)
                            {
                                foreach (string dp in rindexeswithmissingfiles[host].Keys)
                                {
                                    Console.WriteLine("  " + dp);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("No rindex with missing meta files");
                        }
                    }
                }   
            }
            Console.ForegroundColor = oldColor;
        }

        private static bool CompareSysIndexes(System.Xml.XmlDocument x, System.Xml.XmlDocument y)
        {
            if (x != null && y != null)
            {
                return x.OuterXml == y.OuterXml;
            }

            bool xempty = false;
            bool yempty = false;

            if (x == null)
            {
                xempty = true;
            }
            else
            {
                xempty = x.SelectSingleNode("//index") == null;               
            }

            if (y == null)
            {
                yempty = true;
            }
            else
            {
                yempty = y.SelectSingleNode("//index") == null;
            }

            return xempty == yempty;
        }

        private static string GetRIndexTypeInfo(System.Xml.XmlNode node)
        {
            string tableinfo = node["table"]["name"].InnerText;

            System.Xml.XmlNodeList xecols = node.SelectNodes("table/column");
            string colinfo = "";
            foreach (System.Xml.XmlNode xecol in xecols)
            {
                colinfo += ", " + xecol["name"].InnerText + " " + xecol["type"].InnerText;
            }

            if (colinfo.Length > 0)
            {
                colinfo = colinfo.Substring(2);
            }

            return node["name"].InnerText + ":" + Environment.NewLine +
                "  Table: " + tableinfo + "(" + colinfo + ")" + Environment.NewLine +
                "  Ordinal: " + node["ordinal"].InnerText + Environment.NewLine +
                "  PinMemory: " + node["pin"].InnerText + Environment.NewLine +
                "  PinMemoryHash: " + node["pinHash"].InnerText + Environment.NewLine;
        }

        private struct Column
        {
            internal string Name;
            internal string Type;
            internal int Bytes;
        }

        private struct Table
        {
            internal string Name;
            internal Column[] Columns;
        }

        private struct RIndex
        {
            internal string Name;
            internal int Ordinal;
            internal bool Pin;
            internal bool PinHash;
            internal Table Table;
        }
    }
}