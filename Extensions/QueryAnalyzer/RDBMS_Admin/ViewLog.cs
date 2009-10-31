using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS_Admin
{
    partial class Program
    {
        private static void ViewLog(string[] args)
        {
            int maxentries = 1000;
            string[] hosts = Utils.GetQizmtHosts();

            if (hosts.Length == 0)
            {
                Console.Error.WriteLine("No Qizmt host is found.");
                return;
            }

            List<string> logpaths = new List<string>();
            {
                string execpath = CurrentDir.Replace(':', '$');
                foreach (string host in hosts)
                {
                    logpaths.Add(@"\\" + host + @"\" + execpath + @"\errors.txt");
                }
            }           

            const int MAXBYTE = 1024 * 1024 * 64;
            int maxbytepart = MAXBYTE / logpaths.Count;
            int maxentriespart = maxentries / logpaths.Count;
            if (maxentries % logpaths.Count != 0)
            {
                maxentriespart++;
            }

            List<string[]> allentries = new List<string[]>(logpaths.Count);
            RDBMS_Admin.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string logpath)
                {
                    if (!System.IO.File.Exists(logpath))
                    {
                        return;
                    }

                    string token = Environment.NewLine + Environment.NewLine + "[";

                    System.IO.FileStream fs = null;
                    try
                    {
                        fs = new System.IO.FileStream(logpath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite);
                        if (fs.Length > maxbytepart * 2)
                        {
                            fs.Position = fs.Length - maxbytepart;
                        }

                        int ib = 0;
                        List<long> idx = new List<long>();
                        long entryStart = 0;
                        while ((ib = fs.ReadByte()) > -1)
                        {
                            if (ib == (int)token[0])
                            {
                                bool istoken = true;
                                for (int i = 1; i < token.Length; i++)
                                {
                                    if (fs.ReadByte() != (int)token[i])
                                    {
                                        istoken = false;
                                        break;
                                    }
                                }

                                if (istoken)
                                {
                                    idx.Add(entryStart);
                                    entryStart = fs.Position - 1;
                                }
                            }
                        }

                        //get the last entryStart.
                        if (idx.Count > 0)
                        {
                            if (entryStart != idx[idx.Count - 1])
                            {
                                idx.Add(entryStart);
                            }
                        }
                        else
                        {
                            //1 entry only.
                            if (fs.Length > 0)
                            {
                                idx.Add(entryStart);
                            }
                        }

                        if (idx.Count == 0)
                        {
                            return;
                        }

                        long flen = fs.Length;
                        int startidx = idx.Count > maxentriespart ? idx.Count - maxentriespart : 0;
                        long offset = idx[startidx];
                        long buflen = flen - offset;
                        while (buflen > maxbytepart && startidx < idx.Count - 1)
                        {
                            startidx++;
                            offset = idx[startidx];
                            buflen = flen - offset;
                        }
                        if (buflen > maxbytepart)
                        {
                            throw new Exception("log too large");
                        }

                        byte[] buf = new byte[buflen];
                        fs.Position = offset;
                        fs.Read(buf, 0, buf.Length);
                        fs.Close();
                        fs = null;

                        string[] entries = new string[idx.Count - startidx];
                        for (int i = startidx; i < idx.Count; i++)
                        {
                            int pos = (int)(idx[i] - offset);
                            int bytecount = 0;
                            if (i < idx.Count - 1)
                            {
                                bytecount = (int)(idx[i + 1] - offset - pos);
                            }
                            else
                            {
                                bytecount = buf.Length - pos;
                            }
                            entries[i - startidx] = System.Text.Encoding.ASCII.GetString(buf, pos, bytecount);
                        }
                        lock (allentries)
                        {
                            allentries.Add(entries);
                        }
                    }
                    catch
                    {
                        if (fs != null)
                        {
                            fs.Close();
                            fs = null;
                        }
                        throw;
                    }
                }
                ), logpaths, logpaths.Count);

            if (allentries.Count == 0)
            {
                Console.Error.WriteLine("No log entries found.");
                return;
            }

            Console.WriteLine("-");
            Console.WriteLine("Log entries:");
            Console.WriteLine("-");

            if (allentries.Count == 1)
            {
                foreach (string entry in allentries[0])
                {
                    Console.Write(entry);
                    Console.WriteLine("----------------------------------------------------------------");
                }
                Console.WriteLine("-");
                Console.WriteLine("Entries displayed: {0}", allentries[0].Length);
                Console.WriteLine("-");
            }
            else
            {
                List<KeyValuePair<DateTime, string>> list = new List<KeyValuePair<DateTime, string>>();
                foreach (string[] entries in allentries)
                {
                    foreach (string entry in entries)
                    {
                        int del = entry.IndexOf('M');   //AM or PM
                        string sdate = entry.Substring(1, del);
                        try
                        {
                            DateTime dt = DateTime.Parse(sdate);
                            list.Add(new KeyValuePair<DateTime, string>(dt, entry));
                        }
                        catch
                        {
                        }
                    }
                }

                list.Sort(delegate(KeyValuePair<DateTime, string> x, KeyValuePair<DateTime, string> y)
                {
                    return x.Key.CompareTo(y.Key);
                });

                int start = list.Count > maxentries ? list.Count - maxentries : 0;
                for (int i = start; i < list.Count; i++)
                {
                    Console.Write(list[i].Value);
                    Console.WriteLine("----------------------------------------------------------------");
                }

                Console.WriteLine("-");
                Console.WriteLine("Entries displayed: {0}", list.Count - start);
                Console.WriteLine("-");
            }
        }
    }
}
