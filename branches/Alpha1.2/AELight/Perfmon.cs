/**************************************************************************************
 *  MySpace’s Mapreduce Framework is a mapreduce framework for distributed computing  *
 *  and developing distributed computing applications on large clusters of servers.   *
 *                                                                                    *
 *  Copyright (C) 2008  MySpace Inc. <http://qizmt.myspace.com/>                      *
 *                                                                                    *
 *  This program is free software: you can redistribute it and/or modify              *
 *  it under the terms of the GNU General Public License as published by              *
 *  the Free Software Foundation, either version 3 of the License, or                 *
 *  (at your option) any later version.                                               *
 *                                                                                    *
 *  This program is distributed in the hope that it will be useful,                   *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of                    *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                     *
 *  GNU General Public License for more details.                                      *
 *                                                                                    *
 *  You should have received a copy of the GNU General Public License                 *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.             *
***************************************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Win32;

namespace MySpace.DataMining.AELight
{
    public class Perfmon
    {
        delegate string ValueToString(double value);

        public static void SafeGetCounters(string[] args)
        {
            SafeGetCounters(args, null);
        }

        public static void SafeGetCounters(string[] args, string[] hosts)
        {                       
            if (args.Length == 0)
            {
                Console.Error.WriteLine("Action is not provided for perfmon.");
                AELight.SetFailure();
                return;
            }

            string act = args[0].ToLower();
            int nReads = 1;
            int sleep = 5000;
            string category = "";
            string counter = "";
            string instance = "";
            bool friendlyByteSize = false;
            int nThreads = -1;

            if (args.Length > 1)
            {
                for (int i = 1; i < args.Length; i++)
                {
                    string s = args[i];
                    int del = s.IndexOf('=');
                    string nm = s.ToLower();
                    string val = "";
                    if (del > -1)
                    {
                        nm = s.Substring(0, del);
                        val = s.Substring(del + 1);
                    }
                    if (nm.StartsWith("-"))
                    {
                        nm = nm.Substring(1);
                    }

                    switch (nm)
                    {
                        case "a":
                            try
                            {
                                nReads = Int32.Parse(val);
                            }
                            catch
                            {
                                Console.Error.WriteLine("Reading count provided for 'a' is not a valid number.");
                                AELight.SetFailure();
                                return;
                            }

                            if (nReads <= 0)
                            {
                                nReads = 1;
                            }
                            break;

                        case "t":
                            try
                            {
                                nThreads = Int32.Parse(val);
                            }
                            catch
                            {
                                Console.Error.WriteLine("Thread count provided for 't' is not a valid number.");
                                AELight.SetFailure();
                                return;
                            }
                            break;

                        case "s":
                            try
                            {
                                sleep = Int32.Parse(val);
                            }
                            catch
                            {
                                Console.Error.WriteLine("Sleep provided for 's' is not a valid number.");
                                AELight.SetFailure();
                                return;
                            }

                            if (sleep <= 0)
                            {
                                sleep = 5000;
                            }
                            break;

                        case "o":
                            category = val;
                            break;

                        case "c":
                            counter = val;
                            break;

                        case "i":
                            instance = val;
                            break;

                        case "f":
                            friendlyByteSize = true;
                            break;

                        default:
                            if (i == args.Length - 1)
                            {
                                string shosts = args[i];
                                if (shosts.StartsWith("@"))
                                {
                                    shosts = System.IO.File.ReadAllText(shosts.Substring(1));
                                    hosts = shosts.Split(new string[] { ";", ",", Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
                                }
                                else
                                {
                                    hosts = shosts.Split(';', ',');
                                }
                            }
                            else
                            {
                                Console.Error.WriteLine("Unexpected: {0}", args[i]);
                            }
                            break;
                    }
                }
            }

            if (null == hosts)
            {
                dfs dc = AELight.LoadDfsConfig();
                hosts = dc.Slaves.SlaveList.Split(';');
            }

            if (nThreads <= 0)
            {
                nThreads = hosts.Length;
            }

#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
#endif

            switch (act)
            {
                case "network":
                    GetBytesSentReceivedCounters(args, hosts, nThreads, nReads, sleep);
                    break;

                case "cputime":
                    GetGenericCounters("Processor", "% Processor Time", "_Total", hosts, nThreads, nReads, sleep, null);
                    break;

                case "diskio":
                    GetGenericCounters("PhysicalDisk", "Disk Bytes/sec", "_Total", hosts, nThreads, nReads, sleep, GetFriendlyByteSize);
                    break;

                case "availablememory":
                    GetGenericCounters("Memory", "Available Bytes", "", hosts, nThreads, nReads, sleep, GetFriendlyByteSize);
                    break;

                case "generic":
                    if (category.Length == 0 || counter.Length == 0)
                    {
                        Console.Error.WriteLine("Argument(s) missing: [o=<ObjectName>] [c=<CounterName>]");
                        AELight.SetFailure();
                        return;
                    }

                    if (friendlyByteSize)
                    {
                        GetGenericCounters(category, counter, instance, hosts, nThreads, nReads, sleep, GetFriendlyByteSize);
                    }
                    else
                    {
                        GetGenericCounters(category, counter, instance, hosts, nThreads, nReads, sleep, null);
                    }
                    
                    break;

                default:
                    Console.Error.WriteLine("Action is invalid: {0}", act);
                    AELight.SetFailure();
                    return;
            }
        }

        private static int CompareFloat(KeyValuePair<string, float> firstPair, KeyValuePair<string, float> nextPair)
        {
           return firstPair.Value.CompareTo(nextPair.Value);
        }

        private static void GetGenericCounters(string category, string counter, string instance, string[] hosts, int nThreads, int nReads, int sleep, ValueToString func)
        {
            DateTime startTime = DateTime.Now;
            Dictionary<string, float> dtData = new Dictionary<string, float>();

            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string slave)
            {
                if (string.Compare(slave, "localhost", true) == 0)
                {
                    slave = System.Net.Dns.GetHostName();
                }

                lock (dtData)
                {
                    Console.WriteLine();
                    Console.WriteLine("Waiting to connect: {0}", slave);
                }

                try
                {
                    System.Diagnostics.PerformanceCounter pc = new System.Diagnostics.PerformanceCounter(category, counter, instance, slave);

                    lock (dtData)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Connected: {0}", slave);
                    }

                    //Initial reading.
                    pc.NextValue();

                    float data = 0;

                    for (int i = 0; i < nReads; i++)
                    {
                        System.Threading.Thread.Sleep(sleep);
                        data += pc.NextValue();
                    }

                    if (nReads > 1)
                    {
                        data = data / (float)nReads;
                    }

                    lock (dtData)
                    {
                        Console.WriteLine();
                        Console.WriteLine("{0}: {1}: {2}", counter, slave, func == null ? data.ToString("N2") : func(data));
                        dtData.Add(slave, data);
                    }
                }
                catch (Exception e)
                {
                    lock (dtData)
                    {
                        Console.Error.WriteLine("Error: {0}: {1}", slave, e.Message);
                    }
                }
            }
            ), hosts, nThreads);

            if (dtData.Count > 0)
            {
                //Sort
                List<KeyValuePair<string, float>> sData = new List<KeyValuePair<string, float>>(dtData);
                sData.Sort(CompareFloat);

                //Max, min
                Console.WriteLine();
                Console.WriteLine("Min {0}: {1}: {2}", counter, sData[0].Key, func == null ? sData[0].Value.ToString("N2") : func(sData[0].Value));
                Console.WriteLine("Max {0}: {1}: {2}", counter, sData[sData.Count - 1].Key, func == null ? sData[sData.Count - 1].Value.ToString("N2") : func(sData[sData.Count - 1].Value));

                double totalData = 0;

                foreach (float f in dtData.Values)
                {
                    totalData += f;
                }

                //Avg
                double avg = totalData / (double)sData.Count;

                Console.WriteLine();
                Console.WriteLine("Avg {0}: {1}", counter, func == null ? avg.ToString("N2") : func(avg));
            }           

            //Dt
            Console.WriteLine();
            Console.WriteLine("Perfmon Request Time: {0}", startTime.ToString());
            Console.WriteLine("Perfmon End Time: {0}", DateTime.Now.ToString());
        }

        private static string GetFriendlyByteSize(double size)
        {
            return AELight.GetFriendlyByteSize((long)size);
        }
        
        private static void GetBytesSentReceivedCounters(string[] args, string[] hosts, int nThreads, int nReads, int sleep)
        {
            DateTime startTime = DateTime.Now;   
            Dictionary<string, float> dtSent = new Dictionary<string, float>();
            Dictionary<string, float> dtReceived = new Dictionary<string, float>();

            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
            new Action<string>(
            delegate(string slave)
            {
                if (string.Compare(slave, "localhost", true) == 0)
                {
                    slave = System.Net.Dns.GetHostName();
                }

                List<System.Diagnostics.PerformanceCounter> received = new List<System.Diagnostics.PerformanceCounter>();
                List<System.Diagnostics.PerformanceCounter> sent = new List<System.Diagnostics.PerformanceCounter>();

                lock (dtSent)
                {
                    Console.WriteLine();
                    Console.WriteLine("Waiting to connect: {0}", slave);
                }

                System.Diagnostics.PerformanceCounterCategory cat = new System.Diagnostics.PerformanceCounterCategory("Network Interface", slave);
                string[] instances = cat.GetInstanceNames();

                try
                {
                    foreach (string s in instances)
                    {
                        if (s.ToLower().IndexOf("loopback") == -1)
                        {
                            received.Add(new System.Diagnostics.PerformanceCounter("Network Interface", "Bytes Received/sec", s, slave));
                            sent.Add(new System.Diagnostics.PerformanceCounter("Network Interface", "Bytes Sent/sec", s, slave));
                        }
                    }

                    lock (dtSent)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Connected: {0}", slave);
                    }

                    //Initial reading.
                    foreach (System.Diagnostics.PerformanceCounter pc in received)
                    {
                        pc.NextValue();
                    }

                    foreach (System.Diagnostics.PerformanceCounter pc in sent)
                    {
                        pc.NextValue();
                    }

                    float br = 0;
                    float bs = 0;

                    for (int i = 0; i < nReads; i++)
                    {
                        System.Threading.Thread.Sleep(sleep);

                        foreach (System.Diagnostics.PerformanceCounter pc in received)
                        {
                            br += pc.NextValue();
                        }
                        foreach (System.Diagnostics.PerformanceCounter pc in sent)
                        {
                            bs += pc.NextValue();
                        }
                    }

                    if (nReads > 1)
                    {
                        br = br / (float)nReads;
                        bs = bs / (float)nReads;
                    }

                    lock (dtSent)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Received {0}: {1}/s", slave, AELight.GetFriendlyByteSize((long)br));
                        Console.WriteLine("Sent {0}: {1}/s", slave, AELight.GetFriendlyByteSize((long)bs));
                        dtSent.Add(slave, bs);
                    }

                    lock (dtReceived)
                    {
                        dtReceived.Add(slave, br);
                    }
                }
                catch (Exception e)
                {
                    lock (dtSent)
                    {
                        Console.Error.WriteLine("Error while reading counter: {0}: {1}", slave, e.Message);
                    }
                }
            }
            ), hosts, nThreads);

            //Write out total sent, received.
            double totalSent = 0;
            double totalReceived = 0;

            foreach (float f in dtSent.Values)
            {
                totalSent += f;
            }

            foreach (float f in dtReceived.Values)
            {
                totalReceived += f;
            }

            Console.WriteLine();
            Console.WriteLine("Total Received: {0}/s", AELight.GetFriendlyByteSize((long)totalReceived));
            Console.WriteLine("Total Sent: {0}/s", AELight.GetFriendlyByteSize((long)totalSent));

            //Sort
            List<KeyValuePair<string, float>> sSent = new List<KeyValuePair<string, float>>(dtSent);
            sSent.Sort(CompareFloat);

            List<KeyValuePair<string, float>> sReceived = new List<KeyValuePair<string, float>>(dtReceived);
            sReceived.Sort(CompareFloat);

            //Max, min
            Console.WriteLine();
            Console.WriteLine("Min Received: {0} {1}/s", sReceived[0].Key, AELight.GetFriendlyByteSize((long)sReceived[0].Value));
            Console.WriteLine("Min Sent: {0} {1}/s", sSent[0].Key, AELight.GetFriendlyByteSize((long)sSent[0].Value));

            Console.WriteLine("Max Received: {0} {1}/s", sReceived[sReceived.Count - 1].Key, AELight.GetFriendlyByteSize((long)sReceived[sReceived.Count - 1].Value));
            Console.WriteLine("Max Sent: {0} {1}/s", sSent[sSent.Count - 1].Key, AELight.GetFriendlyByteSize((long)sSent[sSent.Count - 1].Value));

            //Avg
            double avgSent = totalSent / (double)sSent.Count;
            double avgReceived = totalReceived / (double)sReceived.Count;

            Console.WriteLine();
            Console.WriteLine("Avg Received: {0}/s", AELight.GetFriendlyByteSize((long)avgReceived));
            Console.WriteLine("Avg Sent: {0}/s", AELight.GetFriendlyByteSize((long)avgSent));

            //Dt
            Console.WriteLine();
            Console.WriteLine("Perfmon Request Time: {0}", startTime.ToString());
            Console.WriteLine("Perfmon End Time: {0}", DateTime.Now.ToString());
        }
    }
}
