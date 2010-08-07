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

//#if DEBUG
#define DOSERVICE_TRACE
//#define FAILOVER_DEBUG
//#define TESTFAULTTOLERANT
//#endif

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Runtime.InteropServices;
using System.Linq;


namespace MySpace.DataMining.DistributedObjects5
{
    public partial class DistributedObjectsService : ServiceBase
    {
        Thread lthd;
        Socket lsock;


        /*
        static void slog(string line)
        {
            Console.WriteLine("{0}", line);
        }
         * */


        void ClientThreadProc(object obj)
        {
            Socket dllclientSock = null;
            Block block = null;
            try
            {
                DistributedObjectsService.DOService_AddTraceThread(null);
                dllclientSock = (Socket)obj;
                block = new Block(dllclientSock);
                block.HandleDllClient();
            }
            catch (Exception e)
            {
                XLog.errorlog("ClientThreadProc (dll) exception: " + e.ToString());

                try
                {
                    if (null != block)
                    {
                        block.abort();
                    }
                }
                catch (Exception e2)
                {
                }
            }
            finally
            {
                if (block != null)
                {
                    try
                    {
                        block.cleanup();
                    }
                    catch
                    {
                    }
                }
            }
            DistributedObjectsService.DOService_RemoveTraceThread(null);
        }


        void ListenThreadProc()
        {
            try
            {
                DOService_AddTraceThread(null);

                lsock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                IPEndPoint ipep = new IPEndPoint(IPAddress.Any, 55900);
                for (int i = 0; ; i++)
                {
                    try
                    {
                        lsock.Bind(ipep);
                        break;
                    }
                    catch(Exception e)
                    {
                        if (i >= 5)
                        {
                            throw;
                        }
                        System.Threading.Thread.Sleep(1000 * 4);
                        continue;
                    }
                }

                lsock.Listen(30);

                for (; ; )
                {
                    Socket dllclientSock = lsock.Accept();                   

                    if (isAllowedMachine(dllclientSock))
                    {                       
                        Thread cthd = new Thread(new ParameterizedThreadStart(ClientThreadProc));
                        cthd.Name = "ClientThread" + NextTCount().ToString();
                        cthd.IsBackground = true;
                        cthd.Start(dllclientSock);
                    }
                    else
                    {
                        XLog.errorlog("Machine not in cluster, closing socket: " + dllclientSock.RemoteEndPoint.ToString());
                        dllclientSock.Close();
                    }                   
                }
            }
            catch (ThreadAbortException e)
            {
            }
            catch (Exception e)
            {
                XLog.errorlog("ListenThreadProc exception: " + e.ToString());
            }
            DOService_RemoveTraceThread(null);
        }

        static bool isAllowedMachine(Socket dllclientSock)
        {
            string firewall = Environment.CurrentDirectory + "\\firewall.dat";

            if (System.IO.File.Exists(firewall))
            {
                IPEndPoint clientIP = (IPEndPoint)dllclientSock.RemoteEndPoint;
                string clientHostname = System.Net.Dns.GetHostEntry(clientIP.Address).HostName;

                string[] machines = System.IO.File.ReadAllLines(firewall);

                foreach (string m in machines)
                {
                    if (string.Compare(clientHostname.Trim(), m, true) == 0)
                    {
                        return true;
                    }
                }
                return false;
            }
            else
            {
                return true;
            }
        }

        static int tcount = 0;

        int NextTCount()
        {
            lock ("{F012D08B-2300-4a37-A1F8-ADAA44A26987}")
            {
                return ++tcount;
            }
        }


        public DistributedObjectsService()
        {
            InitializeComponent();
        }


        public static void DeleteAllFilesOfType(string ext, string indirectory)
        {
            if('.' == ext[0])
            {
                ext = ext.Substring(1);
            }
            foreach (string fn in System.IO.Directory.GetFiles(indirectory))
            {
                if (fn.Length > ext.Length && '.' == fn[fn.Length - ext.Length - 1]
                    && 0 == string.Compare(fn.Substring(fn.Length - ext.Length), ext, true))
                {
                    System.IO.File.Delete(fn);
                }
            }
        }

        public static void DeleteAllFilesOfType(string ext)
        {
            DeleteAllFilesOfType(ext, ".");
        }


        const string HIST_FILENAME = "harddrive_history.txt";
        static int logusagemins = 0;

        private void logusageshutdown()
        {
            using (StreamWriter sw = File.AppendText(HIST_FILENAME))
            {
                sw.WriteLine("-------------- Driver Shutdown: {0}  Stats Interval: {1} minutes -----------------", DateTime.Now.ToString(), logusagemins);
            }
        }


        public static string GetFriendlySize(ulong size)
        {
            string friendlyusage;
            if (size >= 1024) // KB+
            {
                if (size >= 1024 * 1024) // MB+
                {
                    if (size >= 1024 * 1024 * 1024) // GB+
                    {
                        if (size >= (ulong)1024 * 1024 * 1024 * 1024) // TB+
                        {
                            friendlyusage = (size / 1024 / 1024 / 1024 / 1024).ToString() + "TB";
                        }
                        else // GB
                        {
                            friendlyusage = (size / 1024 / 1024 / 1024).ToString() + "GB";
                        }
                    }
                    else // MB
                    {
                        friendlyusage = (size / 1024 / 1024).ToString() + "MB";
                    }
                }
                else // KB
                {
                    friendlyusage = (size / 1024).ToString() + "KB";
                }
            }
            else
            {
                friendlyusage = size.ToString() + "B"; // ...
            }
            return friendlyusage;
        }


        public static string GetAllCpuUsage()
        {
            string result = "";
            try
            {
                using (System.Diagnostics.PerformanceCounter pc = new PerformanceCounter("Processor", "% Processor Time", "_Total"))
                {
                    pc.NextValue();


                }
            }
            finally
            {
                result = "Error getting % Processor Time";
            }
            return result;
        }


        // Log usage thread procedure.
        private void logusageproc()
        {
            using (StreamWriter sw = File.AppendText(HIST_FILENAME))
            {
                sw.WriteLine("-------------- Driver Startup:  {0}  Stats Interval: {1} minutes -----------------", DateTime.Now.ToString(), logusagemins);
            }

            for (; ; )
            {
                try
                {
                    using (StreamWriter sw = File.AppendText(HIST_FILENAME))
                    {
                        ulong totalusage;
                        bool foundunsorted = false;
                        bool foundsorted = false;
                        uint zbcount = 0;
                        ulong zbusage = 0;
                        ulong zbsmallestfile = ulong.MaxValue;
                        ulong zbbiggestfile = 0;
                        //List<ulong> allzblockfilesizes = new List<ulong>();
                        for (; ; )
                        {
                            try // Recover from exceptions and try again; e.g. in case a temp file was there momentarily.
                            {
                                totalusage = 0;
                                foreach (string fn in System.IO.Directory.GetFiles(".")) // Only files, not dirs.
                                {
                                    ulong thisfilesize;
                                    FileInfo fi = new FileInfo(fn);
                                    {
                                        thisfilesize = (ulong)fi.Length;
                                    }

                                    totalusage += thisfilesize;
                                    //allzblockfilesizes.Add(thisfilesize);

                                    if (fn.Length > 3 && ".zb" == fn.Substring(fn.Length - 3))
                                    {
                                        zbcount++;
                                        zbusage += thisfilesize;
                                        if (thisfilesize > zbbiggestfile)
                                        {
                                            zbbiggestfile = thisfilesize;
                                        }
                                        else if (thisfilesize < zbsmallestfile)
                                        {
                                            zbsmallestfile = thisfilesize;
                                        }
                                        if (-1 != fn.IndexOf("_unsorted"))
                                        {
                                            foundunsorted = true;
                                        }
                                        else if (-1 != fn.IndexOf("_sorted"))
                                        {
                                            foundsorted = true;
                                        }
                                    }
                                }
                            }
                            catch
                            {
                                Thread.Sleep(5000);
                                continue;
                            }
                            break;
                        }

                        string friendlyusage = GetFriendlySize(totalusage);

                        string friendlyzbusage = GetFriendlySize(zbusage);
                        string friendlyzbsmallest = "N/A";
                        string friendlyzbbiggest = "N/A";
                        string friendlyzbaverage = "N/A";
                        if (0 != zbcount)
                        {
                            friendlyzbsmallest = GetFriendlySize(zbsmallestfile);
                            friendlyzbbiggest = GetFriendlySize(zbbiggestfile);
                            friendlyzbaverage = GetFriendlySize(zbusage / zbcount);
                        }

                        string phase = "";
                        if (foundunsorted)
                        {
                            if (foundsorted)
                            {
                                // Both.
                                phase += "Sorting";
                            }
                            else
                            {
                                phase += "Loading";
                            }
                        }
                        else if (foundsorted)
                        {
                            phase += "Sorted";
                        }
                        if (!foundunsorted && !foundsorted)
                        {
                            phase = "None";
                        }
                        //phase = " Phase=" + phase;

                        sw.WriteLine("Sample Taken: " + DateTime.Now.ToString() + "\t| Current Disk Usage: " + friendlyusage + "\t| Current Phase: " + phase + "\t| zBlock Count: " + zbcount.ToString() + "\t| Largest zBlock: " + friendlyzbbiggest + "\t| Smallest zBlock: " + friendlyzbsmallest + "\t| Average zBlock: " + friendlyzbaverage + "\t| Total Rows: " + (zbusage / 8).ToString());
                    }
                }
                catch (Exception e)
                {
                    //XLog.errorlog("logusageproc error: " + e.ToString());
                }

                Thread.Sleep(1000 * 60 * logusagemins);
            }
        }


#if DOSERVICE_TRACE
        static List<System.Threading.Thread> DOService_TraceThreads = new List<System.Threading.Thread>();
#endif

        protected internal static void DOService_AddTraceThread(System.Threading.Thread thd)
        {
#if DOSERVICE_TRACE
            if (null == thd)
            {
                thd = System.Threading.Thread.CurrentThread;
            }
            lock (DOService_TraceThreads)
            {
                DOService_TraceThreads.Add(thd);
            }
#endif
        }

        protected internal static void DOService_RemoveTraceThread(System.Threading.Thread thd)
        {
#if DOSERVICE_TRACE
            if (null == thd)
            {
                thd = System.Threading.Thread.CurrentThread;
            }
            lock (DOService_TraceThreads)
            {
                DOService_TraceThreads.Remove(thd);
            }
#endif
        }


        protected override void OnStart(string[] args)
        {
            try
            {
                MySpace.DataMining.AELight.Surrogate.LogonMachines();

                DOService_AddTraceThread(null);

                string service_base_dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
                System.Environment.CurrentDirectory = service_base_dir;

#if DOSERVICE_TRACE
                try
                {
                    System.Threading.Thread stthd = new System.Threading.Thread(
                        new System.Threading.ThreadStart(
                        delegate
                        {
                            string spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
                            try
                            {
                                string dotracefile = spid + ".trace";
                                const string tracefiledelim = "{C8683F6C-0655-42e7-ACD9-0DDED6509A7C}";
                                for (; ; )
                                {
                                    System.IO.StreamWriter traceout = null;
                                    for (System.Threading.Thread.Sleep(1000 * 60)
                                        ; !System.IO.File.Exists(dotracefile)
                                        ; System.Threading.Thread.Sleep(1000 * 60))
                                    {
                                    }
                                    {
                                        string[] tfc;
                                        try
                                        {
                                            tfc = System.IO.File.ReadAllLines(dotracefile);
                                        }
                                        catch
                                        {
                                            continue;
                                        }
                                        if (tfc.Length < 1 || "." != tfc[tfc.Length - 1])
                                        {
                                            continue;
                                        }
                                        try
                                        {
                                            System.IO.File.Delete(dotracefile);
                                        }
                                        catch
                                        {
                                            continue;
                                        }
                                        if ("." != tfc[0])
                                        {
                                            string traceoutfp = tfc[0];
                                            try
                                            {
                                                traceout = System.IO.File.CreateText(traceoutfp);
                                                traceout.Write("BEGIN:");
                                                traceout.WriteLine(tracefiledelim);
                                            }
                                            catch
                                            {
                                                continue;
                                            }
                                        }
                                    }
                                    if (null == traceout)
                                    {
                                        XLog.errorlog("DOSERVICE_TRACE: " + spid + " Start");
                                    }
                                    for (; ; System.Threading.Thread.Sleep(1000 * 60))
                                    {
                                        foreach (System.Threading.Thread tthd in DOService_TraceThreads)
                                        {
                                            string tr = "";
                                            try
                                            {
                                                bool thdsuspended = false;
                                                try
                                                {
                                                    tthd.Suspend();
                                                    thdsuspended = true;
                                                }
                                                catch (System.Threading.ThreadStateException)
                                                {
                                                }
                                                try
                                                {
                                                    System.Diagnostics.StackTrace st = new System.Diagnostics.StackTrace(tthd, false);
                                                    StringBuilder sbst = new StringBuilder();
                                                    const int maxframesprint = 15;
                                                    for (int i = 0, imax = Math.Min(maxframesprint, st.FrameCount); i < imax; i++)
                                                    {
                                                        if (0 != sbst.Length)
                                                        {
                                                            sbst.Append(", ");
                                                        }
                                                        string mn = "N/A";
                                                        try
                                                        {
                                                            System.Reflection.MethodBase mb = st.GetFrame(i).GetMethod();
                                                            mn = mb.ReflectedType.Name + "." + mb.Name;
                                                        }
                                                        catch
                                                        {
                                                        }
                                                        sbst.Append(mn);
                                                    }
                                                    if (st.FrameCount > maxframesprint)
                                                    {
                                                        sbst.Append(" ... ");
                                                        sbst.Append(st.FrameCount - maxframesprint);
                                                        sbst.Append(" more");
                                                    }
                                                    if (null == traceout)
                                                    {
                                                        XLog.errorlog("DOSERVICE_TRACE: " + spid + " " + tthd.Name + " Trace: " + sbst.ToString());
                                                    }
                                                    else
                                                    {
                                                        traceout.Write("Thread ");
                                                        string tthdname = tthd.Name;
                                                        if (null == tthdname || 0 == tthdname.Length)
                                                        {
                                                            //tthdname = "<unnamed>";
                                                            tthdname = tthd.ManagedThreadId.ToString();
                                                        }
                                                        traceout.Write(tthdname);
                                                        traceout.Write(": ");
                                                        traceout.WriteLine(sbst.ToString());
                                                    }
                                                }
                                                catch (Exception e)
                                                {
                                                    XLog.errorlog("DOSERVICE_TRACE: " + spid + " " + tthd.Name + " Error: " + e.ToString());
                                                }
                                                finally
                                                {
                                                    if (thdsuspended)
                                                    {
                                                        tthd.Resume();
                                                    }
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                XLog.errorlog("DOSERVICE_TRACE: " + spid + " " + tthd.Name + " Trace Error: Cannot access thread: " + e.ToString());
                                            }
                                        }

                                        if (null != traceout)
                                        {
                                            {
                                                traceout.WriteLine("LastQueueActions:");
                                                if (null == MySpace.DataMining.DistributedObjects.Scheduler.LastQueueActions)
                                                {
                                                    traceout.WriteLine("    null");
                                                }
                                                else
                                                {
                                                    foreach (string qa in MySpace.DataMining.DistributedObjects.Scheduler.LastQueueActions)
                                                    {
                                                        traceout.WriteLine("    {0}", qa);
                                                    }
                                                }
                                            }
                                            traceout.Write(tracefiledelim);
                                            traceout.WriteLine(":END");
                                            traceout.Close();
                                            break;
                                        }

                                    }

                                }

                            }
                            catch (Exception e)
                            {
                                XLog.errorlog("DOSERVICE_TRACE: " + spid + " Trace Failure: " + e.ToString());
                            }
                        }));
                    stthd.IsBackground = true;
                    stthd.Start();
                }
                catch (Exception est)
                {
                    XLog.errorlog("DOSERVICE_TRACE: Thread start error: " + est.ToString());
                }
#endif

                // Add to Machine PATH... (if not found)
                try
                {
                    string path = Environment.GetEnvironmentVariable("PATH", EnvironmentVariableTarget.Machine);
                    string addpath = service_base_dir;
                    string newpath = null;
                    if (null == path || 0 == path.Trim().Length)
                    {
                        newpath = addpath;
                    }
                    else
                    {
                        if (-1 == (";" + path + ";").IndexOf(";" + addpath + ";", StringComparison.OrdinalIgnoreCase))
                        {
                            newpath = path + ";" + addpath;
                        }
                    }
                    if (null != newpath)
                    {
                        Environment.SetEnvironmentVariable("PATH", newpath, EnvironmentVariableTarget.Machine);
                    }
                }
                catch(Exception eaoja)
                {
                    XLog.errorlog("Problem updating PATH: " + eaoja.ToString());
                }

                Environment.SetEnvironmentVariable("DOSERVICE", "1");

                {
                    //string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                    //if (computer_name == "MAPDDRULE" || computer_name == "MAPDCMILLER")
                    {
                        if (System.IO.File.Exists("sleep.txt"))
                        {
                            System.Threading.Thread.Sleep(1000 * 8);
                            int i32 = 1 + 32;
                        }
                    }
                }

                try
                {
                    DeleteAllFilesOfType("zb");
                }
                catch(Exception e)
                {
                }

                System.IO.File.WriteAllText("driver.pid", System.Diagnostics.Process.GetCurrentProcess().Id.ToString() + Environment.NewLine);

                try
                {
                    System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                    xd.Load("serviceconfig.xml");
                    System.Xml.XmlNode xslave = xd["service"];
                    if (null != xslave)
                    {
                        {
                            System.Xml.XmlNode xlog = xslave["log"];
                            if (null != xlog)
                            {
                                System.Xml.XmlAttribute xaenabled = xlog.Attributes["enabled"];
                                if (null != xaenabled)
                                {
                                    XLog.logging = 0 == string.Compare(xaenabled.Value, "true", true);
                                }
                                else
                                {
                                    //XLog.errorlog("slaveconfig.xml: no 'enabled' attribute for tag 'log' in 'service'");
                                }
                            }
                            else
                            {
                                //XLog.errorlog("slaveconfig.xml: no 'log' tag in 'service'");
                            }
                        }
                        {
                            System.Xml.XmlNode xusage = xslave["usage"];
                            if (null != xusage)
                            {
                                System.Xml.XmlAttribute xamins = xusage.Attributes["logminutes"];
                                if (null != xamins)
                                {
                                    logusagemins = int.Parse(xamins.Value);
                                    if (logusagemins > 0)
                                    {
                                        Thread lut = new Thread(new ThreadStart(logusageproc));
                                        lut.IsBackground = true; // !
                                        lut.Start();
                                    }
                                }
                                else
                                {
                                    //XLog.errorlog("slaveconfig.xml: no 'enabled' attribute for tag 'log' in 'service'");
                                }
                            }
                            else
                            {
                                //XLog.errorlog("slaveconfig.xml: no 'log' tag in 'service'");
                            }
                        }
                    }
                    else
                    {
                        //XLog.errorlog("slaveconfig.xml: no 'service' tag");
                    }
                }
                catch (System.Xml.XmlException e)
                {
                    XLog.errorlog("slaveconfig.xml: " + e.ToString());
                }

                lthd = new Thread(new ThreadStart(ListenThreadProc));
                lthd.IsBackground = true; // TEMPORARY, I HOPE HOPE HOPE
                lthd.Start();

                try
                {
                    Thread schedulethd = new Thread(
                        new ThreadStart(
                        delegate
                        {
                            DOService_AddTraceThread(null);
                            System.Threading.Thread.Sleep(1000 * 30);
                            for (; ; )
                            {
                                try
                                {
                                    MySpace.DataMining.DistributedObjects.Scheduler.RunScheduleService();
                                }
                                catch(Exception schedulee)
                                {
                                    XLog.errorlog("Exception during schedule service: " + schedulee.ToString());
                                    System.Threading.Thread.Sleep(1000 * 30);
                                }
                            }
                            DOService_RemoveTraceThread(null);
                        }));
                    schedulethd.Name = "Scheduler_ScheduleService";
                    schedulethd.IsBackground = true;
                    schedulethd.Start();
                    
                    Thread queuethd = new Thread(
                        new ThreadStart(
                        delegate
                        {
                            DOService_AddTraceThread(null);
                            System.Threading.Thread.Sleep(1000 * 30);
                            for (; ; )
                            {
                                try
                                {
                                    MySpace.DataMining.DistributedObjects.Scheduler.RunQueueService();
                                }
                                catch (Exception queuee)
                                {
                                    XLog.errorlog("Exception during queue service: " + queuee.ToString());
                                    System.Threading.Thread.Sleep(1000 * 30);
                                }
                            }
                            DOService_RemoveTraceThread(null);
                        }));
                    queuethd.Name = "Scheduler_QueueService";
                    queuethd.IsBackground = true;
                    queuethd.Start();
                }
                catch (Exception e)
                {
                    XLog.errorlog("OnStart Scheduler exception: " + e.ToString());
                }

                try
                {
                    Thread notifythd = new Thread(
                        new ThreadStart(
                        delegate
                        {
                            DOService_AddTraceThread(null);
                            System.Threading.Thread.Sleep(1000 * 30);
                            for (; ; )
                            {
                                try
                                {
                                    MySpace.DataMining.DistributedObjects.Scheduler.RunNotifierService();
                                }
                                catch (Exception queuee)
                                {
                                    XLog.errorlog("Exception during notifier service: " + queuee.ToString());
                                    System.Threading.Thread.Sleep(1000 * 30);
                                }
                            }
                            DOService_RemoveTraceThread(null);
                        }));
                    notifythd.Name = "NotifierService";
                    notifythd.IsBackground = true;
                    notifythd.Start();
                }
                catch (Exception e)
                {
                    XLog.errorlog("OnStart Notifier exception: " + e.ToString());
                }

                try
                {
                    Thread filescannerthd = new Thread(
                        new ThreadStart(
                        delegate
                        {
                            DOService_AddTraceThread(null);
                            System.Threading.Thread.Sleep(1000 * 30);
                            for (; ; )
                            {
                                try
                                {
                                    MySpace.DataMining.DistributedObjects.FileDaemon.RunScanner();
                                }
                                catch (Exception queuee)
                                {
                                    XLog.errorlog("Exception during file daemon scanner: " + queuee.ToString());
                                    System.Threading.Thread.Sleep(1000 * 30);
                                }
                            }
                            DOService_RemoveTraceThread(null);
                        }));
                    filescannerthd.Name = "FileDaemon_Scanner";
                    filescannerthd.IsBackground = true;
                    filescannerthd.Priority = ThreadPriority.Lowest;
                    filescannerthd.Start();
                }
                catch (Exception e)
                {
                    XLog.errorlog("OnStart Notifier exception: " + e.ToString());
                }

                try
                {
                    Thread filerepairerthd = new Thread(
                        new ThreadStart(
                        delegate
                        {
                            DOService_AddTraceThread(null);
                            System.Threading.Thread.Sleep(1000 * 30);
                            for (; ; )
                            {
                                try
                                {
                                    MySpace.DataMining.DistributedObjects.FileDaemon.RunRepairer();
                                }
                                catch (Exception queuee)
                                {
                                    XLog.errorlog("Exception during file daemon repairer: " + queuee.ToString());
                                    System.Threading.Thread.Sleep(1000 * 30);
                                }
                            }
                            DOService_RemoveTraceThread(null);
                        }));
                    filerepairerthd.Name = "FileDaemon_Repairer";
                    filerepairerthd.IsBackground = true;
                    filerepairerthd.Priority = ThreadPriority.Lowest;
                    filerepairerthd.Start();
                }
                catch (Exception e)
                {
                    XLog.errorlog("OnStart Notifier exception: " + e.ToString());
                }

            }
            catch (Exception e)
            {
                XLog.errorlog("OnStart exception: " + e.ToString());
            }
        }


        protected override void OnStop()
        {
            try
            {
                /*
                if (XLog.logging)
                {
                    XLog.log("Stopping service");
                }
                 * */

                if (null != lthd)
                {
                    lsock.Close();
                    lsock = null;

                    lthd.Abort();
                    lthd = null;
                }

                {
                    using (StreamWriter stoplog = System.IO.File.CreateText("service-stoplog.txt"))
                    {
                        stoplog.WriteLine("[{0}] OnStop kill log:", DateTime.Now.ToString());
                        System.IO.FileInfo[] pidfilesinfo = (new System.IO.DirectoryInfo(".")).GetFiles("*.pid");
                        stoplog.WriteLine("    {0} pid files returned", pidfilesinfo.Length);
                        if (0 != pidfilesinfo.Length)
                        {
                            System.Threading.Thread.Sleep(4000); // Give processes a chance to stop themselves...
                            foreach (System.IO.FileInfo fi in pidfilesinfo)
                            {
                                try
                                {
                                    if (string.Compare(fi.Name, "driver.pid", StringComparison.OrdinalIgnoreCase) == 0)
                                    {
                                        continue;
                                    }
                                    // After the wait, these files might not exist anymore, so check.
                                    stoplog.WriteLine("        {0}", fi.Name);
                                    if (fi.Exists)
                                    {
                                        string spidStop = fi.Name.Substring(0, fi.Name.IndexOf('.'));
                                        int pidStop = int.Parse(spidStop);
                                        Process stopproc = Process.GetProcessById(pidStop);
                                        stopproc.Kill();
                                        stopproc.WaitForExit(1000);
                                        fi.Delete();
                                    }
                                }
                                catch (Exception e)
                                {
                                    XLog.errorlog("Problem while cleaning up " + fi.Name + " on shutdown: " + e.ToString());
                                    try
                                    {
                                        fi.Delete();
                                    }
                                    catch
                                    {
                                    }
                                    stoplog.WriteLine("            ERROR (see errorlog)");
                                }
                            }
                            System.Threading.Thread.Sleep(1000);
                        }
                        {
                            int[] invincibles;
                            {
                                try
                                {
                                    string invfp = "invincible.dat";
                                    string[] sinvs = System.IO.File.ReadAllLines(invfp);
                                    invincibles = new int[sinvs.Length];
                                    for (int i = 0; i < sinvs.Length; i++)
                                    {
                                        int.TryParse(sinvs[i], out invincibles[i]);
                                    }
                                    System.IO.File.Delete(invfp);
                                }
                                catch
                                {
                                    invincibles = new int[0];
                                }
                            }
                            List<System.Diagnostics.Process> procs = new List<Process>();
                            procs.AddRange(System.Diagnostics.Process.GetProcessesByName("aelight"));
                            procs.AddRange(System.Diagnostics.Process.GetProcessesByName("dspace"));
                            procs.AddRange(System.Diagnostics.Process.GetProcessesByName("qizmt"));
                            procs.AddRange(System.Diagnostics.Process.GetProcessesByName("mrdebug"));
                            try
                            {
                                foreach (System.Diagnostics.Process proc in
                                    System.Diagnostics.Process.GetProcesses())
                                {
                                    try
                                    {
                                        string mmname = proc.MainModule.ModuleName;
                                        if (-1 != mmname.IndexOf("dbg_")
                                            && -1 != mmname.IndexOf('~'))
                                        {
                                            procs.Add(proc);
                                        }
                                    }
                                    catch
                                    {
                                    }
                                }
                            }
                            catch
                            {
                            }
                            foreach (System.Diagnostics.Process proc in procs)
                            {
                                if (invincibles.Contains(proc.Id))
                                {
                                    continue;
                                }
                                try
                                {
                                    proc.Kill();
                                    proc.WaitForExit(500);
                                }
                                catch (Exception e)
                                {
                                    XLog.errorlog("Error killing process during service OnStop: " + e.ToString());
                                }
                            }
                        }
                    }

                    if (logusagemins > 0)
                    {
                        logusageshutdown();
                    }
                }

                try
                {
                    System.IO.FileInfo[] pffiles = (new System.IO.DirectoryInfo(".")).GetFiles("*.pf");
                    foreach (System.IO.FileInfo pff in pffiles)
                    {
                        pff.Delete();
                    }
                }
                catch
                {
                }

                try
                {
                    System.IO.FileInfo[] tfiles = (new System.IO.DirectoryInfo(".")).GetFiles("*.trace");
                    foreach (System.IO.FileInfo tf in tfiles)
                    {
                        tf.Delete();
                    }
                }
                catch
                {
                }

                try
                {
                    System.IO.FileInfo[] toffiles = (new System.IO.DirectoryInfo(".")).GetFiles("*.tof");
                    foreach (System.IO.FileInfo tof in toffiles)
                    {
                        tof.Delete();
                    }
                }
                catch
                {
                }

            }
            catch (Exception e)
            {
                XLog.errorlog("OnStop exception: " + e.ToString());
            }

            try
            {
                DeleteAllFilesOfType("zb");
            }
            catch (Exception e)
            {
            }

            try
            {
                System.IO.File.Delete("driver.pid");
            }
            catch
            {
            }

        }
    }


    public static class XLog
    {
        public static bool logging = false;

        internal static Mutex logmutex = new Mutex(false, "distobjlog");


        public static void errorlog(string line)
        {
            try
            {
                logmutex.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                using (System.IO.StreamWriter fstm = System.IO.File.AppendText("errors.txt"))
                {
                    string build = "";
                    try
                    {
                        System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();
                        System.Reflection.AssemblyName an = asm.GetName();
                        int bn = an.Version.Build;
                        int rv = an.Version.Revision;
                        build = "(build:" + bn.ToString() + "." + rv.ToString() + ") ";
                    }
                    catch
                    {
                    }
                    fstm.WriteLine("[{0} {1}ms] DistributedObjectsService error: {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
                    fstm.WriteLine();
                }
            }
            catch
            {
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }


        public static void log(string name, string line)
        {
            try
            {
                logmutex.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                StreamWriter fstm = File.AppendText(name + ".txt");
                fstm.WriteLine("{0}", line);
                fstm.Close();
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }
    }


    public class Block
    {
        public const int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 8;

        NetworkStream dllclientStm;
        string sdllclienthost; // Host of client (DLL).
        string[] blockinfo;
        string name = "(no-block-name)";
        bool added = false;
        byte[] buf;
        VitalsReporter vitals = null;

        internal void abort()
        {
            if (null != dllclientStm)
            {
                dllclientStm.WriteByte((byte)'\\'); // Close signal.
                dllclientStm.Close(1000);
                dllclientStm = null;
            }
            buf = null; // ...            
        }

        internal void cleanup()
        {
            if (vitals != null)
            {
                vitals.Stop();
                vitals = null;
            }
        }

        public Block(Socket dllclientSock)
        {
            this.sdllclienthost = dllclientSock.RemoteEndPoint.ToString();
            {
                int i = this.sdllclienthost.IndexOf(':');
                if (-1 != i)
                {
                    // Strip off port number.
                    this.sdllclienthost = this.sdllclienthost.Substring(0, i);
                }
            }
            this.dllclientStm = new XNetworkStream(dllclientSock);
            this.buf = new byte[DEFAULT_BUFFER_SIZE];
        }


        public void AddingBlock(string[] blockinfo, ushort dllport, string sjid)
        {
            if (added)
            {
                throw new Exception("Sub process already added");
            }

            this.blockinfo = blockinfo;
            this.name = blockinfo[1];
            added = true;

            try
            {
                // args: <host> <portnum> <typechar> <capacity> <logfile> <jid>
                string procname = "MySpace.DataMining.DistributedObjects.DistributedObjectsSlave.exe";
                string sargs = this.sdllclienthost + " " + dllport.ToString() + " \"" + blockinfo[0] + "\" \"" + blockinfo[2] + "\" \"" + blockinfo[4] + "\" \"" + sjid + "\"";
                bool substartlogging = XLog.logging;
#if DEBUGnoisy
                substartlogging = true;
#endif
                if (substartlogging)
                {
#if DEBUGfailstart
                    if ((System.Threading.Thread.CurrentThread.ManagedThreadId % 20) == 0)
                    {
                        XLog.errorlog("DEBUG: NOT Starting sub process \"" + procname + "\" with arguments: " + sargs);
                        return;
                    }
#endif
                    //XLog.log(this.name, "Starting sub process \"" + procname + "\" with arguments: " + sargs);
                    XLog.errorlog("Starting sub process \"" + procname + "\" with arguments: " + sargs);
                }
#if DEBUG_off
                {
                    string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                    if (computer_name == "MAPDCMILLER")
                    {
                        sargs = "\"" + procname + "\" " + sargs;
                        procname = "vsjitdebugger";
                    }
                }
#endif
                lock ("Process.Start{3561B087-424A-4501-972A-693039F7A168}")
                {
                    System.Threading.Thread.Sleep(20);
                }
                ProcessStartInfo psi = new ProcessStartInfo(procname, sargs);
                psi.CreateNoWindow = true;
                psi.UseShellExecute = false;
                Process proc;
                proc = Process.Start(psi);
                //proc.WaitForExit(20);
                //proc.Dispose();
            }
            catch (Exception e)
            {
                throw new Exception("Error adding sub process: " + e.ToString() + "  [Note: ensure sub process exe is present in the service directory]");
            }
        }


        public static bool IsValidDfsFilePath(string fn)
        {
            foreach (char ch in fn)
            {
                if (!char.IsLetterOrDigit(ch)
                    && '_' != ch
                    && '-' != ch
                    && '.' != ch
                    && '[' != ch
                    && ']' != ch
                    && '~' != ch
                    )
                {
                    return false;
                }
            }
            return true;
        }

        public static void ValidateDfsFilePath(string fn)
        {
            if (!IsValidDfsFilePath(fn))
            {
                throw new Exception("Invalid file name for DFS: " + fn);
            }
        }


        class AliveObj
        {
            public AliveObj(NetworkStream nstm, Thread[] threads) { this.nstm = nstm; this.threads = threads; }
            public bool isalive = true;
            public bool isdone = false;
            public NetworkStream nstm;
            public Thread[] threads;
        }

        void pingputthreadproc(object obj)
        {
            try
            {
                AliveObj alive = (AliveObj)obj;
                for (; ; )
                {
                    if (alive.isdone)
                    {
                        return;
                    }
                    System.Threading.Thread.Sleep(1000 * 1);
                    if (alive.isdone)
                    {
                        return;
                    }
                    lock (alive.nstm)
                    {
                        bool success = false;
                        try
                        {
                            alive.nstm.WriteByte((byte)'p'); // ping
                            success = 'g' == alive.nstm.ReadByte(); // pong?
                        }
                        catch
                        {
                        }
                        if (!success)
                        {
                            XLog.errorlog("pingputthreadproc: pong not received for ping");
                            alive.isalive = false;
                            alive.nstm.Close();
                            for (int it = 0; it < alive.threads.Length; it++)
                            {
                                try
                                {
                                    alive.threads[it].Interrupt();
                                }
                                catch
                                {
                                }
                            }
                            return;
                        }
                    }
                }
            }
            catch (ThreadInterruptedException e)
            {
                // Catch my valid Interrupt().
            }
            catch (Exception e)
            {
                string thishost = "NA";
                try
                {
                    thishost = System.Net.Dns.GetHostName();
                }
                catch
                {
                }
                XLog.errorlog("pingputthreadproc thread named " + System.Threading.Thread.CurrentThread.Name + " exception: " + e.ToString() + " [on " + thishost + "]");
            }
        }

        // Same as pingputthreadproc but doesn't wait for pongs (freeing up the socket to be read elsewhere).
        void pingputthreadproc_reqonly(object obj)
        {
            try
            {
                DistributedObjectsService.DOService_AddTraceThread(null);
                AliveObj alive = (AliveObj)obj;
                for (; ; )
                {
                    if (alive.isdone)
                    {
                        return;
                    }
                    System.Threading.Thread.Sleep(1000 * 1);
                    if (alive.isdone)
                    {
                        return;
                    }
                    lock (alive.nstm)
                    {
                        try
                        {
                            alive.nstm.WriteByte((byte)'p'); // ping
                        }
                        catch
                        {
                        }
                    }
                }
            }
            catch (ThreadInterruptedException e)
            {
                // Catch my valid Interrupt().
            }
            catch (Exception e)
            {
                string thishost = "NA";
                try
                {
                    thishost = System.Net.Dns.GetHostName();
                }
                catch
                {
                }
                XLog.errorlog("pingputthreadproc thread named " + System.Threading.Thread.CurrentThread.Name + " exception: " + e.ToString() + " [on " + thishost + "]");
            }
            DistributedObjectsService.DOService_RemoveTraceThread(null);
        }


        void stdoutputthreadproc(object obj)
        {
            DistributedObjectsService.DOService_AddTraceThread(null);
            byte bch = (byte)' ';
            try
            {
                object[] arr = (object[])obj;
                NetworkStream nstm = (NetworkStream)arr[0];
                StreamReader reader = (StreamReader)arr[1];
                bch = (byte)(char)arr[2];
                string nl = Environment.NewLine;
                byte[] buf = new byte[0x400 * 2]; // Needs own buffer due to thread.
                int buflen;
                bool done = false;
                while (!done)
                {
                    buflen = 0;
                    for (; ; )
                    {
                        int iby = reader.Read();
                        if (-1 == iby)
                        {
                            done = true;
                            break;
                        }
                        if ('\u0017' == iby) // ETB
                        {
                            break;
                        }
                        if (buflen >= buf.Length)
                        {
                            byte[] newbuf = new byte[buf.Length * 2];
                            Buffer.BlockCopy(buf, 0, newbuf, 0, buflen);
                            buf = newbuf;
                        }
                        buf[buflen++] = (byte)iby;
                        if ('\n' == iby)
                        {
                            break;
                        }
                    }
                    lock (nstm)
                    {
                        nstm.WriteByte(bch);
                        XContent.SendXContent(nstm, buf, buflen);
                    }
                }
            }
            catch (ThreadInterruptedException e)
            {
                // Catch my valid Interrupt().
            }
            catch (Exception e)
            {
                string thishost = "NA";
                try
                {
                    thishost = System.Net.Dns.GetHostName();
                }
                catch
                {
                }
                //stdoutputthreadproc (char)bch exception: e.ToString()
                XLog.errorlog("stdoutputthreadproc '" + ((char)bch).ToString() + "' thread named " + System.Threading.Thread.CurrentThread.Name + " exception: " + e.ToString() + " [on " + thishost + "]");
            }
            DistributedObjectsService.DOService_RemoveTraceThread(null);
        }


        public void HandleDllClient()
        {
            string s;
            int x;
            //int len;

            //try
            {
                for (bool stop = false; !stop; )
                {
                    try
                    {
                        x = dllclientStm.ReadByte();
                    }
                    catch (System.IO.IOException e)
                    {
                        x = -1;
                    }
                    if (x < 0)
                    {
                        stop = true;
                        break;
                    }
                    else
                    {
                        buf[0] = (byte)x;

                        /*
                        if (XLog.logging)
                        {
                            XLog.log(this.name, "Service relaying command " + ((char)buf[0]).ToString());
                        }
                         * */

                        switch ((char)buf[0])
                        {
                            case 'B': // AddBlock
                                try
                                {
                                    string sblock = XContent.ReceiveXString(dllclientStm, buf);
                                    // For Hashtable: @"H|objectname|200MB|localhost|C:\hashtable_logs0\|slaveid=0"
                                    // For ArrayList: @"A|objectname|..."
                                    /* // This isn't good because it's before this.name is set, so gets written to bad file.
                                    if (XLog.logging)
                                    {
                                        XLog.log(this.name, "AddBlock received: " + sblock);
                                    }
                                     * */
                                    s = XContent.ReceiveXString(dllclientStm, buf);
                                    ushort dllport = ushort.Parse(s);
                                    string sjid = XContent.ReceiveXString(dllclientStm, buf);
                                    AddingBlock(sblock.Split('|'), dllport, sjid);
                                    dllclientStm.WriteByte((byte)'+');
                                }
                                catch(Exception e)
                                {
                                    dllclientStm.WriteByte((byte)'_');
                                    XContent.SendXContent(dllclientStm, e.Message);
                                    throw;
                                }
                                break;

                            case 'd': // Get current directory.
                                dllclientStm.WriteByte((byte)'+');
                                XContent.SendXContent(dllclientStm, Environment.CurrentDirectory);
                                break;

                            case 't': //hard drive speed test.
                                try
                                {
                                    double wt = 0;
                                    double rd = 0;
                                    ulong filesize = ulong.Parse(XContent.ReceiveXString(dllclientStm, null));
                                    SpeedTest.MeasureReadWrite(filesize, ref wt, ref rd);
                                    dllclientStm.WriteByte((byte)'+');
                                    XContent.SendXContent(dllclientStm, wt.ToString());
                                    XContent.SendXContent(dllclientStm, rd.ToString());
                                }
                                catch
                                {
                                    dllclientStm.WriteByte((byte)'-');
                                    throw;
                                } 
                                break;

                            case 'w': //packet sniff
                                try
                                {
                                    int sniffTime = Int32.Parse(XContent.ReceiveXString(dllclientStm, null));
                                    PacketMonitor mon = new PacketMonitor(System.Net.Dns.GetHostName(), Environment.CurrentDirectory);
                                    mon.Start();
                                    System.Threading.Thread.Sleep(sniffTime);
                                    mon.Stop();
                                    dllclientStm.WriteByte((byte)'+');
                                }
                                catch
                                {
                                    dllclientStm.WriteByte((byte)'-');
                                    throw;
                                }
                                break;

                            case 'u': //network speed test.
                                try
                                {
                                    string targetNetPath = XContent.ReceiveXString(dllclientStm, null);
                                    ulong filesize = ulong.Parse(XContent.ReceiveXString(dllclientStm, null));
                                    double d = 0;
                                    double u = 0;
                                    SpeedTest.MeasureNetworkSpeed(targetNetPath, filesize, ref d, ref u);
                                    dllclientStm.WriteByte((byte)'+');
                                    XContent.SendXContent(dllclientStm, d.ToString());
                                    XContent.SendXContent(dllclientStm, u.ToString());
                                }
                                catch
                                {
                                    dllclientStm.WriteByte((byte)'-');
                                    throw;
                                }
                                break;

                            case 'q': //cpu temperature
                                try
                                {
                                    System.Management.ManagementObjectSearcher moSearch = new System.Management.ManagementObjectSearcher(@"root\WMI", "Select * from MSAcpi_ThermalZoneTemperature");
                                    System.Management.ManagementObjectCollection moReturn = moSearch.Get();
                                    double temp = 0;

                                    foreach (System.Management.ManagementObject mo in moReturn)
                                    {
                                        int raw = Convert.ToInt32(mo["CurrentTemperature"]);
                                        temp = raw / 10d;
                                        temp = (temp - 273) * (9d / 5d) + 32;   //Convert to F.                                    
                                    }
                                    dllclientStm.WriteByte((byte)'+');
                                    XContent.SendXContent(dllclientStm, temp.ToString());
                                }
                                catch
                                {
                                    dllclientStm.WriteByte((byte)'-');
                                    throw;
                                }
                                break;

                            case 'y': // Download a file.
                                try
                                {
                                    string srcfn = XContent.ReceiveXString(dllclientStm, buf); // Get this file...
                                    string destfn = XContent.ReceiveXString(dllclientStm, buf); // ...Put it here!
                                    System.IO.File.Copy(srcfn, destfn, true); // Overwrite (easier resume of tasks).
                                    dllclientStm.WriteByte((byte)'+');
                                }
                                catch
                                {
                                    dllclientStm.WriteByte((byte)'-');
                                    throw;
                                }
                                break;

                            case 'n': // Batch download files with fault tolerance
                                {
#if FAILOVER_DEBUG
                                    string debugfile = "";
#endif
                                    try
                                    {                                        
                                        int len;
                                        buf = XContent.ReceiveXBytes(dllclientStm, out len, buf);
                                        int ftretries = MyBytesToInt(buf, 0);
                                        long jid = MySpace.DataMining.DistributedObjects.Entry.BytesToLong(buf, 4);

                                        string[] files = XContent.ReceiveXString(dllclientStm, buf).Split('\u0001');
                                        Dictionary<string, int> tattledhosts = new Dictionary<string, int>(files.Length);


#if FAILOVER_DEBUG
                                        debugfile = @"c:\temp\slavebatchopts_" + jid.ToString() + "_" +
                                        ftretries.ToString() + "_" +
                                        Guid.NewGuid().ToString() + ".txt";
                                        System.IO.File.WriteAllText(debugfile, string.Join(";", files));
#endif

#if TESTFAULTTOLERANT
                                        {
                                            while (System.IO.File.Exists(@"c:\temp\failoverslavetestrep.txt"))
                                            {
                                                System.Threading.Thread.Sleep(10000);
                                            }                                            
                                        }
#endif

                                        

                                        if (files.Length < 2) // Will only be 1 thread here.
                                        {
                                            FTReplication worker = new FTReplication();
                                            worker.allpullfiles = files;
                                            worker.jid = jid;
                                            worker.ftretries = ftretries;
                                            worker.start = 0;
                                            worker.stop = files.Length;
                                            worker.tattledhosts = tattledhosts;
#if TESTFAULTTOLERANT
                                            worker.fttest = new MySpace.DataMining.AELight.FTTest(jid);
#endif
                                            worker.ThreadProc();

#if TESTFAULTTOLERANT
                                            worker.fttest.Close();
#endif
                                        }
                                        else // More than one thread!
                                        {
                                            int nthreads = MyThreadTools.NumberOfProcessors;
                                            int ntasks = files.Length;
                                            int tpt = ntasks / nthreads; // Tasks per thread.
                                            if (0 != (ntasks % nthreads))
                                            {
                                                tpt++;
                                            }
                                            List<FTReplication> ptos = new List<FTReplication>(nthreads);
                                            int offset = 0;
                                            for (int it = 0; offset < ntasks; it++)
                                            {
                                                FTReplication pto = new FTReplication();
                                                pto.allpullfiles = files;
                                                pto.jid = jid;
                                                pto.ftretries = ftretries;
                                                pto.start = offset;
                                                offset += tpt;
                                                if (offset > ntasks)
                                                {
                                                    offset = ntasks;
                                                }
                                                pto.stop = offset;
                                                pto.tattledhosts = tattledhosts;
#if TESTFAULTTOLERANT
                                                pto.fttest = new MySpace.DataMining.AELight.FTTest(jid);
#endif
                                                ptos.Add(pto);
                                                pto.thread = new System.Threading.Thread(new System.Threading.ThreadStart(pto.ThreadProc));
                                                pto.thread.IsBackground = true;
                                                pto.thread.Start();
                                            }
#if FAILOVER_DEBUG
                                            System.IO.File.AppendAllText(debugfile, Environment.NewLine + "repl threads started:" + ptos.Count.ToString());

#endif                                          

                                            for (int i = 0; i < ptos.Count; i++)
                                            {
                                                ptos[i].thread.Join();
#if TESTFAULTTOLERANT
                                                ptos[i].fttest.Close();
#endif
                                                if (ptos[i].exception != null)
                                                {
#if FAILOVER_DEBUG
                                                    System.IO.File.AppendAllText(debugfile, Environment.NewLine + "thread.exception is not null");
#endif
                                                    throw ptos[i].exception;
                                                }
                                            }
                                        }

#if FAILOVER_DEBUG
                                        System.IO.File.AppendAllText(debugfile, Environment.NewLine + "repl threads joined");
#endif
                                        dllclientStm.WriteByte((byte)'+');
                                        string stattledhosts = "";
                                        foreach (KeyValuePair<string, int> pair in tattledhosts)
                                        {
                                            if (stattledhosts.Length > 0)
                                            {
                                                stattledhosts += ";";
                                            }
                                            stattledhosts += pair.Key;
                                        }
                                        XContent.SendXContent(dllclientStm, stattledhosts);

#if FAILOVER_DEBUG
                                        System.IO.File.AppendAllText(debugfile, Environment.NewLine + "tattled:" + stattledhosts);
#endif
                                    }
                                    catch (Exception ex)
                                    {
#if FAILOVER_DEBUG
                                        System.IO.File.AppendAllText(debugfile, Environment.NewLine + "exception caught:" + ex.ToString());
#endif
                                        dllclientStm.WriteByte((byte)'-');
                                        throw;
                                    }
                                }                                
                                break;

                            case 'Y': // Batch download files.
                                try
                                {
                                    int len;
                                    buf = XContent.ReceiveXBytes(dllclientStm, out len, buf);
                                    bool eachfeedback = false; // Send a dot for each file downloaded?
                                    if (len >= 1)
                                    {
                                        eachfeedback = 0 != buf[0];
                                    }
                                    int CookTimeout = 0, CookRetries = 0;
                                    if (len >= 1 + 4 + 4)
                                    {
                                        CookTimeout = MyBytesToInt(buf, 1);
                                        CookRetries = MyBytesToInt(buf, 1 + 4);
                                    }
                                    int cooking_cooksremain = CookRetries;
                                    string[] files = XContent.ReceiveXString(dllclientStm, buf).Split('\u0001');
                                    List<string> errors = new List<string>(files.Length);
                                    const int MAX_SIZE_PER_RECEIVE = 0x400 * 64;

                                    //for (int fi = 0; fi < files.Length; fi++)
                                    MyThreadTools<string>.ParallelWithTrace(
                                        new Action<string>(delegate(string fn)
                                        {                                            
                                            //string fn = files[fi];
                                            string destfn;
                                            {
                                                int ix;
                                                ix = fn.LastIndexOf('\u0002');
                                                if (-1 != ix)
                                                {
                                                    destfn = fn.Substring(ix + 1);
                                                    fn = fn.Substring(0, ix);
                                                }
                                                else
                                                {
                                                    ix = fn.LastIndexOf('\\');
                                                    if (-1 != ix)
                                                    {
                                                        destfn = fn.Substring(ix + 1);
                                                    }
                                                    else
                                                    {
                                                        destfn = fn;
                                                    }
                                                }
                                            }

                                            if (fn.Length < 1 || destfn.Length < 1)
                                            {
                                                throw new Exception("Error: empty file name");
                                            }

                                            try
                                            {
                                                System.IO.File.Copy(fn, destfn, true); // overwrite=true
                                            }                                            
                                            catch
                                            {
                                                bool haserr = false;
                                                using (System.IO.FileStream fdest = new System.IO.FileStream(destfn, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, 0x1000))
                                                {
                                                    System.IO.FileStream fsource = null;
                                                    byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];
                                                    bool cooking_is_read = false;
                                                    bool cooking_is_cooked = false;
                                                    long cooking_pos = 0;
                                                    long prev_cooking_pos = -1;
                                                    int stuckretries = 3;
                                                    int stuckremains = stuckretries;

                                                    for (; ; ) //while cooked
                                                    {
                                                        try
                                                        {
                                                            for (; ; )
                                                            {
                                                                //----------------------------COOKING--------------------------------
                                                                cooking_is_read = true;
                                                                if (cooking_is_cooked)
                                                                {
                                                                    cooking_is_cooked = false;
                                                                    if (fsource != null)
                                                                    {
                                                                        fsource.Close();
                                                                        fsource = null;
                                                                    }                                                                    
                                                                    fsource = new System.IO.FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read, 0x1000);
                                                                    fsource.Seek(cooking_pos, SeekOrigin.Begin);
                                                                }
                                                                //----------------------------COOKING--------------------------------                                                        
                                                                if (fsource == null)
                                                                {
                                                                    fsource = new System.IO.FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read, 0x1000);
                                                                }
                                                                int xread = fsource.Read(fbuf, 0, MAX_SIZE_PER_RECEIVE);
                                                                cooking_pos += xread;
                                                                //----------------------------COOKING--------------------------------
                                                                cooking_is_read = false;
                                                                //----------------------------COOKING--------------------------------
                                                                if (xread <= 0)
                                                                {
                                                                    break;
                                                                }
                                                                fdest.Write(fbuf, 0, xread);
                                                            }
                                                            break;
                                                        }
                                                        catch (Exception e)
                                                        {
                                                            if (!cooking_is_read)
                                                            {
                                                                try
                                                                {
                                                                    XLog.errorlog("Non-reading error; file=" + fn
                                                                        + ";error=" + e.ToString());
                                                                }
                                                                catch
                                                                {
                                                                }
                                                                lock (errors)
                                                                {
                                                                    errors.Add(fn);
                                                                }
                                                                haserr = true;
                                                                break;
                                                            }

                                                            bool firstcook = cooking_cooksremain == CookRetries;
                                                            if (cooking_cooksremain-- <= 0)
                                                            {
                                                                try
                                                                {
                                                                    XLog.errorlog("cooked too many times (retries="
                                                                        + CookRetries.ToString()
                                                                        + "; timeout=" + CookTimeout.ToString()
                                                                        + ") on " + System.Net.Dns.GetHostName()
                                                                        + "; file=" + fn + ";error=" + e.ToString());
                                                                }
                                                                catch
                                                                {
                                                                }
                                                                lock (errors)
                                                                {
                                                                    errors.Add(fn);
                                                                }
                                                                haserr = true;
                                                                break;
                                                            }

                                                            bool dosleep = true;

                                                            //-------------bad sector-----------------                                                        
                                                            if (prev_cooking_pos != cooking_pos)
                                                            {
                                                                prev_cooking_pos = cooking_pos;
                                                                stuckremains = stuckretries;
                                                            }
                                                            else if (cooking_pos > 2) //make sure it is not at the begining of the file.
                                                            {
                                                                dosleep = false;
                                                                if (stuckremains-- <= 0)
                                                                {
                                                                    try
                                                                    {
                                                                        XLog.errorlog("Bad sector detected at byte position=" + cooking_pos.ToString()
                                                                           + "; file=" + fn
                                                                           + "; worker host=" + System.Net.Dns.GetHostName() + ";error=" + e.ToString());
                                                                    }
                                                                    catch
                                                                    {
                                                                    }
                                                                    lock (errors)
                                                                    {
                                                                        errors.Add(fn);
                                                                    }
                                                                    haserr = true;
                                                                    break;
                                                                }
                                                            }
                                                            //-------------bad sector-----------------

                                                            if (dosleep)
                                                            {
                                                                System.Threading.Thread.Sleep(MyRealRetryTimeout(CookTimeout));
                                                            }

                                                            if (firstcook)
                                                            {
                                                                try
                                                                {
                                                                    XLog.errorlog("cooking started (retries=" + CookRetries.ToString()
                                                                        + "; timeout=" + CookTimeout.ToString()
                                                                        + ") on " + System.Net.Dns.GetHostName()
                                                                        + "; file=" + fn
                                                                        + "; cooking_pos=" + cooking_pos.ToString()
                                                                        + "; prev_cooking_pos=" + prev_cooking_pos.ToString()
                                                                        + "; stuckremains=" + stuckremains.ToString()
                                                                        + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod()
                                                                        + "; error: " + e.ToString());
                                                                }
                                                                catch
                                                                {
                                                                }
                                                            }
                                                            else
                                                            {
                                                                try
                                                                {
                                                                    if ((CookRetries - (cooking_cooksremain + 1)) % 60 == 0)
                                                                    {
                                                                        XLog.errorlog("cooking continues with " + cooking_cooksremain
                                                                            + " more retries (retries=" + CookRetries.ToString()
                                                                            + "; timeout=" + CookTimeout.ToString()
                                                                            + ") on " + System.Net.Dns.GetHostName()
                                                                            + "; file=" + fn
                                                                            + "; cooking_pos=" + cooking_pos.ToString()
                                                                            + "; prev_cooking_pos=" + prev_cooking_pos.ToString()
                                                                            + "; stuckremains=" + stuckremains.ToString()
                                                                            + " in " + (new System.Diagnostics.StackTrace()).GetFrame(0).GetMethod()
                                                                            + "; error: " + e.ToString()
                                                                            + Environment.NewLine + e.ToString());
                                                                    }
                                                                }
                                                                catch
                                                                {
                                                                }
                                                            }
                                                            cooking_is_cooked = true;
                                                            continue;
                                                        }
                                                    }
                                                }

                                                if (haserr)
                                                {
                                                    System.IO.File.Delete(destfn);
                                                }
                                            }                                            
                                            
                                            if (eachfeedback)
                                            {
                                                lock (dllclientStm)
                                                {
                                                    dllclientStm.WriteByte((byte)'.');
                                                }
                                            }
                                        }), files);

                                    if (errors.Count > 0)
                                    {
                                        dllclientStm.WriteByte((byte)'e');
                                        StringBuilder sberr = new StringBuilder(1024);
                                        foreach(string err in errors)
                                        {
                                            if(sberr.Length > 0)
                                            {
                                                sberr.Append(';');
                                            }
                                            sberr.Append(err);
                                        }
                                        XContent.SendXContent(dllclientStm, sberr.ToString());
                                    }
                                    else
                                    {
                                        dllclientStm.WriteByte((byte)'+');
                                    }
                                }
                                catch
                                {
                                    dllclientStm.WriteByte((byte)'-');
                                    throw;
                                }
                                break;

                            case 'a': // Add rogue hosts
                                {
                                    int len;
                                    buf = XContent.ReceiveXBytes(dllclientStm, out len, buf);
                                    long jobid = MySpace.DataMining.DistributedObjects.Entry.BytesToLong(buf, 0);
                                    string[] roguehosts = XContent.ReceiveXString(dllclientStm, buf).Split(';');
                                    foreach (string rh in roguehosts)
                                    {
                                        RogueHosts.Add(jobid, rh);
                                    }
                                }
                                break;

                            case 'h': // Start vitals reporter
                                {
                                    if (vitals != null)
                                    {
                                        vitals.Stop();
                                        vitals = null;
                                    }

                                    int len;
                                    buf = XContent.ReceiveXBytes(dllclientStm, out len, buf);
                                    long jobid = MySpace.DataMining.DistributedObjects.Entry.BytesToLong(buf, 0);
                                    int heartbeattimeout = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(buf, 8);
                                    int heartbeatretries = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(buf, 8 + 4);
                                    int tattletimeout = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(buf, 8 + 4 + 4);

                                    vitals = new VitalsReporter(jobid, dllclientStm);
                                    vitals.Start(heartbeattimeout, heartbeatretries, tattletimeout);                           
                                }
                                break;

                            case 's': // Stop vitals reporter
                                {
                                    if (vitals != null)
                                    {
                                        vitals.Stop();
                                        vitals = null;
                                    }
                                }
                                break;

#if TESTFAULTTOLERANT
                            case '1': // Fault tolerant test
                                {
                                    string[] opts = XContent.ReceiveXString(dllclientStm, buf).Split(':');
                                    long jid = Int64.Parse(opts[0]);
                                    string host = opts[1].ToLower();
                                    string phase = opts[2].ToLower();
                                    int max = Int32.Parse(opts[3]);                                    
                                    string controlfile = @"c:\temp\fttestcount_23014BB4-9383-406e-BD0E-49CA2E10F244.txt";                                    
                                    lock ("{435C4771-8AD0-4659-B6D5-F27218F30550}")
                                    {
                                        int curcount = 0;
                                        bool thrownbefore = false;
                                        if (System.IO.File.Exists(controlfile))
                                        {
                                            string[] lines = System.IO.File.ReadAllLines(controlfile);
                                            foreach (string line in lines)
                                            {
                                                string[] parts = line.Split(':');
                                                long thisjid = Int64.Parse(parts[0]);
                                                string thishost = parts[1].ToLower();
                                                string thisphase = parts[2].ToLower();
                                                if (thisjid == jid)
                                                {
                                                    if (thisphase == phase)
                                                    {
                                                        if (host == thishost) //host has already thrown exception before for this phase.
                                                        {
                                                            thrownbefore = true;
                                                            break;
                                                        }
                                                        curcount++;
                                                    }
                                                }                                                
                                            }
                                        }
                                        
                                        if (thrownbefore) //continue to throw for this host, don't need to update control file.
                                        {
                                            dllclientStm.WriteByte((byte)'+');
                                        }
                                        else if (curcount < max) //host has not thrown before, let it.
                                        {
                                            System.IO.File.AppendAllText(controlfile, jid.ToString() + ":" + host + ":" + phase + Environment.NewLine);
                                            dllclientStm.WriteByte((byte)'+');
                                        }
                                        else
                                        {
                                            dllclientStm.WriteByte((byte)'-');
                                        }
                                    }
                                }
                                break;
#endif

                            case 'v': // Verify write/read/delete file.
                                {
                                    string f = XContent.ReceiveXString(dllclientStm, buf);
                                    try
                                    {
                                        using (System.IO.FileStream fs = new FileStream(f, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None)) // Force new.
                                        {
                                            fs.WriteByte((byte)'T');
                                            fs.WriteByte((byte)'e');
                                            fs.WriteByte((byte)'s');
                                            fs.WriteByte((byte)'t');
                                            fs.Seek(0, SeekOrigin.Begin);
                                            if ((int)'T' != fs.ReadByte()
                                                || (int)'e' != fs.ReadByte()
                                                || (int)'s' != fs.ReadByte()
                                                || (int)'t' != fs.ReadByte()
                                                || fs.Length != 4)
                                            {
                                                throw new System.IO.IOException("'v' Test data written was not read back correctly");
                                            }
                                        }
                                        System.IO.File.Delete(f);
                                        if (System.IO.File.Exists(f))
                                        {
                                            throw new System.IO.IOException("'v' file still exists");
                                        }

                                        dllclientStm.WriteByte((byte)'+');
                                    }
                                    catch
                                    {
                                        dllclientStm.WriteByte((byte)'-');
                                        throw;
                                    }
                                }
                                break;

                            case 'M':
                                {
                                    MEMORYSTATUSEX mem = GetMemoryStatus();
                                    StringBuilder sb = new StringBuilder(100);
                                    sb.AppendFormat("MemoryLoad: {0}%\r\n", mem.dwMemoryLoad);
                                    sb.AppendFormat("TotalPhys: {0}\r\n", mem.ullTotalPhys);
                                    sb.AppendFormat("AvailPhys: {0}\r\n", mem.ullAvailPhys);
                                    sb.AppendFormat("TotalPageFile: {0}\r\n", mem.ullTotalPageFile);
                                    sb.AppendFormat("AvailPageFile: {0}\r\n", mem.ullAvailPageFile);
                                    sb.AppendFormat("TotalVirtual: {0}\r\n", mem.ullTotalVirtual);
                                    sb.AppendFormat("AvailVirtual: {0}\r\n", mem.ullAvailVirtual);
                                    sb.AppendFormat("AvailExtendedVirtual: {0}\r\n", mem.ullAvailExtendedVirtual);
                                    XContent.SendXContent(dllclientStm, sb.ToString());
                                }
                                break;

                            case 'J': //Hand shake with client for cluster-wide lock
                                {
                                    dllclientStm.WriteByte((byte)'+');                                               
                                }
                                break;

                            case 'Q':
                                {
                                    System.Threading.Mutex mutex = new System.Threading.Mutex(false, @"DEClusterM");
                                    try
                                    {
                                        try
                                        {
                                            mutex.WaitOne();
                                        }
                                        catch (System.Threading.AbandonedMutexException e)
                                        {                                            
                                        }
                                        dllclientStm.WriteByte((byte)'+');
                                        dllclientStm.ReadByte();
                                        mutex.ReleaseMutex();
                                        mutex = null;
                                        dllclientStm.WriteByte((byte)'+');
                                    }
                                    catch (Exception e)
                                    {
                                        XLog.errorlog("Problem encountered while acquiring cluster-wide mutex \"DEClusterM\":  " + e.ToString());
                                    }
                                    finally
                                    {
                                        if (mutex != null)
                                        {
                                            mutex.ReleaseMutex();
                                            mutex = null;
                                        }                                        
                                    }                 
                                }
                                break;

                            case (char)(128 + 'Q'): // 128+Q: Named global lock.
                                {
                                    string lockname = XContent.ReceiveXString(dllclientStm, buf);
                                    System.Threading.Mutex mutex = new System.Threading.Mutex(false, @"DEClusterM_" + lockname);
                                    try
                                    {
                                        try
                                        {
                                            mutex.WaitOne();
                                        }
                                        catch (System.Threading.AbandonedMutexException e)
                                        {
                                        }
                                        dllclientStm.WriteByte((byte)'+');
                                        dllclientStm.ReadByte();
                                        mutex.ReleaseMutex();
                                        mutex = null;
                                        dllclientStm.WriteByte((byte)'+');
                                    }
                                    catch (Exception e)
                                    {
                                        XLog.errorlog("Problem encountered while acquiring named cluster-wide mutex \"DEClusterM_" + lockname + "\":  " + e.ToString());
                                    }
                                    finally
                                    {
                                        if (mutex != null)
                                        {
                                            mutex.ReleaseMutex();
                                            mutex = null;
                                        }
                                    }
                                }
                                break;

                            case '%': // Standard input and output (bi-directional).
                                try
                                {
                                    //"[\"-DOSLAVE<X>\" ][-ASYNC ]<winusername>: <app> <sargs> "
                                    string str = XContent.ReceiveXString(dllclientStm, buf);
                                    int i;

                                    string doslavevalue = null;
                                    if (str.StartsWith("\"-DOSLAVE"))
                                    {
                                        int iend = str.IndexOf('"', 9);
                                        doslavevalue = str.Substring(9, iend);
                                        str = str.Substring(iend + 1).TrimStart();
                                    }

                                    bool async = false;
                                    if (str.StartsWith("-ASYNC "))
                                    {
                                        async = true;
                                        str = str.Substring(7);
                                    }

                                    i = str.IndexOf(": ");
                                    string winusername = str.Substring(0, i);
                                    str = str.Substring(i + 2);

                                    i = str.IndexOf(' ');
                                    string app = str.Substring(0, i);
                                    string sargs = str.Substring(i + 1);

                                    string newsargs = sargs;
                                    if (app.StartsWith("aelight", StringComparison.OrdinalIgnoreCase))
                                    {
                                        newsargs = "\"-$" + winusername + "\" " + sargs;
                                    }
                                    System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(app, newsargs);
                                    if (null != doslavevalue)
                                    {
                                        psi.EnvironmentVariables.Add("DOSLAVE", doslavevalue);
                                    }
                                    psi.UseShellExecute = false;
                                    psi.CreateNoWindow = true;
                                    if (!async)
                                    {
                                        psi.StandardOutputEncoding = Encoding.UTF8;
                                        psi.RedirectStandardOutput = true;
                                        psi.RedirectStandardError = true;
                                        psi.RedirectStandardInput = true;
                                    }
                                    System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
                                    dllclientStm.WriteByte((byte)'+'); // !

                                    if (async)
                                    {
                                        proc.Close();
                                        dllclientStm.WriteByte((byte)'o');
                                        XContent.SendXContent(dllclientStm, "Asynchronous command pending");
                                        MyToBytes(0, buf, 0); // Async exit code.
                                        dllclientStm.WriteByte((byte)'r');
                                        XContent.SendXContent(dllclientStm, buf, 4);
                                        dllclientStm.Close(500);
                                        return;
                                    }

                                    string tname = System.Threading.Thread.CurrentThread.Name;

                                    Thread outthread = new Thread(new ParameterizedThreadStart(stdoutputthreadproc));
                                    outthread.Name = "stdoutputthread_from" + tname;
                                    //outthread.IsBackground = false; // ! need to hold the process up to get these replies. (old)
                                    outthread.IsBackground = true;
                                    outthread.Start(new object[] { dllclientStm, proc.StandardOutput, 'o' });

                                    Thread errthread = new Thread(new ParameterizedThreadStart(stdoutputthreadproc));
                                    errthread.Name = "stderrorthread_from" + tname;
                                    errthread.IsBackground = true;
                                    errthread.Start(new object[] { dllclientStm, proc.StandardError, 'e' });

                                    AliveObj alive = new AliveObj(dllclientStm, new Thread[] { outthread, errthread });
                                    Thread pingthread = new Thread(new ParameterizedThreadStart(pingputthreadproc_reqonly));
                                    pingthread.Name = "pingputthread_from" + tname;
                                    pingthread.IsBackground = true;
                                    pingthread.Start(alive);

                                    Thread inthread = null;
                                    {
                                        byte[] inbuf = new byte[buf.Length];
                                        inthread = new Thread(new ThreadStart(
                                            delegate()
                                            {
                                                try
                                                {
                                                    DistributedObjectsService.DOService_AddTraceThread(null);
                                                    while (!alive.isdone)
                                                    {
                                                        if (1 != dllclientStm.Read(inbuf, 0, 1))
                                                        {
                                                            break;
                                                        }
                                                        switch ((char)inbuf[0])
                                                        {
                                                            case '\u0007':
                                                                {
                                                                    string sin = XContent.ReceiveXString(dllclientStm, inbuf);
                                                                    proc.StandardInput.Write(sin);
                                                                }
                                                                break;

                                                            case 'g':
                                                                // Pong...
                                                                break;

                                                            default:
                                                                throw new Exception("Invalid inthread tag: " + ((char)inbuf[0]).ToString() + " (or pong not received for ping)");
                                                        }
                                                    }
                                                }
                                                catch
                                                {
                                                }
                                                DistributedObjectsService.DOService_RemoveTraceThread(null);
                                            }));
                                        inthread.Name = "inthread_from" + tname;
                                        inthread.IsBackground = true;
                                        inthread.Start();
                                    }

                                    try
                                    {
                                        for (; ; )
                                        {
                                            if (outthread.Join(200))
                                            {
                                                //XLog.errorlog("note: `" + app + " " + sargs + "` exec: outthread joined first"); // ...
                                                break;
                                            }
                                            if (errthread.Join(200))
                                            {
                                                //XLog.errorlog("note: `" + app + " " + sargs + "` exec: errthread joined first"); // ...
                                                break;
                                            }
                                            if (null != inthread)
                                            {
                                                if (inthread.Join(200))
                                                {
                                                    //XLog.errorlog("note: `" + app + " " + sargs + "` exec: inthread joined first"); // ...
                                                    break;
                                                }
                                            }
                                            if (pingthread.Join(200))
                                            {
                                                //XLog.errorlog("note: `" + app + " " + sargs + "` exec: pingthread joined first"); // ...
                                                break;
                                            }
                                        }
                                        alive.isdone = true;
                                        if (alive.isalive)
                                        {
                                            if (!outthread.Join(1000))
                                            {
                                                alive.isalive = false;
                                                outthread.Interrupt();
                                            }
                                            if (!errthread.Join(1000))
                                            {
                                                alive.isalive = false;
                                                errthread.Interrupt();
                                            }
                                            pingthread.Interrupt();
                                            if (null != inthread)
                                            {
                                                inthread.Interrupt();
                                            }
                                        }
                                        else
                                        {
                                            //XLog.errorlog("warning: `" + app + " " + sargs + "` exec no longer alive, interrupting client threads"); // ...
                                            outthread.Interrupt();
                                            errthread.Interrupt();
                                            pingthread.Interrupt();
                                            if (null != inthread)
                                            {
                                                inthread.Interrupt();
                                            }
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        alive.isalive = false;
                                        XLog.errorlog(e.ToString());
                                    }
                                    if (alive.isalive)
                                    {
                                        if (!proc.WaitForExit(1000))
                                        {
                                            alive.isalive = false;
                                        }
                                    }
                                    if (alive.isalive)
                                    {
                                        //XLog.errorlog("note: `" + app + " " + sargs + "` exec finished, ExitCode ('r'): " + proc.ExitCode.ToString()); // ...
                                        lock (dllclientStm)
                                        {
                                            MyToBytes(proc.ExitCode, buf, 0);
                                            dllclientStm.WriteByte((byte)'r');
                                            XContent.SendXContent(dllclientStm, buf, 4);
                                        }

                                        proc.Close();
                                    }
                                    else
                                    {
                                        try
                                        {
                                            proc.Kill();
                                        }
                                        catch (Exception e)
                                        {
                                            XLog.errorlog("`" + app + " " + sargs + "` exec: non-alive proc kill failure: " + e.ToString());
                                        }
                                    }
                                    dllclientStm.Close(500);
                                    return;
                                }
                                catch (IOException e)
                                {
                                    //XLog.errorlog("exec general IOException: " + e.ToString()); // ...
                                    try
                                    {
                                        lock (dllclientStm)
                                        {
                                            dllclientStm.WriteByte((byte)'-');
                                        }
                                    }
                                    catch
                                    {
                                    }
                                    if (e.InnerException.GetType() != typeof(SocketException))
                                    {
                                        throw;
                                    }
#if DEBUG
                                    XLog.errorlog("DistributedObjectsService Warning: IOException+SocketException during %BiDiExecIO: " + e.ToString());
#endif
                                }
                                catch
                                {
                                    lock (dllclientStm)
                                    {
                                        dllclientStm.WriteByte((byte)'-');
                                    }
                                    throw;
                                }
                                break;

                            case 'U': //start DProcess
                                {
                                    System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(@"DProcess.exe", "");
                                    psi.UseShellExecute = false;
                                    psi.CreateNoWindow = true;                                    
                                    System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
                                    proc.Close();
                                    dllclientStm.WriteByte((byte)'+');
                                    dllclientStm.Close();
                                    return;                                    
                                }
                                break;

                            case '$': // Standard output only.
                                try
                                {
                                    //"[\"-DOSLAVE<X>\" ][-ASYNC ]<winusername>: <app> <sargs> "
                                    string str = XContent.ReceiveXString(dllclientStm, buf);
                                    int i;

                                    string doslavevalue = null;
                                    if (str.StartsWith("\"-DOSLAVE"))
                                    {
                                        int iend = str.IndexOf('"', 9);
                                        doslavevalue = str.Substring(9, iend);
                                        str = str.Substring(iend + 1).TrimStart();
                                    }

                                    bool async = false;
                                    if (str.StartsWith("-ASYNC "))
                                    {
                                        async = true;
                                        str = str.Substring(7);
                                    }

                                    i = str.IndexOf(": ");
                                    string winusername = str.Substring(0, i);
                                    str = str.Substring(i + 2);

                                    i = str.IndexOf(' ');
                                    string app = str.Substring(0, i);
                                    string sargs = str.Substring(i + 1);

                                    string newsargs = sargs;
                                    if (app.StartsWith("aelight", StringComparison.OrdinalIgnoreCase))
                                    {
                                        newsargs = "\"-$" + winusername + "\" " + sargs;
                                    }
                                    System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(app, newsargs);
                                    if (null != doslavevalue)
                                    {
                                        psi.EnvironmentVariables.Add("DOSLAVE", doslavevalue);
                                    }
                                    psi.UseShellExecute = false;
                                    psi.CreateNoWindow = true;
                                    if (!async)
                                    {
                                        psi.StandardOutputEncoding = Encoding.UTF8;
                                        psi.RedirectStandardOutput = true;
                                        psi.RedirectStandardError = true;
                                        //psi.RedirectStandardInput = true;
                                    }
                                    System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
                                    dllclientStm.WriteByte((byte)'+'); // !

                                    if (async)
                                    {
                                        proc.Close();
                                        dllclientStm.WriteByte((byte)'o');
                                        XContent.SendXContent(dllclientStm, "Asynchronous command pending");
                                        MyToBytes(0, buf, 0); // Async exit code.
                                        dllclientStm.WriteByte((byte)'r');
                                        XContent.SendXContent(dllclientStm, buf, 4);
                                        dllclientStm.Close(500);
                                        return;
                                    }

                                    string tname = System.Threading.Thread.CurrentThread.Name;

                                    Thread outthread = new Thread(new ParameterizedThreadStart(stdoutputthreadproc));
                                    outthread.Name = "stdoutputthread_from" + tname;
                                    outthread.IsBackground = false; // ! need to hold the process up to get these replies.
                                    outthread.Start(new object[] { dllclientStm, proc.StandardOutput, 'o' });

                                    Thread errthread = new Thread(new ParameterizedThreadStart(stdoutputthreadproc));
                                    errthread.Name = "stderrorthread_from" + tname;
                                    errthread.IsBackground = true;
                                    errthread.Start(new object[] { dllclientStm, proc.StandardError, 'e' });

                                    AliveObj alive = new AliveObj(dllclientStm, new Thread[] { outthread, errthread });
                                    Thread pingthread = new Thread(new ParameterizedThreadStart(pingputthreadproc));
                                    pingthread.Name = "pingputthread_from" + tname;
                                    pingthread.IsBackground = true;
                                    pingthread.Start(alive);

                                    try
                                    {
                                        for (; ; )
                                        {
                                            if (outthread.Join(500))
                                            {
                                                //XLog.errorlog("note: `" + app + " " + sargs + "` exec: outthread joined first"); // ...
                                                break;
                                            }
                                            if (errthread.Join(500))
                                            {
                                                //XLog.errorlog("note: `" + app + " " + sargs + "` exec: errthread joined first"); // ...
                                                break;
                                            }
                                            if (pingthread.Join(500))
                                            {
                                                //XLog.errorlog("note: `" + app + " " + sargs + "` exec: pingthread joined first"); // ...
                                                break;
                                            }
                                        }
                                        alive.isdone = true;
                                        if (alive.isalive)
                                        {
                                            outthread.Join();
                                            errthread.Join();
                                            pingthread.Interrupt();
                                        }
                                        else
                                        {
                                            //XLog.errorlog("warning: `" + app + " " + sargs + "` exec no longer alive, interrupting client threads"); // ...
                                            outthread.Interrupt();
                                            errthread.Interrupt();
                                            pingthread.Interrupt();
                                        }
                                    }
                                    catch(Exception e)
                                    {
                                        alive.isalive = false;
                                        XLog.errorlog(e.ToString());
                                    }
                                    if (alive.isalive)
                                    {
                                        proc.WaitForExit();

                                        //XLog.errorlog("note: `" + app + " " + sargs + "` exec finished, ExitCode ('r'): " + proc.ExitCode.ToString()); // ...
                                        lock (dllclientStm)
                                        {
                                            MyToBytes(proc.ExitCode, buf, 0);
                                            dllclientStm.WriteByte((byte)'r');
                                            XContent.SendXContent(dllclientStm, buf, 4);
                                        }

                                        proc.Close();
                                    }
                                    else
                                    {
                                        try
                                        {
                                            proc.Kill();
                                        }
                                        catch(Exception e)
                                        {
                                            XLog.errorlog("`" + app + " " + sargs + "` exec: non-alive proc kill failure: " + e.ToString());
                                        }
                                    }
                                    dllclientStm.Close(500);
                                    return;
                                }
                                catch (IOException e)
                                {
                                    //XLog.errorlog("exec general IOException: " + e.ToString()); // ...
                                    try
                                    {
                                        lock (dllclientStm)
                                        {
                                            dllclientStm.WriteByte((byte)'-');
                                        }
                                    }
                                    catch
                                    {
                                    }
                                    if (e.InnerException.GetType() != typeof(SocketException))
                                    {
                                        throw;
                                    }
#if DEBUG
                                    XLog.errorlog("DistributedObjectsService Warning: IOException+SocketException during $UniDiExecIO: " + e.ToString());
#endif
                                }
                                catch
                                {
                                    lock (dllclientStm)
                                    {
                                        dllclientStm.WriteByte((byte)'-');
                                    }
                                    throw;
                                }
                                break;

                            case 'k': // Kill a slave!
                                try
                                {
                                    string spidStop = XContent.ReceiveXString(dllclientStm, buf);
                                    int pidStop = int.Parse(spidStop);
                                    spidStop = pidStop.ToString(); // Normalize.
                                    {
                                        // Ensure it's a slave process...
                                        if (0 == System.IO.Directory.GetFiles(".", spidStop + ".j*.slave.pid").Length)
                                        {
                                            throw new Exception("kill request is not a slave process; PID: " + spidStop);
                                        }
                                    }
                                    System.Diagnostics.Process killproc = System.Diagnostics.Process.GetProcessById(pidStop);
                                    killproc.Kill();
                                    killproc.WaitForExit(1000 * 5);
                                    killproc.Close();
                                    dllclientStm.WriteByte((byte)'+');
                                }
                                catch
                                {
                                    dllclientStm.WriteByte((byte)'-');
                                    throw;
                                }
                                break;

                            case 'D': // DFS command...
                                x = dllclientStm.ReadByte();
                                if (x < 0)
                                {
                                    stop = true;
                                    break;
                                }
                                else
                                {
                                    buf[1] = (byte)x;
                                    switch ((char)buf[1])
                                    {
                                        case 'D': // Delete...
                                            try
                                            {
                                                string fn = XContent.ReceiveXString(dllclientStm, buf);
                                                ValidateDfsFilePath(fn);

                                                System.IO.File.Delete(fn);

                                                dllclientStm.WriteByte((byte)'+');
                                            }
                                            catch
                                            {
                                                try
                                                {
                                                    dllclientStm.WriteByte((byte)'-');
                                                }
                                                catch
                                                {
                                                }
                                                throw;
                                            }
                                            break;

                                        case 'G': // Get...
                                            try
                                            {
                                                string fn = XContent.ReceiveXString(dllclientStm, buf);
                                                ValidateDfsFilePath(fn);

                                                using (System.IO.FileStream fs = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read, DEFAULT_BUFFER_SIZE))
                                                {
                                                    long fpartlen = fs.Length;

                                                    const int MAX_SIZE_PER_RECEIVE = 0x400 * 64;
                                                    byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];

                                                    dllclientStm.WriteByte((byte)'+');

                                                    {
                                                        byte[] balen = XContent.IntHexBytes(fpartlen);
                                                        dllclientStm.Write(balen, 0, balen.Length);
                                                    }

                                                    long fremain = fpartlen;
                                                    while (fremain > 0)
                                                    {
                                                        int xread = fs.Read(fbuf, 0, (fremain > MAX_SIZE_PER_RECEIVE) ? MAX_SIZE_PER_RECEIVE : (int)fremain);
                                                        if (xread <= 0)
                                                        {
                                                            throw new Exception("Problem reading file for DFS.Get: " + fn);
                                                        }
                                                        dllclientStm.Write(fbuf, 0, xread);
                                                        fremain -= xread;
                                                    }
                                                }
                                            }
                                            catch
                                            {
                                                try
                                                {
                                                    dllclientStm.WriteByte((byte)'-');
                                                }
                                                catch
                                                {
                                                }
                                                throw;
                                            }
                                            break;

                                        case 'P': // Put...
                                            {
                                                try
                                                {
                                                    string fn = XContent.ReceiveXString(dllclientStm, buf);
                                                    ValidateDfsFilePath(fn);

                                                    using (System.IO.FileStream fs = new System.IO.FileStream(fn, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, DEFAULT_BUFFER_SIZE))
                                                    {
                                                        // File header is part of the content from the sender.
                                                        const int MAX_SIZE_PER_RECEIVE = 0x400 * 64;
                                                        byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];
                                                        long fpartlen = XContent.ReceiveXLength(dllclientStm, buf);
                                                        while (fpartlen > 0)
                                                        {
                                                            int rlen = dllclientStm.Read(fbuf, 0, (fpartlen > MAX_SIZE_PER_RECEIVE) ? MAX_SIZE_PER_RECEIVE : (int)fpartlen);
                                                            if (0 == rlen)
                                                            {
                                                                throw new Exception("Unable to receive DFS file");
                                                            }
                                                            fs.Write(fbuf, 0, rlen);
                                                            fpartlen -= rlen;
                                                        }
                                                    }

                                                    dllclientStm.WriteByte((byte)'+');
                                                    
                                                    System.GC.Collect();
                                                    //System.GC.WaitForPendingFinalizers();
                                                }
                                                catch
                                                {
                                                    try
                                                    {
                                                        dllclientStm.WriteByte((byte)'-');
                                                    }
                                                    catch
                                                    {
                                                    }
                                                    throw;
                                                }
                                            }
                                            break;

                                        default:
                                            throw new Exception("DFS tag (D) error: received unknown sub-tag: " + buf[1].ToString() + " 0x" + buf[1].ToString("X2"));
                                    }
                                }
                                break;

                            case 'C': // MemCache
                                {
                                    switch (dllclientStm.ReadByte())
                                    {
                                        case 'f': // Flush MemCache.
                                            try
                                            {
                                                string mcname = XContent.ReceiveXString(dllclientStm, buf);
                                                List<KeyValuePair<string, int>> flushinfo = new List<KeyValuePair<string, int>>();
                                                MySpace.DataMining.DistributedObjects.MemCache.FlushInternal(mcname, flushinfo);
                                                dllclientStm.WriteByte((byte)'+');
                                                {
                                                    StringBuilder sbfi = new StringBuilder(1024);
                                                    foreach (KeyValuePair<string, int> kvp in flushinfo)
                                                    {
                                                        if (0 != sbfi.Length)
                                                        {
                                                            sbfi.Append('\n');
                                                        }
                                                        sbfi.Append(kvp.Key);
                                                        sbfi.Append(' ');
                                                        sbfi.Append(kvp.Value);
                                                    }
                                                    XContent.SendXContent(dllclientStm, sbfi.ToString());
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                try
                                                {
                                                    dllclientStm.WriteByte((byte)'-');
                                                    XContent.SendXContent(dllclientStm, e.ToString());
                                                }
                                                catch
                                                {
                                                    throw e;
                                                }
                                            }
                                            break;

                                        case 'r': // Release MemCache.
                                            try
                                            {
                                                string mcname = XContent.ReceiveXString(dllclientStm, buf);
                                                int len;
                                                buf = XContent.ReceiveXBytes(dllclientStm, out len, buf);
                                                bool force = len > 0 && buf[0] != 0;
                                                MySpace.DataMining.DistributedObjects.MemCache.ReleaseInternal(mcname, force);
                                                dllclientStm.WriteByte((byte)'+');
                                            }
                                            catch (Exception e)
                                            {
                                                try
                                                {
                                                    dllclientStm.WriteByte((byte)'-');
                                                    XContent.SendXContent(dllclientStm, e.ToString());
                                                }
                                                catch
                                                {
                                                    throw e;
                                                }
                                            }
                                            break;

                                        case 'l': // Load MemCache.
                                            try
                                            {
                                                string mcname = XContent.ReceiveXString(dllclientStm, buf);
                                                MySpace.DataMining.DistributedObjects.MemCache.LoadInternal(mcname);
                                                dllclientStm.WriteByte((byte)'+');
                                            }
                                            catch (Exception e)
                                            {
                                                try
                                                {
                                                    dllclientStm.WriteByte((byte)'-');
                                                    XContent.SendXContent(dllclientStm, e.ToString());
                                                }
                                                catch
                                                {
                                                    throw e;
                                                }
                                            }
                                            break;
                                    }
                                }
                                break;

                            case '\\':
                                stop = true;
                                break;

                            default:
                                char tch = (char)buf[0];
                                //slog("Client tag error: received unknown tag '" + tch.ToString() + "' 0x" + buf[0].ToString("X2"));
                                //stop = true;
                                //break;
                                throw new Exception("Client tag error: received unknown tag '" + tch.ToString() + "' 0x" + buf[0].ToString("X2"));
                        }
                    }
                }
            }
            /*catch (System.IO.IOException ioex) // Quit message ('\\') makes this pointless.
            {
            }*/

            dllclientStm.Close(2000); // Wait secs to finish sending pending data.
            dllclientStm.Dispose();
        }


        private static Random _myrrt = new Random(unchecked(
            System.Threading.Thread.CurrentThread.ManagedThreadId
            + DateTime.Now.Millisecond));
        public static int MyRealRetryTimeout(int timeout)
        {
            if (timeout <= 3)
            {
                return timeout;
            }
            lock (_myrrt)
            {
                return _myrrt.Next(timeout / 4, timeout + 1);
            }
        }


        #region MyThreadTools
        public class MyThreadTools<TaskItem, ThreadItem>
        {
            internal static void _Parallel(Action<TaskItem> action1, Action<TaskItem, ThreadItem> action2, IList<TaskItem> tasks, IList<ThreadItem> threads)
            {
                _Parallel(action1, action2, tasks, threads, false);
            }

            internal static void _Parallel(Action<TaskItem> action1, Action<TaskItem, ThreadItem> action2, IList<TaskItem> tasks, IList<ThreadItem> threads, bool trace)
            {
                int ntasks = tasks.Count;
                int nthreads = threads.Count;
                if (nthreads < 2 || ntasks < 2) // Will only be 1 thread here.
                {
                    if (trace)
                    {
                        DistributedObjectsService.DOService_AddTraceThread(null);       
                    }                                 
                    if (null != action2)
                    {
                        for (int i = 0; i < ntasks; i++)
                        {
                            action2(tasks[i], threads[0]);
                        }
                    }
                    else
                    {
                        for (int i = 0; i < ntasks; i++)
                        {
                            action1(tasks[i]);
                        }
                    }
                    if (trace)
                    {
                        DistributedObjectsService.DOService_RemoveTraceThread(null);
                    }                    
                }
                else // More than one thread!
                {
                    int tpt = ntasks / nthreads; // Tasks per thread.
                    if (0 != (ntasks % nthreads))
                    {
                        tpt++;
                    }
                    List<PTO> ptos = new List<PTO>(nthreads);
                    int offset = 0;
                    for (int it = 0; offset < ntasks; it++)
                    {
                        PTO pto = new PTO();
                        pto.thread = new System.Threading.Thread(new System.Threading.ThreadStart(pto.threadproc));                        
                        pto.alltasks = tasks;
                        pto.start = offset;
                        offset += tpt;
                        if (offset > ntasks)
                        {
                            offset = ntasks;
                        }
                        pto.stop = offset;
                        pto.action1 = action1;
                        pto.action2 = action2;
                        pto.threaditem = threads[it];
                        ptos.Add(pto);
                        pto.thread.Start();
                        if (trace)
                        {
                            DistributedObjectsService.DOService_AddTraceThread(pto.thread);
                        }
                    }
                    for (int i = 0; i < ptos.Count; i++)
                    {                        
                        ptos[i].thread.Join();

                        if (trace)
                        {
                            DistributedObjectsService.DOService_RemoveTraceThread(ptos[i].thread);
                        }
                        
                        if (ptos[i].exception != null)
                        {
                            throw ptos[i].exception;
                        }
                    }
                }
            }

            /*public static void Parallel(Action<TaskItem> action, IList<TaskItem> tasks, IList<ThreadItem> threads)
            {
                _Parallel(action, null, tasks, threads);
            }*/

            public static void Parallel(Action<TaskItem, ThreadItem> action, IList<TaskItem> tasks, IList<ThreadItem> threads)
            {
                _Parallel(null, action, tasks, threads);
            }


            class PTO
            {
                internal System.Threading.Thread thread;
                internal IList<TaskItem> alltasks;
                internal int start;
                internal int stop;
                internal Action<TaskItem> action1;
                internal Action<TaskItem, ThreadItem> action2;
                internal ThreadItem threaditem;
                internal Exception exception = null;

                internal void threadproc()
                {
                    try
                    {
                        if (null != action2)
                        {
                            for (int i = start; i < stop; i++)
                            {
                                action2(alltasks[i], threaditem);
                            }
                        }
                        else
                        {
                            for (int i = start; i < stop; i++)
                            {
                                action1(alltasks[i]);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        exception = e;
#if DEBUG
                        if (!System.Diagnostics.Debugger.IsAttached)
                        {
                            System.Diagnostics.Debugger.Launch();
                        }
#endif
                    }
                }

            }

        }


        public class MyThreadTools<TaskItem>
        {
            public static void Parallel(Action<TaskItem> action, IList<TaskItem> tasks, int nthreads)
            {
                MyThreadTools<TaskItem, int>._Parallel(action, null, tasks, new MyListCounter(nthreads));
            }

            public static void Parallel(Action<TaskItem> action, IList<TaskItem> tasks)
            {
                MyThreadTools<TaskItem, int>._Parallel(action, null, tasks, new MyListCounter(MyThreadTools.NumberOfProcessors));
            }

            public static void ParallelWithTrace(Action<TaskItem> action, IList<TaskItem> tasks)
            {
                MyThreadTools<TaskItem, int>._Parallel(action, null, tasks, new MyListCounter(MyThreadTools.NumberOfProcessors), true);
            }
        }


        public class MyThreadTools
        {

            public static void Parallel(Action<object> action, IList<object> tasks, int nthreads)
            {
                MyThreadTools<object>.Parallel(action, tasks, nthreads);
            }

            public static void Parallel(Action<object> action, IList<object> tasks)
            {
                MyThreadTools<object>.Parallel(action, tasks);
            }


            public static void Parallel(Action<int> action, int count, int nthreads)
            {
                MyThreadTools<int>.Parallel(action, new MyListCounter(count), nthreads);
            }

            public static void Parallel(Action<int> action, int count)
            {
                MyThreadTools<int>.Parallel(action, new MyListCounter(count));
            }


            public static int NumberOfProcessors
            {
                get
                {
                    if (_ncpus < 1)
                    {
                        try
                        {
                            _ncpus = int.Parse(Environment.GetEnvironmentVariable("NUMBER_OF_PROCESSORS"));
                        }
                        catch
                        {
                            _ncpus = 1;
                        }
                    }
                    return _ncpus;
                }
            }
            static int _ncpus = 0;

        }


        #region ListCounter
        class MyListCounter : IList<int>
        {
            int _count;
            public MyListCounter(int count)
            {
                this._count = count;
            }


            public int IndexOf(int item)
            {
                if (!Contains(item))
                {
                    return -1;
                }
                return item;
            }

            public void Insert(int index, int item)
            {
                throw new NotSupportedException();
            }

            public void RemoveAt(int index)
            {
                throw new NotSupportedException();
            }

            public int this[int index]
            {
                get
                {
#if DEBUG
                    if (index < 0 || index >= _count)
                    {
                        throw new ArgumentException("ListCounter: Out of bounds");
                    }
#endif
                    return index;
                }
                set
                {
                    throw new NotSupportedException();
                }
            }

            public void Add(int item)
            {
                throw new NotSupportedException();
            }

            public void Clear()
            {
                throw new NotSupportedException();
            }

            public bool Contains(int item)
            {
                return item >= 0 && item < Count;
            }

            public void CopyTo(int[] array, int arrayIndex)
            {
                for (int i = 0; i < _count; i++)
                {
                    array[arrayIndex + i] = i;
                }
            }

            public int Count
            {
                get { return _count; }
            }

            public bool IsReadOnly
            {
                get { return true; }
            }

            public bool Remove(int item)
            {
                throw new NotSupportedException();
            }

            public IEnumerator<int> GetEnumerator()
            {
                throw new NotImplementedException();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                throw new NotImplementedException();
            }

        }
        #endregion

        #endregion


        // Big-endian
        public static void MyToBytes(Int32 x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 24);
            resultbuf[bufoffset + 1] = (byte)(x >> 16);
            resultbuf[bufoffset + 2] = (byte)(x >> 8);
            resultbuf[bufoffset + 3] = (byte)x;
        }


        // Big-endian.
        public static Int32 MyBytesToInt(IList<byte> x, int offset)
        {
            int result = 0;
            result |= (int)x[offset + 0] << 24;
            result |= (int)x[offset + 1] << 16;
            result |= (int)x[offset + 2] << 8;
            result |= x[offset + 3];
            return result;
        }

        public static Int32 MyBytesToInt(IList<byte> x)
        {
            return MyBytesToInt(x, 0);
        }


        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        static extern int GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer); // Note: returns BOOL.

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
        public class MEMORYSTATUSEX
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;
            public MEMORYSTATUSEX()
            {
                this.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
            }
        }


        public static MEMORYSTATUSEX GetMemoryStatus()
        {
            MEMORYSTATUSEX result = new MEMORYSTATUSEX();
            if (0 == GlobalMemoryStatusEx(result))
            {
                throw new Exception("Unable to get global memory status");
            }
            return result;
        }


    }
}
