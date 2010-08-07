using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;

namespace QizmtEC2Service
{
    public partial class Service1 : ServiceBase
    {
        static System.Threading.Thread lthd;
        static System.Net.Sockets.Socket lsock;
        static int userhit = 0;


        void BeginService()
        {
            try
            {
                string service_base_dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
                System.Environment.CurrentDirectory = service_base_dir;

                XLog.statuslog("Started");

                {
                    try
                    {
                        XLog.statuslog("Starting QizmtEC2ServiceInit.exe");
                        System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics
                            .ProcessStartInfo("QizmtEC2ServiceInit.exe");
                        psi.UseShellExecute = false;
                        psi.CreateNoWindow = true;
                        System.Diagnostics.Process initproc = System.Diagnostics.Process.Start(psi);
#if RESETEC2SERVICEPASSWORD
                        initproc.Dispose();
                        this.Stop(); // Note! QizmtEC2ServiceInit will restart the service with new creds.
                        XLog.statuslog("Stopping this service");
                        return;
#else
                        initproc.WaitForExit();
                        initproc.Dispose();
                        XLog.statuslog("Waited on QizmtEC2ServiceInit.exe");
#endif
                    }
                    catch (System.Threading.ThreadAbortException)
                    {
                        return;
                    }
                    catch (System.Threading.ThreadInterruptedException)
                    {
                        return;
                    }
                    catch (Exception e)
                    {
                        XLog.errorlog("OnStart exception while running QizmtEC2ServiceInit: " + e.ToString());
                        return;
                    }
                }

                {
                    lthd = new System.Threading.Thread(new System.Threading.ThreadStart(ListenThreadProc));
                    lthd.IsBackground = true;
                    lthd.Start();
                }
            }
            catch (Exception e)
            {
                XLog.errorlog("OnStart exception: " + e.ToString());
            }
        }


        public Service1()
        {
            InitializeComponent();
        }


        protected override void OnStart(string[] args)
        {
            System.Threading.Thread beginthd = new System.Threading.Thread(
                new System.Threading.ThreadStart(
                    delegate()
                    {
                        BeginService();
                    }));
            beginthd.IsBackground = true;
            beginthd.Start();
        }


        protected override void OnStop()
        {
        }


        static void ListenThreadProc()
        {
            try
            {
                lsock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                    System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                System.Net.IPEndPoint ipep = new System.Net.IPEndPoint(System.Net.IPAddress.Any, 55907);
                for (int i = 0; ; i++)
                {
                    try
                    {
                        lsock.Bind(ipep);
                        break;
                    }
                    catch (Exception e)
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

                XLog.statuslog("Accepting connections on port " + ipep.Port);

                for (; ; )
                {
                    System.Net.Sockets.Socket dllclientSock = lsock.Accept();

                    System.Threading.Thread cthd = new System.Threading.Thread(
                        new System.Threading.ParameterizedThreadStart(ClientThreadProc));
                    cthd.Name = "ClientThread" + (++userhit).ToString();
                    cthd.IsBackground = true;
                    cthd.Start(dllclientSock);
                }
            }
            catch (System.Threading.ThreadAbortException e)
            {
            }
            catch (Exception e)
            {
                XLog.errorlog("ListenThreadProc exception: " + e.ToString());
            }
        }


        static void ClientThreadProc(object obj)
        {
            System.Net.Sockets.Socket dllclientSock = null;
            byte[] buf = new byte[0x400 * 4];
            try
            {
                dllclientSock = (System.Net.Sockets.Socket)obj;
                System.Net.Sockets.NetworkStream netstm = new System.Net.Sockets.NetworkStream(dllclientSock);
                for (bool run = true; run; )
                {
                    int ich = -1;
                    try
                    {
                        ich = netstm.ReadByte();
                    }
                    catch
                    {
                    }
                    if (ich == -1)
                    {
                        break;
                    }
                    switch (ich)
                    {
                        case 'r': // Run!
                            {
                                Exception outputexception = null;
                                string output = "";
                                try
                                {
                                    string cmd = XContent.ReceiveXString(netstm, buf);
#if QEC2S_RUNTHREAD
                                    System.Threading.Thread thd = new System.Threading.Thread(
                                        new System.Threading.ThreadStart(
                                            delegate()
                                            {
#endif
                                                try
                                                {
                                                    output = Exec.Shell(cmd, false);
                                                }
                                                catch (Exception einner)
                                                {
                                                    outputexception = einner;
                                                }
#if QEC2S_RUNTHREAD
                                            }));
                                    thd.IsBackground = true;
                                    thd.Start();
                                    thd.Join();
#endif
                                    if (null == outputexception)
                                    {
                                        netstm.WriteByte((byte)'+');
                                        XContent.SendXContent(netstm, output);
                                    }
                                }
                                catch (Exception eouter)
                                {
                                    if (null == outputexception)
                                    {
                                        outputexception = eouter;
                                    }
                                }
                                if (null != outputexception)
                                {
                                    try
                                    {
                                        netstm.WriteByte((byte)'-');
                                        XContent.SendXContent(netstm, outputexception.ToString());
                                    }
                                    catch (Exception e)
                                    {
                                        XLog.errorlog("Error while reporting error: " + outputexception.ToString());
                                    }
                                }
                            }
                            break;

                        case 'c': // Close.
                            run = false;
                            break;
                    }
                }
                netstm.Close();
                dllclientSock.Close();
            }
            catch (Exception e)
            {
                XLog.errorlog("ClientThreadProc exception: " + e.ToString());

                try
                {
                    dllclientSock.Close();
                }
                catch (Exception e2)
                {
                }
            }
        }


        public static class XLog
        {
            public static bool logging = false;

            internal static System.Threading.Mutex logmutex = new System.Threading.Mutex(false, "QizmtEC2ServiceLog");


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
                    using (System.IO.StreamWriter fstm = System.IO.File.AppendText("QizmtEC2Service-errors.txt"))
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
                        fstm.WriteLine("[{0} {1}ms] QizmtEC2Service service error: {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
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


            public static void statuslog(string line)
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
                    using (System.IO.StreamWriter fstm = System.IO.File.AppendText("QizmtEC2Service-status.txt"))
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
                        fstm.WriteLine("[{0} {1}ms] QizmtEC2Service service status: {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
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
                    System.IO.StreamWriter fstm = System.IO.File.AppendText(name + ".txt");
                    fstm.WriteLine("{0}", line);
                    fstm.Close();
                }
                finally
                {
                    logmutex.ReleaseMutex();
                }
            }
        }


    }

}
