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
using System.Threading;

namespace DProcess
{
    class DProcess
    {
        static void Main(string[] args)
        {           
            System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, 
                System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            System.Net.Sockets.NetworkStream netstm = null;
            try
            {
                sock.Connect(System.Net.Dns.GetHostName(), 55901);
                netstm = new System.Net.Sockets.NetworkStream(sock);

                string str = XContent.ReceiveXString(netstm, null);
                string app = str;
                string sargs = "";
                int i = str.IndexOf(' ');
                if (i > -1)
                {
                    app = str.Substring(0, i);
                    sargs = str.Substring(i + 1);
                }

                System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(app, sargs);
                psi.UseShellExecute = false;
                psi.CreateNoWindow = true;
                psi.StandardOutputEncoding = Encoding.UTF8;
                psi.RedirectStandardOutput = true;
                psi.RedirectStandardError = true;
                System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);

                netstm.WriteByte((byte)'+');

                string tname = System.Threading.Thread.CurrentThread.Name;

                System.Threading.Thread outthread = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(stdoutputthreadproc));
                outthread.Name = "stdoutputthread_from" + tname;
                outthread.IsBackground = false;
                outthread.Start(new object[] { netstm, proc.StandardOutput, 'o' });

                System.Threading.Thread errthread = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(stdoutputthreadproc));
                errthread.Name = "stderrorthread_from" + tname;
                errthread.IsBackground = true;
                errthread.Start(new object[] { netstm, proc.StandardError, 'e' });

                outthread.Join();
                errthread.Join();

                proc.WaitForExit();
                proc.Close();
            }
            catch (Exception e)
            {
                XLog.errorlog("DProcess error " + e.ToString());
            }
            finally
            {
                if(netstm != null)
                {
                    netstm.Close();
                    netstm = null;
                }
                sock.Close();
                sock = null;
            }
        }

        private static void stdoutputthreadproc(object obj)
        {
            byte bch = (byte)' ';
            try
            {
                object[] arr = (object[])obj;
                System.Net.Sockets.NetworkStream nstm = (System.Net.Sockets.NetworkStream)arr[0];
                System.IO.StreamReader reader = (System.IO.StreamReader)arr[1];
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
            catch (Exception e)
            {
               XLog.errorlog("stdoutputthreadproc '" + ((char)bch).ToString() + "' thread named " + System.Threading.Thread.CurrentThread.Name + " exception: " + e.ToString());
            }
        }
    }

    public static class XLog
    {
        private static Mutex logmutex = new Mutex(false, "dprocesslog");

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
                using (System.IO.StreamWriter fstm = System.IO.File.AppendText("dprocess-errors.txt"))
                {
                    fstm.WriteLine("[{0} {1}ms] DProcess error: {2}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, line);
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
    }
}