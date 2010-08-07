using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Runtime.InteropServices;

namespace QizmtEC2ServiceInit
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {

                XLog.statuslog("Started");

                try
                {
                    // C:\Program Files\Amazon\Ec2ConfigService\Logs\Ec2ConfigLog.txt
                    // contains "Ec2RebootInstance:Windows is Ready to use"
                    // Allow thread abort and interrupt exceptions here.
                    XLog.statuslog("Waiting on EC2 instance to fully to initialize");
                    for (; ; )
                    {
                        string content = System.IO.File.ReadAllText(
                            @"C:\Program Files\Amazon\Ec2ConfigService\Logs\Ec2ConfigLog.txt");
                        if (-1 != content.IndexOf("Ec2RebootInstance:Windows is Ready to use"))
                        {
                            break;
                        }
                        System.Threading.Thread.Sleep(1000 * 20);
                    }
                }
                catch (System.Threading.ThreadInterruptedException)
                {
                    return;
                }
                catch (System.Threading.ThreadAbortException)
                {
                    return;
                }

                XLog.statuslog("Processing user data");
                string userdata;
                for (int itries = 0; ; itries++)
                {
                    try
                    {
                        System.Net.WebClient wc = new System.Net.WebClient();
                        userdata = wc.DownloadString("http://169.254.169.254/2009-04-04/user-data");
                    }
                    catch
                    {
                        if (itries > 5)
                        {
                            throw;
                        }
                        continue;
                    }
                    break;
                }

                userdata = userdata.Trim();
                string[] options = get(userdata).Split('\t');
                if (options.Length < 1
                    || options[0].Length < 1
                    || options[0][0] != '@')
                {
                    throw new Exception("Not expected format of user data");
                }

                string mypassword = "";

                // Do the other options first.
                for (int i = 1; i < options.Length; i++)
                {
                    string opt = options[i];
                    if (opt.StartsWith("PASSWORD:"))
                    {
                        mypassword = opt.Substring(opt.IndexOf(':') + 1);
                    }
                }
#if DEBUG
                //Console.WriteLine(string.Join("\t", options));
#endif

                int iarg = 0;
                if (args.Length > iarg
                    && args[iarg].StartsWith("PASSWORD:"))
                {
                    mypassword = args[iarg].Substring(args[iarg].IndexOf(':') + 1);
                    iarg++;
                }

                if (string.IsNullOrEmpty(mypassword))
                {
                    throw new Exception("Expected new password");
                }

                if (!string.IsNullOrEmpty(mypassword))
                {
                    XLog.statuslog("Updating password");
                    // Set the password: http://support.microsoft.com/kb/149427
                    Exec.Shell("net user administrator  " + mypassword);
                }

#if RESETEC2SERVICEPASSWORD
                {
                    XLog.statuslog("Setting QizmtEC2Service permissions");

                    {
                        // Should already be stopped...
                        //System.Threading.Thread.Sleep(1000 * 2);
                        Exec.Shell(@"sc stop QizmtEC2Service");
                    }

                    {
                        System.Threading.Thread.Sleep(1000 * 2);
                        CallSC(@"sc config QizmtEC2Service obj= .\administrator password= " + mypassword);
                    }

                    {
                        System.Threading.Thread.Sleep(1000 * 4);
                        CallSC(@"sc start QizmtEC2Service");
                    }
                }
#endif

                {

                    XLog.statuslog("Setting Qizmt DistributedObjects permissions");

                    {
                        //System.Threading.Thread.Sleep(1000 * 2);
                        Exec.Shell(@"sc stop DistributedObjects");
                    }

                    {
                        System.Threading.Thread.Sleep(1000 * 2);
                        CallSC(@"sc config DistributedObjects obj= .\administrator password= " + mypassword);
                    }

                    {
                        System.Threading.Thread.Sleep(1000 * 4);
                        CallSC(@"sc start DistributedObjects");
                    }
                }

                System.Threading.Thread.Sleep(1000 * 2);

                if (options[0].StartsWith("@SURROGATEWORKER:"))
                {
                    // Participating surrogate.
                    XLog.statuslog("Configuring Qizmt surrogate (participating)");
                    DoSurrogate(options[0].Substring(options[0].IndexOf(':') + 1),
                        mypassword, true);
                }
                else if (options[0].StartsWith("@SURROGATENONWORKER:"))
                {
                    // Non-participating surrogate
                    XLog.statuslog("Configuring Qizmt surrogate (non-participating)");
                    DoSurrogate(options[0].Substring(options[0].IndexOf(':') + 1),
                        mypassword, false);
                }
                else
                {
                    XLog.statuslog("Configuring Qizmt worker");
                }

            }
            catch (Exception e)
            {
                XLog.errorlog(e.ToString());
            }

            XLog.statuslog("Done");

        }


        public static string GetHostnameInternal(string ipaddrInternal)
        {
            if (string.IsNullOrEmpty(ipaddrInternal))
            {
                throw new Exception("Ec2Instance.GetHostnameInternal: ipaddrInternal is null or empty");
            }
            System.Net.IPAddress addr = System.Net.IPAddress.Parse(ipaddrInternal);
            byte[] bytes = addr.GetAddressBytes();
            int laddr = BitConverter.ToInt32(bytes, 0);
            laddr = System.Net.IPAddress.NetworkToHostOrder(laddr);
            return "ip-" + laddr.ToString("X8");
        }


        static string CallSC(string args)
        {
            string cmd = args;
            if (!args.StartsWith("sc ", StringComparison.OrdinalIgnoreCase)
                && !args.StartsWith("sc.exe ", StringComparison.OrdinalIgnoreCase))
            {
                cmd = "sc.exe " + args;
            }
            string result = Exec.Shell(cmd);
            {
                string eresult = result.Trim();
                string line = eresult;
                int iline = eresult.IndexOf('\n');
                if (-1 != iline)
                {
                    line = eresult.Substring(0, iline).TrimEnd();
                }
                if (-1 != line.IndexOf(" FAILED"))
                {
                    string errmsg = eresult.Replace('\n', ' ').Replace("\r", "");
                    string safecmd = cmd;
                    {
                        const string pwfind = "password=";
                        int ipw = safecmd.IndexOf(pwfind);
                        if (-1 != ipw)
                        {
                            safecmd = safecmd.Substring(0, ipw + pwfind.Length) + "{masked}";
                        }
                    }
                    throw new Exception("Error calling `" + safecmd + "`: " + errmsg);
                }
            }
            return result;
        }


        static void DoSurrogate(string hostsinfo, string mypassword, bool participating)
        {
            string[] hinfos = hostsinfo.Split((char)1);
            string myipaddr = IPAddressUtil.GetIPv4Address(System.Net.Dns.GetHostName());
            string myhost = GetHostnameInternal(myipaddr);
            try
            {
                if (myipaddr != IPAddressUtil.GetIPv4Address(myhost))
                {
                    throw new Exception("IP addresses do not match");
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error with internal IP address and host name"
                    + " (" + myipaddr + " != " + myhost + ")", e);
            }
            // File in the format:
            // <WindowsUser>
            // <machine>=<password>
            string logondatContent = "administrator" + Environment.NewLine;
            logondatContent += myhost + "=" + mypassword + Environment.NewLine;

            string formatmachines = "";
            if (participating)
            {
                formatmachines = myhost;
            }

            string appendhostsfiles = Environment.NewLine
                + myipaddr + "    " + myhost + Environment.NewLine;

            foreach (string hinfo in hinfos)
            {
                int ieq = hinfo.IndexOf('=');
                if (-1 != ieq)
                {
                    // hinfo already in form <machine>=<password>
                    //logondatContent += hinfo + Environment.NewLine;
                    // I want the hostname, not the IPaddr.

                    string rhost = hinfo.Substring(0, ieq); // Probably an internal IP address.
                    string rpasswd = hinfo.Substring(ieq + 1);
                    {
                        if (formatmachines.Length > 0)
                        {
                            formatmachines += ";";
                        }
                        string fmhost = rhost;
                        try
                        {
                            fmhost = GetHostnameInternal(rhost);
                            appendhostsfiles += rhost + "    " + fmhost + Environment.NewLine;
                        }
                        catch
                        {
                        }
                        formatmachines += fmhost;
                        logondatContent += fmhost + "=" + rpasswd + Environment.NewLine;
                    }
                }
            }

            // Append my appendhostsfiles to hosts file.
            string hostsfilepath = Environment.ExpandEnvironmentVariables(
                @"%SystemRoot%\system32\drivers\etc\hosts");
            System.IO.File.Copy(hostsfilepath, "hosts.old", true); // Backup old one.
            System.IO.File.AppendAllText(ToNetworkPath(hostsfilepath, myipaddr), appendhostsfiles);
            System.Threading.Thread.Sleep(1000);

            // Write my logon.dat; and logon.
            string thisnetpath = NetworkPathForHost(myipaddr);
            if (!System.IO.Directory.Exists(thisnetpath))
            {
                System.IO.Directory.CreateDirectory(thisnetpath);
            }
            string thislogonfile = thisnetpath + @"\logon.dat";
            System.IO.File.WriteAllText(thislogonfile, logondatContent);
            XLog.statuslog("Logging on all machines in cluster");
            if (!LogonMachines(thislogonfile))
            {
                throw new Exception("Was not able to LogonMachines");
            }
            XLog.statuslog("Logged on all machines in cluster");

            XLog.statuslog("Configuring machines in cluster");
            // Write logon.dat to other machines.
            // Append appendhostsfiles to other machines' hosts file. 
            foreach (string hinfo in hinfos)
            {
                int ieq = hinfo.IndexOf('=');
                if (-1 != ieq)
                {
                    string rhost = hinfo.Substring(0, ieq); // Probably an internal IP address.

                    string fmhost = rhost;
                    try
                    {
                        fmhost = GetHostnameInternal(rhost);
                        appendhostsfiles += rhost + "    " + fmhost + Environment.NewLine;
                    }
                    catch
                    {
                    }

                    string rnetpath = NetworkPathForHost(fmhost);
                    if (!System.IO.Directory.Exists(rnetpath))
                    {
                        System.IO.Directory.CreateDirectory(rnetpath);
                    }
                    string rlogonfile = rnetpath + @"\logon.dat";
                    System.IO.File.WriteAllText(rlogonfile, logondatContent);

                    System.IO.File.AppendAllText(ToNetworkPath(hostsfilepath, fmhost), appendhostsfiles);
                }
            }
            XLog.statuslog("Configured machines in cluster");

            {
                string fmtcmd = "Qizmt format Machines=" + formatmachines;
                XLog.statuslog("Formatting Qizmt cluster: " + fmtcmd);
                Exec.Shell(fmtcmd);
            }

        }


        public static bool LogonMachines(string logonfile)
        {
            // File in the format:
            // <WindowsUser>
            // <machine>=<password>
            try
            {
                using (System.IO.StreamReader sr = new System.IO.StreamReader(logonfile))
                {
                    string user = sr.ReadLine();
                    if (string.IsNullOrEmpty(user))
                    {
                        return false;
                    }
                    for (; ; )
                    {
                        string s = sr.ReadLine();
                        if (null == s)
                        {
                            break;
                        }
                        string machine = null;
                        string passwd = s;
                        int ieq = s.IndexOf('=');
                        if (ieq > 0)
                        {
                            machine = s.Substring(0, ieq);
                            passwd = s.Substring(ieq + 1);
                        }
                        else
                        {
                            //continue;
                            throw new InvalidOperationException("LogonMachines error in file (=)");
                        }
                        string netpath = NetworkPathForHost(machine);
                        int idollar = netpath.IndexOf('$');
                        if (-1 == idollar)
                        {
                            //continue;
                            throw new InvalidOperationException("LogonMachines error in path ($)");
                        }
                        string sharepath = netpath.Substring(0, idollar + 1);
                        try
                        {
                            System.IO.DirectoryInfo di = new System.IO.DirectoryInfo(sharepath);
                            // Note: ensuring di.Attributes call passes or fails.
                            if ((di.Attributes & System.IO.FileAttributes.Offline)
                                == System.IO.FileAttributes.Offline)
                            {
                                throw new System.IO.IOException("Network share is offline: " + sharepath);
                            }
                            // Has access...
                            continue;
                        }
                        catch
                        {
                        }
                        //AccessNetworkShare(sharepath, user, passwd);
                        {
                            string cmdname = "net";
                            string cmdargs = "use * \"" + sharepath + "\" \"/USER:" + user + "\" \"" + passwd + "\"";
                            System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo(cmdname, cmdargs);
                            psi.UseShellExecute = false;
                            psi.CreateNoWindow = true;
                            psi.RedirectStandardError = true;
                            System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
                            //proc.WaitForExit();
                            string erroutput = proc.StandardError.ReadToEnd().Trim();
                            proc.Dispose();
                            if (erroutput.Length > 0)
                            {
                                string safeargs = cmdargs;
                                {
                                    const string findslashuser = "/USER:";
                                    int islashuser = cmdargs.IndexOf(findslashuser);
                                    if (-1 != islashuser)
                                    {
                                        safeargs = cmdargs.Substring(0, islashuser + findslashuser.Length) + "{masked}";
                                    }
                                }
                                throw new Exception("Process.Start(\"" + cmdname + "\", \"" + safeargs + "\") error: " + erroutput);
                            }
                        }
                    }
                    return true;
                }
            }
            catch (System.IO.FileNotFoundException)
            {
                return false;
            }
        }


        public static string NetworkPathForHost(string host)
        {
            // NOTE: Hardcoded path!
            return ToNetworkPath(@"C:\Qizmt", host);
        }


        public static string ToNetworkPath(string path, string host)
        {
            if (path.Length < 3
                   || ':' != path[1]
                   || '\\' != path[2]
                   || !char.IsLetter(path[0])
                   )
            {
                if (path.StartsWith(@"\\"))
                {
                    int ix = path.IndexOf('\\', 2);
                    if (-1 != ix)
                    {
                        return @"\\" + host + path.Substring(ix);
                    }
                }
                throw new Exception("ToNetworkPath invalid path: " + path);
            }
            return @"\\" + host + @"\" + path.Substring(0, 1) + @"$" + path.Substring(2);
        }


        static string get(string s)
        {
            byte[] inbuf = Convert.FromBase64String(s);
            byte[] buf = new byte[inbuf.Length];
            System.Security.Cryptography.MD5CryptoServiceProvider cp =
                new System.Security.Cryptography.MD5CryptoServiceProvider();
            byte[] kv = cp.ComputeHash(
                Encoding.ASCII.GetBytes("StringBuilder sb = new StringBuilder();"
                + inbuf.Length).Reverse().ToArray());
            int ik = inbuf.Length % kv.Length;
            for (int i = 0; i < inbuf.Length; i++)
            {
                buf[i] = (byte)((inbuf[i] ^ kv[ik]) - (byte)i);
                if (++ik == kv.Length)
                {
                    ik = 0;
                }
            }
            return Encoding.UTF8.GetString(buf);
        }


        public class IPAddressUtil
        {

            public static string GetIPv4Address(string HostnameOrIP)
            {
                System.Net.IPAddress[] addresslist = System.Net.Dns.GetHostAddresses(HostnameOrIP);
                for (int i = 0; i < addresslist.Length; i++)
                {
                    if (System.Net.Sockets.AddressFamily.InterNetwork == addresslist[i].AddressFamily)
                    {
                        return addresslist[i].ToString();
                    }
                }
                throw new Exception("IPAddressUtil.GetAddress: No IPv4 address found for " + HostnameOrIP);
            }

            public static string GetNameNoCache(string ipaddr)
            {
                try
                {
                    System.Net.IPHostEntry iphe = System.Net.Dns.GetHostEntry(ipaddr);
                    if (null == iphe || null == iphe.HostName)
                    {
                        return ipaddr;
                    }
                    return iphe.HostName;
                }
                catch (Exception e)
                {
#if CLIENT_LOG_ALL
                AELight.LogOutputToFile("CLIENT_LOG_ALL: IPAddressUtil.GetNameNoCache: unable to lookup host for IP address '" + ipaddr + "': " + e.ToString());
#endif
                    return ipaddr;
                }
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
                    fstm.WriteLine("[{0} {1}ms] QizmtEC2ServiceInit error: {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
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
                    fstm.WriteLine("[{0} {1}ms] QizmtEC2ServiceInit status: {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
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
