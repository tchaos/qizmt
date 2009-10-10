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

        static void ServiceCommands(string[] args)
        {
            if (args.Length <= 1 || !System.IO.File.Exists(args[1]))
            {
                throw new Exception("Expected path to DFS.xml");
            }
            string dfsxmlpath = args[1];

            dfs dc = dfs.ReadDfsConfig_unlocked(dfsxmlpath);

            string masterhost = IPAddressUtil.GetName(Surrogate.MasterHost);
            string[] allmachines;
            {
                string[] sl = dc.Slaves.SlaveList.Split(';');
                List<string> aml = new List<string>(sl.Length + 1);
                aml.Add(masterhost);
                foreach (string slave in sl)
                {
                    if (0 != string.Compare(IPAddressUtil.GetName(slave), IPAddressUtil.GetName(masterhost), StringComparison.OrdinalIgnoreCase))
                    {
                        aml.Add(IPAddressUtil.GetName(slave));
                    }
                }
                allmachines = aml.ToArray();
            }

            string tempdir = @"\\" + System.Net.Dns.GetHostName() + @"\c$\temp";
            if (!System.IO.Directory.Exists(tempdir))
            {
                System.IO.Directory.CreateDirectory(tempdir);
            }

            string hoststxtpath = tempdir + @"\dspace-hosts-" + Guid.NewGuid().ToString() + ".txt";
            System.IO.File.WriteAllText(hoststxtpath, "# Test" + Environment.NewLine + string.Join(Environment.NewLine, allmachines));
            try
            {
                Console.WriteLine("Confirm initial cluster status...");
                {
                    Dictionary<string, string> status = _GetStatusAll("");
                    if (status.Count != allmachines.Length)
                    {
                        throw new Exception("Qizmt servicestatusall: (status.Count != allmachines.Length)");
                    }
                    try
                    {
                        _EnsureAllStatus(status, allmachines, "RUNNING");
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Qizmt servicestatusall: " + e.Message, e);
                    }
                }
                {
                    string arg = string.Join(",", allmachines);
                    Dictionary<string, string> status = _GetStatusAll(arg);
                    if (status.Count != allmachines.Length)
                    {
                        throw new Exception("Qizmt servicestatusall " + arg + ": (status.Count != allmachines.Length)");
                    }
                    try
                    {
                        _EnsureAllStatus(status, allmachines, "RUNNING");
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Qizmt servicestatusall " + arg + ": " + e.Message, e);
                    }
                }
                {
                    string arg = "\"@" + hoststxtpath + "\"";
                    Dictionary<string, string> status = _GetStatusAll(arg);
                    if (status.Count != allmachines.Length)
                    {
                        throw new Exception("Qizmt servicestatusall " + arg + ": (status.Count != allmachines.Length)");
                    }
                    try
                    {
                        _EnsureAllStatus(status, allmachines, "RUNNING");
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Qizmt servicestatusall " + arg + ": " + e.Message, e);
                    }
                }

                Console.WriteLine("Stopping surrogate...");
                Console.WriteLine(Exec.Shell("Qizmt stopsurrogate"));

                Console.WriteLine("Confirm stopped status of surrogate...");
                {
                    string host, statusmsg;
                    if (!_GetStatusFromLine(Exec.Shell("Qizmt servicestatussurrogate"), out host, out statusmsg)
                        || statusmsg.StartsWith("RUNNING"))
                    {
                        throw new Exception("Qizmt servicestatussurrogate");
                    }
                }
                {
                    Dictionary<string, string> status = _GetStatusAll("");
                    if (status.Count != allmachines.Length)
                    {
                        throw new Exception("(surrogate down check) Qizmt servicestatusall: (status.Count != allmachines.Length)");
                    }
                    try
                    {
                        _EnsureAllStatus(status, allmachines, "RUNNING");
                        throw new Exception("(surrogate down check) Qizmt servicestatusall: cluster reported healthy, but surrogate should be down");
                    }
                    catch
                    {
                        // Expected!
                    }
                }

                Console.WriteLine("Starting surrogate...");
                Console.WriteLine(Exec.Shell("Qizmt startsurrogate"));

                Console.WriteLine("Confirm started status of surrogate...");
                {
                    string host, statusmsg;
                    if (!_GetStatusFromLine(Exec.Shell("Qizmt servicestatussurrogate"), out host, out statusmsg)
                        || !statusmsg.StartsWith("RUNNING"))
                    {
                        throw new Exception("(restarted surrogate check) Qizmt servicestatussurrogate");
                    }
                }
                {
                    Dictionary<string, string> status = _GetStatusAll("");
                    if (status.Count != allmachines.Length)
                    {
                        throw new Exception("(restarted surrogate check) Qizmt servicestatusall: (status.Count != allmachines.Length)");
                    }
                    try
                    {
                        _EnsureAllStatus(status, allmachines, "RUNNING");
                    }
                    catch (Exception e)
                    {
                        throw new Exception("(restarted surrogate check) Qizmt servicestatusall: " + e.Message, e);
                    }
                }
                {
                    string arg = string.Join(",", allmachines);
                    Dictionary<string, string> status = _GetStatusAll(arg);
                    if (status.Count != allmachines.Length)
                    {
                        throw new Exception("(restarted surrogate check) Qizmt servicestatusall " + arg + ": (status.Count != allmachines.Length)");
                    }
                    try
                    {
                        _EnsureAllStatus(status, allmachines, "RUNNING");
                    }
                    catch (Exception e)
                    {
                        throw new Exception("(restarted surrogate check) Qizmt servicestatusall " + arg + ": " + e.Message, e);
                    }
                }
                {
                    string arg = "\"@" + hoststxtpath + "\"";
                    Dictionary<string, string> status = _GetStatusAll(arg);
                    if (status.Count != allmachines.Length)
                    {
                        throw new Exception("(restarted surrogate check) Qizmt servicestatusall " + arg + ": (status.Count != allmachines.Length)");
                    }
                    try
                    {
                        _EnsureAllStatus(status, allmachines, "RUNNING");
                    }
                    catch (Exception e)
                    {
                        throw new Exception("(restarted surrogate check) Qizmt servicestatusall " + arg + ": " + e.Message, e);
                    }
                }

                foreach (string xallargs in new string[] { "", string.Join(",", allmachines), "\"@" + hoststxtpath + "\"" })
                {
                    Console.WriteLine("Stopping all services in cluster, including surrogate...");
                    Console.WriteLine(Exec.Shell("Qizmt stopall " + xallargs));

                    Console.WriteLine("Confirming stopped status of all services...");
                    {
                        Dictionary<string, string> status = _GetStatusAll("");
                        if (status.Count != allmachines.Length)
                        {
                            throw new Exception("stopall " + xallargs + ": (stopped all services check) Qizmt servicestatusall: (status.Count != allmachines.Length)");
                        }
                        try
                        {
                            _EnsureAllStatus(status, allmachines, "STOPPED");
                        }
                        catch (Exception e)
                        {
                            throw new Exception("stopall " + xallargs + ": (stopped all services check) Qizmt servicestatusall: " + e.Message, e);
                        }
                    }
                    {
                        string arg = string.Join(",", allmachines);
                        Dictionary<string, string> status = _GetStatusAll(arg);
                        if (status.Count != allmachines.Length)
                        {
                            throw new Exception("stopall " + xallargs + ": (stopped all services check) Qizmt servicestatusall " + arg + ": (status.Count != allmachines.Length)");
                        }
                        try
                        {
                            _EnsureAllStatus(status, allmachines, "STOPPED");
                        }
                        catch (Exception e)
                        {
                            throw new Exception("stopall " + xallargs + ": (stopped all services check) Qizmt servicestatusall " + arg + ": " + e.Message, e);
                        }
                    }
                    {
                        string arg = "\"@" + hoststxtpath + "\"";
                        Dictionary<string, string> status = _GetStatusAll(arg);
                        if (status.Count != allmachines.Length)
                        {
                            throw new Exception("stopall " + xallargs + ": (stopped all services check) Qizmt servicestatusall " + arg + ": (status.Count != allmachines.Length)");
                        }
                        try
                        {
                            _EnsureAllStatus(status, allmachines, "STOPPED");
                        }
                        catch (Exception e)
                        {
                            throw new Exception("stopall " + xallargs + ": (stopped all services check) Qizmt servicestatusall " + arg + ": " + e.Message, e);
                        }
                    }

                    Console.WriteLine("Starting all services in cluster, including surrogate...");
                    Console.WriteLine(Exec.Shell("Qizmt startall " + xallargs));

                    Console.WriteLine("Confirm started status of all services...");
                    {
                        Dictionary<string, string> status = _GetStatusAll("");
                        if (status.Count != allmachines.Length)
                        {
                            throw new Exception("startall " + xallargs + ": (restarted all services check) Qizmt servicestatusall: (status.Count != allmachines.Length)");
                        }
                        try
                        {
                            _EnsureAllStatus(status, allmachines, "RUNNING");
                        }
                        catch (Exception e)
                        {
                            throw new Exception("startall " + xallargs + ": (restarted all services check) Qizmt servicestatusall: " + e.Message, e);
                        }
                    }
                    {
                        string arg = string.Join(",", allmachines);
                        Dictionary<string, string> status = _GetStatusAll(arg);
                        if (status.Count != allmachines.Length)
                        {
                            throw new Exception("startall " + xallargs + ": (restarted all services check) Qizmt servicestatusall " + arg + ": (status.Count != allmachines.Length)");
                        }
                        try
                        {
                            _EnsureAllStatus(status, allmachines, "RUNNING");
                        }
                        catch (Exception e)
                        {
                            throw new Exception("startall " + xallargs + ": (restarted all services check) Qizmt servicestatusall " + arg + ": " + e.Message, e);
                        }
                    }
                    {
                        string arg = "\"@" + hoststxtpath + "\"";
                        Dictionary<string, string> status = _GetStatusAll(arg);
                        if (status.Count != allmachines.Length)
                        {
                            throw new Exception("startall " + xallargs + ": (restarted all services check) Qizmt servicestatusall " + arg + ": (status.Count != allmachines.Length)");
                        }
                        try
                        {
                            _EnsureAllStatus(status, allmachines, "RUNNING");
                        }
                        catch (Exception e)
                        {
                            throw new Exception("startall " + xallargs + ": (restarted all services check) Qizmt servicestatusall " + arg + ": " + e.Message, e);
                        }
                    }
                }

            }
            finally
            {
                try
                {
                    System.IO.File.Delete(hoststxtpath);
                }
                catch
                {
                }
            }

            Console.WriteLine("[PASSED] - " + string.Join(" ", args));

        }


        static void _EnsureAllStatus(Dictionary<string, string> statusall, string[] allhosts, string needstatus)
        {
            foreach (string host in allhosts)
            {
                if (!statusall.ContainsKey(host))
                {
                    throw new Exception("Host '" + host + "' expected but not returned");
                }
                string statusmsg = statusall[host];
                if (!statusmsg.StartsWith(needstatus))
                {
                    throw new Exception("Host '" + host + "' does not have expected status of " + needstatus + " but has status of " + statusmsg);
                }
            }
        }


        static bool _GetStatusFromLine(string line, out string host, out string statusmsg)
        {
            line = line.Trim();
            int ix = line.IndexOf(':');
            if (-1 != ix)
            {
                string xhost = line.Substring(0, ix);
                host = IPAddressUtil.GetName(xhost);
                statusmsg = line.Substring(ix + 1).TrimStart();
                return true;
            }
            host = null;
            statusmsg = null;
            return false;
        }


        static Dictionary<string, string> _GetStatusAll(string arg)
        {
            Dictionary<string, string> status = new Dictionary<string, string>(new Surrogate.CaseInsensitiveEqualityComparer());
            string[] xlines = Exec.Shell("Qizmt servicestatusall " + arg).Split('\n');
            foreach (string xline in xlines)
            {
                string host, msg;
                if (_GetStatusFromLine(xline, out host, out msg))
                {
                    status[host] = msg;
                }
            }
            return status;
        }


    }
}
