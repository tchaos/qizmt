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

//#define FAILOVER_DEBUG

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects5
{
    public class RogueHosts
    {
        static Dictionary<long, RogueHostsInfo> jobIDToRogueHosts = new Dictionary<long, RogueHostsInfo>(1000);

        internal static void Add(long jobid, string badhost)
        {
            string _badhost = badhost.ToLower();
            lock (jobIDToRogueHosts)
            {

#if FAILOVER_DEBUG
                {
                    string debugtxt = "";
                    foreach (KeyValuePair<long, RogueHostsInfo> pair in jobIDToRogueHosts)
                    {
                        debugtxt += Environment.NewLine + pair.Key.ToString() + "*****:";
                        foreach (string bh in pair.Value.RogueHosts.Keys)
                        {
                            debugtxt += ";" + bh;
                        }
                    }
                    Log("before AddBadHosts for:" + jobid.ToString() + ";badhost=" + badhost + Environment.NewLine +
                        ":jobIDToBadHosts:" + debugtxt);
                }
#endif
                RogueHostsInfo info;
                if (!jobIDToRogueHosts.ContainsKey(jobid))
                {
                    info.RogueHosts = new Dictionary<string, long>(10);
                    info.LastUpdated = 0;
                    jobIDToRogueHosts.Add(jobid, info);
                }
                else
                {
                    info = jobIDToRogueHosts[jobid];
                }
                if (!info.RogueHosts.ContainsKey(_badhost))
                {
                    long now = DateTime.Now.Ticks;
                    info.RogueHosts.Add(_badhost, now);
                    info.LastUpdated = now;
                    jobIDToRogueHosts[jobid] = info;
                }

#if FAILOVER_DEBUG
                {
                    string debugtxt = "";
                    foreach (KeyValuePair<long, RogueHostsInfo> pair in jobIDToRogueHosts)
                    {
                        debugtxt += Environment.NewLine + pair.Key.ToString() + "*****:";
                        foreach (string bh in pair.Value.RogueHosts.Keys)
                        {
                            debugtxt += ";" + bh;
                        }
                    }
                    Log("After AddBadHosts for:" + jobid.ToString() + ";badhost=" + badhost + Environment.NewLine +
                        ":jobIDToBadHosts:" + debugtxt);
                }
#endif
            }
        }

        internal static long Get(long jobid, long lastupdated, List<string> badhosts)
        {
            lock (jobIDToRogueHosts)
            {
#if FAILOVER_DEBUG
                {
                    string debugtxt = "";
                    foreach (KeyValuePair<long, RogueHostsInfo> pair in jobIDToRogueHosts)
                    {
                        debugtxt += Environment.NewLine + pair.Key.ToString() + "*****:";
                        foreach (string bh in pair.Value.RogueHosts.Keys)
                        {
                            debugtxt += ";" + bh;
                        }
                    }
                    Log("GetBadHosts for:" + jobid.ToString() + Environment.NewLine +
                        ":jobIDToBadHosts:" + debugtxt);
                }
#endif

                if (jobIDToRogueHosts.ContainsKey(jobid))
                {
                    RogueHostsInfo info = jobIDToRogueHosts[jobid];
                    if (info.LastUpdated > lastupdated)
                    {
                        badhosts.Clear();
                        foreach (KeyValuePair<string, long> pair in info.RogueHosts)
                        {
                            if (pair.Value > lastupdated)
                            {
                                badhosts.Add(pair.Key);
                            }
                        }
                        return info.LastUpdated;
                    }
                }
            }
            return lastupdated;
        }

        internal static void Clear(long jobid)
        {
            lock (jobIDToRogueHosts)
            {
#if FAILOVER_DEBUG
                {
                    string debugtxt = "";
                    foreach (KeyValuePair<long, RogueHostsInfo> pair in jobIDToRogueHosts)
                    {
                        debugtxt += Environment.NewLine + pair.Key.ToString() + "*****:";
                        foreach (string bh in pair.Value.RogueHosts.Keys)
                        {
                            debugtxt += ";" + bh;
                        }
                    }
                    Log("before ClearBadHosts for:" + jobid.ToString() + ":" + debugtxt);
                }

#endif
                jobIDToRogueHosts.Remove(jobid);

#if FAILOVER_DEBUG
                {
                    string debugtxt = "";
                    foreach (KeyValuePair<long, RogueHostsInfo> pair in jobIDToRogueHosts)
                    {
                        debugtxt += Environment.NewLine + pair.Key.ToString() + "*****:";
                        foreach (string bh in pair.Value.RogueHosts.Keys)
                        {
                            debugtxt += ";" + bh;
                        }
                    }
                    Log("after ClearBadHosts for:" + jobid.ToString() + ":" + debugtxt);
                }
#endif

            }
        }

        internal static void Log(string msg)
        {
            string tempfile = @"c:\temp\roguehostslog_102C5560-2012-4537-AFC8-C40ADF0AD2B8.txt";
            lock (typeof(RogueHosts))
            {
                using (System.IO.StreamWriter w = new System.IO.StreamWriter(tempfile, true))
                {
                    w.WriteLine("  [" + DateTime.Now.ToString() + "]");
                    w.WriteLine(msg);
                }
            }
        }

        struct RogueHostsInfo
        {
            public Dictionary<string, long> RogueHosts;
            public long LastUpdated;
        }
    }
}
