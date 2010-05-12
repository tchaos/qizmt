//#define DISKCHECK_DEBUG

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySpace.DataMining.AELight;

namespace MySpace.DataMining.DistributedObjects
{
    public class DiskCheck
    {
        private List<KeyValuePair<string, Surrogate.HealthPlugin>> healthplugins = null;

        public DiskCheck(string[] pluginpaths)
        {
            healthplugins = GetHealthPlugins(pluginpaths);
        }

        private static List<KeyValuePair<string, Surrogate.HealthPlugin>> GetHealthPlugins(string[] pluginpaths)
        {
            List<KeyValuePair<string, Surrogate.HealthPlugin>> plugins = new List<KeyValuePair<string, Surrogate.HealthPlugin>>();
            try
            {
                foreach (string path in pluginpaths)
                {
                    bool foundhealthmethod = false;
                    try
                    {
                        System.Reflection.Assembly hasm = System.Reflection.Assembly.LoadFrom(path);                       
                        foreach (Type t in hasm.GetTypes())
                        {
                            if (-1 != t.Name.IndexOf("Health", StringComparison.OrdinalIgnoreCase))
                            {
                                System.Reflection.MethodInfo mi = t.GetMethod("CheckHealth",
                                    System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                                if (null != mi)
                                {
                                    Surrogate.HealthMethod hm = (Surrogate.HealthMethod)Delegate.CreateDelegate(typeof(Surrogate.HealthMethod), mi);

                                    System.Reflection.FieldInfo fstartit = t.GetField("StartIteration",
                                    System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public);
                                    System.Reflection.FieldInfo fskip = t.GetField("ExecutionSkipFactor",
                                    System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public);

                                    if (fstartit != null && fskip != null)
                                    {
                                        uint startit = (uint)fstartit.GetValue(null);
                                        uint skip = (uint)fskip.GetValue(null);

                                        Surrogate.HealthPlugin plugin;
                                        plugin.Method = hm;
                                        plugin.StartInteration = startit;
                                        plugin.ExecutionSkipFactor = skip;

                                        int del = path.LastIndexOf(@"\");
                                        string pluginname = path.Substring(del + 1);
                                        plugins.Add(new KeyValuePair<string, Surrogate.HealthPlugin>(pluginname + " " + t.Name, plugin));
                                        foundhealthmethod = true;
                                    }
                                }
                            }
                        }
                        if (!foundhealthmethod)
                        {
                            throw new Exception("Did not find a Health public class with CheckHealth public static method (HealthMethod), public static uint StartIteration, public static uint ExecutionSkipFactor");
                        }
                    }
                    catch (Exception epl)
                    {
                        throw new Exception("Unable to use plugin " + path + ": " + epl.Message, epl);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Health plugin error: " + e.Message);
            }
            return plugins;
        }

        public bool IsDiskFailure(string host, Dictionary<string, int> roguehosts, out string reason)
        {
            return IsDiskFailure(true, 0, host, roguehosts, out reason); //dochecknow=true            
        }

        public bool IsDiskFailure(uint iteration, string host, Dictionary<string, int> roguehosts, out string reason)
        {
            return IsDiskFailure(false, iteration, host, roguehosts, out reason); //dochecknow=false             
        }

        private bool IsDiskFailure(bool dochecknow, uint iteration, string host, Dictionary<string, int> roguehosts, out string reason)
        {
            reason = null;
            foreach (KeyValuePair<string, Surrogate.HealthPlugin> pair in healthplugins)
            {
                Surrogate.HealthPlugin plugin = pair.Value;
                bool docheck = false;

                if (dochecknow)
                {
                    docheck = true;
                }
                else
                {
                    if (iteration > plugin.StartInteration)
                    {
                        docheck = (iteration - plugin.StartInteration) % plugin.ExecutionSkipFactor == 0;
                    }
                    else if (iteration == plugin.StartInteration)
                    {
                        docheck = true;
                    }
                }          

                if (docheck)
                {
                    Surrogate.HealthMethod hm = plugin.Method;
                    if (Surrogate.SafeCallHealthMethod(hm, host, roguehosts, out reason))
                    {
                        reason = null;
                    }
                    else
                    {
                        return true;
                    }
                }                
            }
            return false;
        }       
    }
}