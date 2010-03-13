using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySpace.DataMining.AELight;

namespace MySpace.DataMining.DistributedObjects
{
    public class DiskCheck
    {
        List<KeyValuePair<string, Surrogate.HealthMethod>> healthplugins = null;

        public DiskCheck(string[] pluginpaths)
        {
            healthplugins = GetHealthPlugins(pluginpaths);
        }

        static List<KeyValuePair<string, Surrogate.HealthMethod>> GetHealthPlugins(string[] pluginpaths)
        {
            List<KeyValuePair<string, Surrogate.HealthMethod>> plugins = new List<KeyValuePair<string, Surrogate.HealthMethod>>();
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
                                    int del = path.LastIndexOf(@"\");
                                    string pluginname = path.Substring(del + 1);
                                    plugins.Add(new KeyValuePair<string, Surrogate.HealthMethod>(pluginname + " " + t.Name, hm));
                                    foundhealthmethod = true;
                                }
                            }
                        }
                        if (!foundhealthmethod)
                        {
                            throw new Exception("Did not find a Health public class with CheckHealth public static method (HealthMethod)");
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

        public bool IsDiskFailure(string host, out string reason)
        {
            reason = null;
            foreach (KeyValuePair<string, Surrogate.HealthMethod> plugin in healthplugins)
            {
                Surrogate.HealthMethod hm = plugin.Value;
                if (Surrogate.SafeCallHealthMethod(hm, host, out reason))
                {
                    reason = null;
                }
                else
                {
                    return true;
                }
            }
            return false;
        }                        
    }
}