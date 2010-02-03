using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_EditConfig
{
    public class Program
    {  
        private static string thispath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        private static DataProvider[] dataproviders;

        public static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                throw new Exception("No action specified.");
            }

            dataproviders = new DataProvider[2];
            dataproviders[0] = DataProvider.Prepare("DSpace_DataProvider", "DSpace Data Provider",
                "Data Provider for DSpace", 
                "QueryAnalyzer_DataProvider.QaClientFactory, QueryAnalyzer_DataProvider, Version=1.0.0.0, Culture=neutral, PublicKeyToken=7074aee6b0b130c1");
            dataproviders[1] = DataProvider.Prepare("Qizmt_DataProvider", "Qizmt Data Provider",
                "Data Provider for Qizmt",
                "QueryAnalyzer_DataProvider.QaClientFactory, QueryAnalyzer_DataProvider, Version=1.0.0.0, Culture=neutral, PublicKeyToken=7074aee6b0b130c1");
            
            switch (args[0].ToLower())
            {
                case "a":
                    Set();
                    break;
                case "r":
                    Remove();
                    break;
                default:
                    throw new Exception("Invalid action.");
            }
        }

        private static void Set()
        {           
            try
            {
                System.EnterpriseServices.Internal.Publish pub = new System.EnterpriseServices.Internal.Publish();
                pub.GacInstall(thispath + @"\QueryAnalyzer_DataProvider.dll");
                Console.WriteLine("Added QueryAnalyzer_DataProvider to gac.");

                {
                    Configuration c = ConfigurationManager.OpenMachineConfiguration();
                    string cpath = c.FilePath;
                    SetNode(cpath);

                    string cpath32 = GetConfigPath32(cpath);
                    if (cpath32 != null)
                    {
                        SetNode(cpath32);
                    }
                }            
            }
            catch (Exception e)
            {
                Console.WriteLine("Error occurred while installing QueryAnalyzer_DataProvider.dll: {0}", e.ToString());
            }           
        }

        private static void SetNode(string cpath)
        {   
            System.Xml.XmlDocument doc = new System.Xml.XmlDocument();
            doc.Load(cpath);
            Console.WriteLine("Adding to machine.config path: {0}", cpath);

            foreach (DataProvider dp in dataproviders)
            {
                System.Xml.XmlNode node = doc.SelectSingleNode("configuration/system.data/DbProviderFactories/add[@invariant=\"" + dp.invariant + "\"]");
                if (node == null)
                {
                    System.Xml.XmlNode root = doc.SelectSingleNode("configuration/system.data/DbProviderFactories");
                    node = doc.CreateElement("add");
                    System.Xml.XmlAttribute at = doc.CreateAttribute("name");
                    node.Attributes.Append(at);
                    at = doc.CreateAttribute("invariant");
                    node.Attributes.Append(at);
                    at = doc.CreateAttribute("description");
                    node.Attributes.Append(at);
                    at = doc.CreateAttribute("type");
                    node.Attributes.Append(at);
                    root.AppendChild(node);
                }
                node.Attributes["name"].Value = dp.name;
                node.Attributes["invariant"].Value = dp.invariant;
                node.Attributes["description"].Value = dp.description;
                node.Attributes["type"].Value = dp.type;
                Console.WriteLine("Added Data Provider node in machine.config.");
            }            

            doc.Save(cpath);
            Console.WriteLine("machine.config saved: {0}", cpath);
        }

        private static void Remove()
        {
            try
            {
                System.EnterpriseServices.Internal.Publish pub = new System.EnterpriseServices.Internal.Publish();
                pub.GacRemove(thispath + @"\QueryAnalyzer_DataProvider.dll");
                Console.WriteLine("QueryAnalyzer_DataProvider.dll is removed from gac.");

                {
                    Configuration c = ConfigurationManager.OpenMachineConfiguration();
                    string cpath = c.FilePath;
                    RemoveNode(cpath);

                    string cpath32 = GetConfigPath32(cpath);
                    if (cpath32 != null)
                    {
                        RemoveNode(cpath32);
                    }
                }            
            }
            catch (Exception e)
            {
                Console.WriteLine("Error occurred while uninstalling QueryAnalyzer_DataProvider.dll: {0}", e.ToString());
            }            
        }

        private static string GetConfigPath32(string cpath)
        {
            if (cpath.IndexOf("framework64", StringComparison.OrdinalIgnoreCase) > -1)
            {
                string cpath32 = cpath.ToLower().Replace("framework64", "framework");
                if (System.IO.File.Exists(cpath32))
                {
                    return cpath32;
                }
            }
            return null;
        }

        private static void RemoveNode(string cpath)
        {
            System.Xml.XmlDocument doc = new System.Xml.XmlDocument();
            doc.Load(cpath);
            Console.WriteLine("Removing from machine.config path: {0}", cpath);

            foreach (DataProvider dp in dataproviders)
            {
                System.Xml.XmlNode node = doc.SelectSingleNode("configuration/system.data/DbProviderFactories/add[@invariant=\"" + dp.invariant + "\"]");
                if (node != null)
                {
                    node.ParentNode.RemoveChild(node);
                    Console.WriteLine("Data provider removed from machine.config.");
                }
            }

            doc.Save(cpath);
            Console.WriteLine("machine.config saved: {0}", cpath);
        }

        private struct DataProvider
        {
            public string invariant;
            public string name;
            public string description;
            public string type;

            public static DataProvider Prepare(string invariant, string name, string description, string type)
            {
                DataProvider dp;
                dp.invariant = invariant;
                dp.name = name;
                dp.description = description;
                dp.type = type;
                return dp;
            }
        }
    }
}
