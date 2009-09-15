using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_EditConfig
{
    public class Program
    {
        private static string invariant = "DSpace_DataProvider";
        private static string thispath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);

        public static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                throw new Exception("No action specified.");
            }
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
            string name = "QueryAnalyzer Data Provider";
            string description = "Data Provider for Query Analyzer";
            string type = "QueryAnalyzer_DataProvider.QaClientFactory, QueryAnalyzer_DataProvider, Version=1.0.0.0, Culture=neutral, PublicKeyToken=7074aee6b0b130c1";

            System.Xml.XmlDocument doc = new System.Xml.XmlDocument();
            doc.Load(cpath);
            System.Xml.XmlNode node = doc.SelectSingleNode("configuration/system.data/DbProviderFactories/add[@invariant=\"" + invariant + "\"]");

            Console.WriteLine("Adding to machine.config path: {0}", cpath);

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

            node.Attributes["name"].Value = name;
            node.Attributes["invariant"].Value = invariant;
            node.Attributes["description"].Value = description;
            node.Attributes["type"].Value = type;

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
            System.Xml.XmlNode node = doc.SelectSingleNode("configuration/system.data/DbProviderFactories/add[@invariant=\"" + invariant + "\"]");

            Console.WriteLine("Removing from machine.config path: {0}", cpath);

            if (node != null)
            {
                node.ParentNode.RemoveChild(node);
            }

            doc.Save(cpath);
            Console.WriteLine("machine.config saved: {0}", cpath);
        }
    }
}
