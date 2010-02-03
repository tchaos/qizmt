using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public class DFSUtils
    {
        public static bool MakeFileBackup(string fpath, ref string fback)
        {
            fback = fpath + "_back_" + Guid.NewGuid().ToString();
            try
            {
                System.IO.File.Copy(fpath, fback);
                return true;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error backing up {0}: {1}", fpath, e.Message);
                return false;
            }
        }

        public static bool UndoFileChanges(string fpath, string fback)
        {
            try
            {
                System.IO.File.Copy(fback, fpath, true);
                return true;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error while undoing changes: {0}.  Location of original file to restore: {1}", e.Message, fback);
                return false;
            }            
        }

        public static void ChangeDFSXMLSlaveList(System.Xml.XmlDocument dfs, string saveAs, string newSlavelist)
        {
            System.Xml.XmlNode node = dfs.SelectSingleNode("//SlaveList");
            node.InnerText = newSlavelist;
            dfs.Save(saveAs);
        }

        public static void ChangeDFSXMLSlaveList(string dfspath, string newSlavelist)
        {
            System.Xml.XmlDocument doc = new System.Xml.XmlDocument();
            doc.Load(dfspath);
            System.Xml.XmlNode node = doc.SelectSingleNode("//SlaveList");
            node.InnerText = newSlavelist;
            doc.Save(dfspath);
        }
    }
}
