using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{

    public partial class Scheduler
    {

        public static void RunNotifierService()
        {
            for (bool wait = true; ; )
            {
                if (wait)
                {
                    System.Threading.Thread.Sleep(1000 * 30);
                }
                else
                {
                    System.Threading.Thread.Sleep(1000 * 3);
                }
                wait = true;

                {
                    if (!MySpace.DataMining.AELight.dfs.DfsConfigExists(
                        MySpace.DataMining.AELight.dfs.DFSXMLNAME))
                    {
                        // If not surrogate, wait a lot longer.
                        // Need to keep this loop in case this machine becomes surrogate.
                        System.Threading.Thread.Sleep(1000 * 30 * 9);
                        continue;
                    }
                }

                NotifyInfo ninfo = NotifyInfo.Load();

                {
                    for (int i = 0; i < ninfo.Notify.Count; i++)
                    {
                        NotifyInfo.NEntry ne = ninfo.Notify[i];
                        if (!System.IO.File.Exists(ne.WaitOnJID.ToString() + ".jid"))
                        {
                            ninfo.Notify.RemoveAt(i);
                            i--;
                            ninfo.Save();
                            try
                            {
                                string clustername = "N/A";
                                string SMTP;
                                {
                                    MySpace.DataMining.AELight.dfs dc =
                                        MySpace.DataMining.AELight.dfs.ReadDfsConfig_unlocked(
                                        MySpace.DataMining.AELight.dfs.DFSXMLNAME);
                                    if (dc.ClusterName != null)
                                    {
                                        clustername = dc.ClusterName;
                                    }
                                    SMTP = dc.SMTP;
                                }
                                if (null == SMTP)
                                {
                                    throw new Exception("SMTP server is null");
                                }

                                string subject = "Qizmt Notification";
                                string body;
                                {
                                    StringBuilder sbbody = new StringBuilder();
                                    sbbody.AppendFormat("{1} - Cluster Name{0}", Environment.NewLine, clustername);
                                    sbbody.AppendFormat("{1} - Job Identifier{0}", Environment.NewLine, ne.WaitOnJID);
                                    sbbody.AppendFormat("{1} - Completion Time{0}", Environment.NewLine, DateTime.Now);
                                    body = sbbody.ToString();
                                }
                                SendQNEmail(SMTP, ne.Email, subject, body);
                            }
                            catch (Exception e)
                            {
                                throw new Exception("Problem sending email to " + ne.Email + " after JID " + ne.WaitOnJID + " finished", e);
                            }
                            wait = false;
                            break;
                        }
                    }
                }

            }
        }


        public class NotifyInfo
        {
            public const string NOTIFYXMLNAME = "notify.xml";
            public const string MUTEXNAME = "Notifier{353B473D-3E4E-40c8-ABC3-C5FD82B54B8F}";


            [System.Xml.Serialization.XmlIgnore]
            public bool FileExists = false;


            public long LastNotifyID = 0;


            public class NEntry : ScheduleInfo.IEntry
            {
                public long ID = 0;
                public DateTime TimeAdded; // When this entry was added.
                public string UserAdded; // Who added this entry.
                public long WaitOnJID;
                public string Email;

                public long GetID() { return ID; }
                public DateTime GetTimeAdded() { return TimeAdded; }
                public string GetUserAdded() { return UserAdded; }

            }

            public List<NEntry> Notify;


            public int FindNEntryByID(long id)
            {
                return ScheduleInfo.FindID<NEntry>(id, Notify);
            }


            protected internal NEntry AddNotify_unlocked(NEntry ne)
            {
#if DEBUG
                if (0 != ne.ID)
                {
                    throw new Exception("DEBUG:  NEntry.AddNotify_unlocked: (0 != ne.ID) might already be added to notify list");
                }
#endif
                ne.ID = ++this.LastNotifyID;
                ne.TimeAdded = DateTime.Now;
                Notify.Add(ne);
                return ne;
            }


            protected internal static NotifyInfo Load_unlocked(string fp)
            {
                NotifyInfo result;
                for (; ; )
                {
                    try
                    {
                        System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(NotifyInfo));
                        using (System.IO.StreamReader sr = System.IO.File.OpenText(fp))
                        {
                            result = (NotifyInfo)xs.Deserialize(sr);
                        }
                        result.FileExists = true;
                    }
                    catch (System.IO.FileNotFoundException)
                    {
                        result = new NotifyInfo();
                    }
                    catch (System.IO.IOException)
                    {
                        System.Threading.Thread.Sleep(500);
                        continue;
                    }
                    break;
                }
                if (null == result.Notify)
                {
                    result.Notify = new List<NEntry>();
                }
                return result;
            }

            protected internal static NotifyInfo Load_unlocked()
            {
                return Load_unlocked(APPDIR + @"\" + NOTIFYXMLNAME);
            }

            public static NotifyInfo Load()
            {
                System.Threading.Mutex mu = new System.Threading.Mutex(false, MUTEXNAME);
                try
                {
                    mu.WaitOne();
                }
                catch (System.Threading.AbandonedMutexException)
                {
                }
                try
                {
                    return Load_unlocked();
                }
                finally
                {
                    mu.ReleaseMutex();
                    IDisposable dmu = mu;
                    dmu.Dispose();
                }
            }


            protected internal void Save_unlocked(string fp)
            {
                System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(NotifyInfo));
                string tempfp = fp + "$";
                try
                {
                    System.IO.File.Delete(tempfp);
                }
                catch
                {
                }
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(tempfp))
                {
                    xs.Serialize(sw, this);
                }
                try
                {
                    System.IO.File.Delete(fp);
                }
                catch
                {
                }
                System.IO.File.Move(tempfp, fp);

                this.FileExists = true;
            }

            protected internal void Save_unlocked()
            {
                Save_unlocked(APPDIR + @"\" + NOTIFYXMLNAME);
            }

            public void Save()
            {
                System.Threading.Mutex mu = new System.Threading.Mutex(false, MUTEXNAME);
                try
                {
                    mu.WaitOne();
                }
                catch (System.Threading.AbandonedMutexException)
                {
                }
                try
                {
                    Save_unlocked();
                }
                finally
                {
                    mu.ReleaseMutex();
                    IDisposable dmu = mu;
                    dmu.Dispose();
                }
            }

        }


        public static IList<NotifyInfo.NEntry> GetNotifySnapshot()
        {
            return NotifyInfo.Load().Notify;
        }


        public static NotifyInfo.NEntry AddNotify(long WaitOnJID, string email, string user)
        {
            NotifyInfo.NEntry ne = new NotifyInfo.NEntry();
            ne.WaitOnJID = WaitOnJID;
            ne.Email = email;
            ne.UserAdded = user;
            System.Threading.Mutex mu = new System.Threading.Mutex(false, NotifyInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                NotifyInfo ninfo = NotifyInfo.Load_unlocked();
                NotifyInfo.NEntry result = ninfo.AddNotify_unlocked(ne);
                ninfo.Save_unlocked();
                return result;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static bool NotifyKill(long NotifyID)
        {
            System.Threading.Mutex mu = new System.Threading.Mutex(false, NotifyInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                NotifyInfo ninfo = NotifyInfo.Load_unlocked();
                int index = ninfo.FindNEntryByID(NotifyID);
                if (-1 == index)
                {
                    return false;
                }
                ninfo.Notify.RemoveAt(index);
                ninfo.Save_unlocked();
                return true;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static void ClearNotify()
        {
            System.Threading.Mutex mu = new System.Threading.Mutex(false, NotifyInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                NotifyInfo ninfo = NotifyInfo.Load_unlocked();
                if (ninfo.Notify.Count < 1)
                {
                    return;
                }
                ninfo.Notify = new List<NotifyInfo.NEntry>(0);
                ninfo.Save_unlocked();
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        static bool SendQNEmail(string smtp, string toaddresses, string subject, string body)
        {
            if (-1 != toaddresses.IndexOf('<'))
            {
                string xw = toaddresses.Substring(toaddresses.IndexOf('<'));
                if (xw.Length > 30)
                {
                    xw = xw.Substring(0, 30 - 3) + "...";
                }
                throw new Exception("Invalid email address at " + xw);
            }
            toaddresses = toaddresses.Replace(" ", "");
            string[] addrs = toaddresses.Split(new char[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries);
            if (0 == addrs.Length)
            {
                throw new Exception("No email addresses: " + toaddresses);
            }

            {
                System.Net.Mail.MailMessage msg = new System.Net.Mail.MailMessage();
                foreach (string addr in addrs)
                {
                    msg.Bcc.Add(new System.Net.Mail.MailAddress(addr));
                }
                msg.Subject = subject;
                msg.Body = body;
                msg.IsBodyHtml = false;
                msg.From = new System.Net.Mail.MailAddress("qizmt-noreply@myspace-inc.com", "Qizmt Notifier");
                //msg.ReplyTo = new System.Net.Mail.MailAddress(); // Qizmt support email?

                System.Net.Mail.SmtpClient client = new System.Net.Mail.SmtpClient(smtp);
                client.Send(msg);
            }

            return true;
        }

    }

}