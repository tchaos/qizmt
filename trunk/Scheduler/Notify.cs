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

                MySpace.DataMining.AELight.dfs dc = null; // Load once if needed.

                {
                    DateTime starttime = DateTime.Now;
                    if (null == ninfo.LastHistoryTime)
                    {
                        ninfo.LastHistoryTime = starttime.ToString();
                        ninfo.Save();
                    }
                    else
                    {
                        bool AnyNotifyAll = false;
                        for (int i = 0; i < ninfo.Notify.Count; i++)
                        {
                            NotifyInfo.NEntry ne = ninfo.Notify[i];
                            if (-1 == ne.WaitOnJID)
                            {
                                AnyNotifyAll = true;
                                break;
                            }
                        }
                        if (null == ninfo.NewHistory)
                        {
                            if (AnyNotifyAll)
                            {
                                ninfo.NewHistory = new List<NotifyInfo.ConfigNewHistory>();
                            }
                        }
                        else
                        {
                            if (!AnyNotifyAll)
                            {
                                ninfo.NewHistory = null;
                                ninfo.LastHistoryTime = starttime.ToString();
                                ninfo.Save();
                            }
                        }
                        if (AnyNotifyAll)
                        {
                            DateTime lasthistory = DateTime.Parse(ninfo.LastHistoryTime);
                            try
                            {
                                using (System.IO.FileStream stmhist = new System.IO.FileStream("execlog.txt", System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))
                                {
                                    long stmhistLength = stmhist.Length;
                                    bool skipfirst = false;
                                    if (stmhistLength > 0x400 * 8)
                                    {
                                        stmhist.Position = stmhistLength - (0x400 * 8);
                                        skipfirst = true;
                                    }
                                    System.IO.StreamReader srhist = new System.IO.StreamReader(stmhist);
                                    if (skipfirst)
                                    {
                                        srhist.ReadLine(); // Probably partial line, ignore it.
                                    }
                                    string ln;
                                    while (null != (ln = srhist.ReadLine()))
                                    {
                                        if (-1 != ln.IndexOf(" exec ", StringComparison.OrdinalIgnoreCase))
                                        {
                                            string user = null, stime = null, cmd = null, snhjid = null;
                                            if (ParseExecLogLine(ln, ref user, ref stime, ref cmd, ref snhjid))
                                            {
                                                DateTime time = DateTime.Parse(stime);
                                                if (time > lasthistory)
                                                {
                                                    string secondarg = "";
                                                    {
                                                        int secondargstart = 0;
                                                        if (cmd.StartsWith("\""))
                                                        {
                                                            secondargstart = cmd.IndexOf('"', 1);
                                                            secondargstart++;
                                                        }
                                                        secondargstart = cmd.IndexOf(' ', secondargstart);
                                                        secondargstart++;
                                                        int secondargend = cmd.IndexOf(' ', secondargstart);
                                                        if (-1 != secondargend)
                                                        {
                                                            secondarg = cmd.Substring(secondargstart, secondargend - secondargstart).Trim('"');
                                                        }
                                                    }
                                                    if (0 == string.Compare("exec", secondarg, StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        NotifyInfo.ConfigNewHistory cnh = new NotifyInfo.ConfigNewHistory();
                                                        cnh.JID = long.Parse(snhjid);
                                                        //cnh.User = user;
                                                        //cnh.Time = stime;
                                                        //cnh.Command = cmd;
                                                        cnh.History = ln;
                                                        ninfo.NewHistory.Add(cnh);
                                                    }
                                                }
                                            }
                                        }

                                    }
                                }
                            }
                            catch (System.IO.FileNotFoundException)
                            {
                            }
                            {
                                ninfo.LastHistoryTime = starttime.ToString();
                                ninfo.Save();
                            }

                            {
                                // Handle the 'notifyfinish <JobInfo>' cases now.
                                for (int inh = 0; inh < ninfo.NewHistory.Count; inh++)
                                {
                                    NotifyInfo.ConfigNewHistory cnh = ninfo.NewHistory[inh];
                                    if (!System.IO.File.Exists(cnh.JID.ToString() + ".jid"))
                                    {
                                        ninfo.NewHistory.RemoveAt(inh);
                                        inh--;
                                        ninfo.Save();
                                        for (int i = 0; i < ninfo.Notify.Count; i++)
                                        {
                                            NotifyInfo.NEntry ne = ninfo.Notify[i];
                                            if (-1 == ne.WaitOnJID)
                                            {
                                                if (System.Text.RegularExpressions.Regex.IsMatch(cnh.History, ne.WaitOnHistoryRegex,
                                                    System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                                                {
                                                    try
                                                    {
                                                        string clustername = "N/A";
                                                        string SMTP;
                                                        {
                                                            if (dc == null)
                                                            {
                                                                dc = MySpace.DataMining.AELight.dfs.ReadDfsConfig_unlocked(
                                                                    MySpace.DataMining.AELight.dfs.DFSXMLNAME);
                                                            }
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
                                                            sbbody.AppendLine(ne.GetFinishedUserMessage());
                                                            sbbody.AppendFormat("{1} - Cluster Name{0}", Environment.NewLine, clustername);
                                                            //sbbody.AppendFormat("{1} - Command{0}", Environment.NewLine, cnh.Command); // Insecure.
                                                            sbbody.AppendFormat("{1} - Job Identifier{0}", Environment.NewLine, cnh.JID);
                                                            sbbody.AppendFormat("{1} - Notify Identifier{0}", Environment.NewLine, ne.ID);
                                                            sbbody.AppendFormat("{1} - Completion Time{0}", Environment.NewLine, DateTime.Now);
                                                            sbbody.AppendFormat("{1} - User Added Notify{0}", Environment.NewLine, ne.GetPlainUserAdded());
                                                            body = sbbody.ToString();
                                                        }
                                                        SendQNEmail(SMTP, ne.Email, subject, body);
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        throw new Exception("Problem sending email to " + ne.Email + " after JID " + ne.WaitOnJID + " finished  (notifyfinish <JobInfo>)", e);
                                                    }

                                                    System.Threading.Thread.Sleep(1000 * 3);
                                                }

                                            }
                                        }
                                        wait = false;
                                        break;
                                    }
                                }
                            }

                        }
                    }
                    for (int i = 0; i < ninfo.Notify.Count; i++)
                    {
                        NotifyInfo.NEntry ne = ninfo.Notify[i];
                        if (-1 == ne.WaitOnJID)
                        {
                        }
                        else if (!System.IO.File.Exists(ne.WaitOnJID.ToString() + ".jid"))
                        {
                            ninfo.Notify.RemoveAt(i);
                            i--;
                            ninfo.Save();
                            try
                            {
                                string clustername = "N/A";
                                string SMTP;
                                {
                                    if (dc == null)
                                    {
                                        dc = MySpace.DataMining.AELight.dfs.ReadDfsConfig_unlocked(
                                            MySpace.DataMining.AELight.dfs.DFSXMLNAME);
                                    }
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
                                    sbbody.AppendLine(ne.GetFinishedUserMessage());
                                    sbbody.AppendFormat("{1} - Cluster Name{0}", Environment.NewLine, clustername);
                                    sbbody.AppendFormat("{1} - Job Identifier{0}", Environment.NewLine, ne.WaitOnJID);
                                    sbbody.AppendFormat("{1} - Notify Identifier{0}", Environment.NewLine, ne.ID);
                                    sbbody.AppendFormat("{1} - Completion Time{0}", Environment.NewLine, DateTime.Now);
                                    sbbody.AppendFormat("{1} - User Added Notify{0}", Environment.NewLine, ne.GetPlainUserAdded());
                                    body = sbbody.ToString();
                                }
                                SendQNEmail(SMTP, ne.Email, subject, body);
                            }
                            catch (Exception e)
                            {
                                throw new Exception("Problem sending email to " + ne.Email + " after JID " + ne.WaitOnJID + " finished (notifyfinish <JobID>)", e);
                            }
                            wait = false;
                            break;
                        }
                    }
                }

            }
        }


        internal static bool ParseExecLogLine(string line, ref string User, ref string sTime, ref string Command, ref string sJID)
        {
            int nameend = line.IndexOf(" [");
            if (-1 != nameend)
            {
                int itime = nameend + 2;
                int itimeend = line.IndexOf("] ", itime);
                if (-1 != itimeend)
                {
                    string stime = line.Substring(itime, itimeend - itime);
                    {
                        int icmdstart = -1;
                        string nhsjid = null;
                        {
                            int ijid = line.IndexOf(" @JID#");
                            if (-1 != ijid)
                            {
                                ijid += 6;
                                int ijidend = line.IndexOf(' ', ijid);
                                icmdstart = ijidend + 1;
                                if (-1 != ijidend)
                                {
                                    nhsjid = line.Substring(ijid, ijidend - ijid);
                                }
                            }
                        }
                        if (null != nhsjid)
                        {
                            //long nhjid = long.Parse(nhsjid);
                            string cmd = line.Substring(icmdstart);

                            User = line.Substring(0, nameend);
                            sTime = stime;
                            sJID = nhsjid;
                            Command = cmd;
                            return true;
                        }
                    }
                }
            }
            return false;
        }


        public class NotifyInfo
        {
            public const string NOTIFYXMLNAME = "notify.xml";
            public const string MUTEXNAME = "Notifier{353B473D-3E4E-40c8-ABC3-C5FD82B54B8F}";


            [System.Xml.Serialization.XmlIgnore]
            public bool FileExists = false;


            public long LastNotifyID = 0;


            public string LastHistoryTime = null;


            public class NEntry : ScheduleInfo.IEntry
            {
                public long ID = 0;
                public DateTime TimeAdded; // When this entry was added.
                public string UserAdded; // Who added this entry.
                public long WaitOnJID; // -1 means use WaitOnJobInfo and WaitOnHistoryRegex.
                public string WaitOnJobInfo = null;
                public string WaitOnHistoryRegex = null;
                public string Email;
                public string UserMessage = null;

                public long GetID() { return ID; }
                public DateTime GetTimeAdded() { return TimeAdded; }
                public string GetUserAdded() { return UserAdded; }

                public string GetFinishedUserMessage()
                {
                    if (string.IsNullOrEmpty(UserMessage))
                    {
                        return "Job Completed";
                    }
                    return UserMessage;
                }

                public string GetPlainUserAdded()
                {
                    int iat = UserAdded.IndexOf('@');
                    if (-1 != iat)
                    {
                        return UserAdded.Substring(0, iat);
                    }
                    return UserAdded;
                }

            }

            public List<NEntry> Notify;


            public int FindNEntryByID(long id)
            {
                return ScheduleInfo.FindID<NEntry>(id, Notify);
            }


            public class ConfigNewHistory
            {
                public long JID;
                //public string User;
                //public string Time;
                //public string Command;
                public string History;
            }

            public List<ConfigNewHistory> NewHistory;


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


        static NotifyInfo.NEntry _AddNotify(long WaitOnJID, string JobInfo, string email, string UserAdded, string UserMessage)
        {
            NotifyInfo.NEntry ne = new NotifyInfo.NEntry();
            ne.WaitOnJID = WaitOnJID;
            ne.WaitOnJobInfo = JobInfo;
            string HistoryRegex = null;
            if (JobInfo != null)
            {
                HistoryRegex = MySpace.DataMining.AELight.Surrogate.WildcardRegexSubstring(JobInfo);
                if (!JobInfo.StartsWith("*"))
                {
                    HistoryRegex = @"\b" + HistoryRegex;
                }
                if (!JobInfo.EndsWith("*"))
                {
                    HistoryRegex += @"\b";
                }
            }
            ne.WaitOnHistoryRegex = HistoryRegex;
            ne.Email = email;
            ne.UserAdded = UserAdded;
            ne.UserMessage = UserMessage;
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

        public static NotifyInfo.NEntry AddNotify(long WaitOnJID, string email, string UserAdded, string JobInfo)
        {
            return _AddNotify(WaitOnJID, null, email, UserAdded, JobInfo);
        }

        public static NotifyInfo.NEntry AddNotify(string HistoryRegex, string email, string UserAdded, string JobInfo)
        {
            return _AddNotify(-1, HistoryRegex, email, UserAdded, JobInfo);
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