using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{
    public partial class Scheduler
    {


        public static Queue<string> LastQueueActions;

        public static void RunQueueService()
        {
            LastQueueActions = new Queue<string>(5);
            LastQueueActions.Enqueue("Starting queue service thread");
            for (bool wait = true; ; )
            {
                if (wait)
                {
                    System.Threading.Thread.Sleep(1000 * 30);
                }
                else
                {
                    System.Threading.Thread.Sleep(1000 * 10);
                }
                wait = true;

                ScheduleInfo.QEntry qe = null;
                bool ispaused;

                {
                    System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
                    try
                    {
                        mu.WaitOne();
                    }
                    catch (System.Threading.AbandonedMutexException)
                    {
                    }
                    try
                    {
                        ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                        ispaused = sched.QueuePaused;
                        if (!ispaused && sched.Queued.Count > 0)
                        {
                            qe = sched.Queued[0];
                            sched.Queued.RemoveAt(0);
                            sched.Save();
                        }
                    }
                    finally
                    {
                        mu.ReleaseMutex();
                        IDisposable dmu = mu;
                        dmu.Dispose();
                    }
                }

                if (null == qe)
                {
                    if (LastQueueActions.Count >= 5)
                    {
                        LastQueueActions.Dequeue();
                    }
                    if (ispaused)
                    {
                        LastQueueActions.Enqueue("Queue is paused; waiting for unpause");
                    }
                    else
                    {
                        LastQueueActions.Enqueue("Queue is empty; waiting for queue entry");
                    }
                }
                else
                {
                    wait = false;
                    int WaitForProcessTimeout;
                    if (qe.Timeout <= 0)
                    {
                        WaitForProcessTimeout = int.MaxValue; // Infinity for Process.WaitForExit.
                    }
                    else
                    {
                        WaitForProcessTimeout = qe.Timeout * 1000;
                    }
                    try
                    {
                        RunQizmtExec(qe.GetCommand(), qe.GetOut(), qe.GetError(), WaitForProcessTimeout, LastQueueActions);
                    }
                    catch (RunCommandTimeout rct)
                    {
                        if (!string.IsNullOrEmpty(qe.TimeoutCommand))
                        {
                            string tcmd = qe.TimeoutCommand;
                            for (; ; )
                            {
                                string JIDREPL = "#JID#";
                                int ij = tcmd.IndexOf(JIDREPL, StringComparison.OrdinalIgnoreCase);
                                if (-1 == ij)
                                {
                                    break;
                                }
                                tcmd = tcmd.Substring(0, ij) + rct.JID + tcmd.Substring(ij + JIDREPL.Length);
                            }
                            RunQizmtExec(tcmd);
                        }

                    }

                }

            }
        }


        internal class RunningSEntry
        {
            internal ScheduleInfo.SEntry sentry;
            internal System.Threading.Thread thread;
            internal int result = int.MinValue;
        }

        static List<RunningSEntry> RunningScheduledTasks;

        static bool IsScheduledTaskRunning(long sid)
        {
            if (null == RunningScheduledTasks)
            {
                return false;
            }
            lock (RunningScheduledTasks)
            {
                foreach (RunningSEntry rse in RunningScheduledTasks)
                {
                    if (sid == rse.sentry.ID)
                    {
                        // Just seeing if it's in the array!
                        return true;
                    }
                }
            }
            return false;
        }

        
        public static void RunScheduleService()
        {
            RunningScheduledTasks = new List<RunningSEntry>();
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

                lock (RunningScheduledTasks)
                {
                    for (int ir = 0; ir < RunningScheduledTasks.Count; ir++)
                    {
                        if (RunningScheduledTasks[ir].thread.Join(0))
                        {
                            RunningScheduledTasks.RemoveAt(ir);
                            ir--;
                        }
                    }
                }

                ScheduleInfo.SEntry se = null;

                {
                    System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
                    try
                    {
                        mu.WaitOne();
                    }
                    catch (System.Threading.AbandonedMutexException)
                    {
                    }
                    try
                    {
                        ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                        if (sched.Scheduled.Count > 0)
                        {
                            sched.SortScheduled();
                            for (int inext = 0; inext < sched.Scheduled.Count; inext++)
                            {
                                if (sched.Scheduled[inext].Paused)
                                {
                                    // Skip this one, it's paused.
                                    continue;
                                }
                                if (DateTime.Now >= sched.Scheduled[inext].NextRun)
                                {
                                    if (IsScheduledTaskRunning(sched.Scheduled[inext].ID))
                                    {
                                        // Skip this one, it's still running.
                                        continue;
                                    }
                                    se = sched.Scheduled[inext];
                                    if (!se.CalculateNextRun())
                                    {
                                        sched.Scheduled.RemoveAt(inext);
                                    }
                                    sched.Save();
                                }
                                break;
                            }
                        }
                    }
                    finally
                    {
                        mu.ReleaseMutex();
                        IDisposable dmu = mu;
                        dmu.Dispose();
                    }
                }

                if (null != se)
                {
                    wait = false;
                    RunningSEntry rse = new RunningSEntry();
                    rse.sentry = se;
                    rse.thread = new System.Threading.Thread(
                        new System.Threading.ThreadStart(
                        delegate
                        {
                            rse.result = RunQizmtExec(se.GetCommand(), se.GetOut(), se.GetError());
                        }));
                    rse.thread.IsBackground = true;
                    rse.thread.Name = "_ScheduledRunCommand" + se.GetCommand().GetHashCode().ToString();
                    RunningScheduledTasks.Add(rse);
                    rse.thread.Start();
                }

            }
        }


        public static bool IsQizmtExecCommand(string Cmd)
        {
            if (Cmd.StartsWith("qizmt", StringComparison.OrdinalIgnoreCase) // "qizmt ..." and "qizmt.exe ..."
                    || Cmd.StartsWith("dspace", StringComparison.OrdinalIgnoreCase)) // "dspace ..." and "dspace.exe ..."
            {
                foreach (string word in Cmd.Split(' '))
                {
                    if (0 == string.Compare(word, "exec", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
            return false;
        }


        public class RunCommandTimeout : Exception
        {
            public RunCommandTimeout(System.Diagnostics.Process RunProcess, long JID)
                : base("Command timed out")
            {
                this.RunProcess = RunProcess;
                this.JID = JID;
            }

            public RunCommandTimeout(System.Diagnostics.Process RunProcess)
                : this(RunProcess, -1)
            {
            }

            public System.Diagnostics.Process RunProcess;
            public long JID;
        }

        public static int RunQizmtExec(string Cmd)
        {
            return RunQizmtExec(Cmd, null, null);
        }

        public static int RunQizmtExec(string Cmd, string Out, string Err)
        {
            return RunQizmtExec(Cmd, Out, Err, int.MaxValue, null);
        }

        // If StatusQueue is not null, LastDequeuedJID will be updated with the JID.
        public static int RunQizmtExec(string Cmd, string Out, string Err, int WaitForProcessTimeout, Queue<string> StatusQueue)
        {
            try
            {
                long jid = -1;
                bool IsQizmtExec = IsQizmtExecCommand(Cmd);
                if (!IsQizmtExec)
                {
                    throw new Exception("Must be qizmt exec: " + Cmd);
                }

                System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo("cmd.exe", @"/C " + Cmd);
                psi.CreateNoWindow = true;
                //psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
                psi.UseShellExecute = false;
                if (null != Out || IsQizmtExec)
                {
                    psi.RedirectStandardOutput = true;
                }
                if (null != Err)
                {
                    psi.RedirectStandardError = true;
                }
                int result;
                Exception thde = null;
                System.Threading.Thread outthd = null;
                System.Threading.Thread errthd = null;
                StringBuilder jidfinder = null;
                if (IsQizmtExec)
                {
                    jidfinder = new StringBuilder(60);
                }
                using (System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi))
                {
                    if (psi.RedirectStandardOutput)
                    {
                        outthd = new System.Threading.Thread(
                            new System.Threading.ThreadStart(
                            delegate()
                            {
                                try
                                {
                                    char[] rbuf = new char[0x400 * 4];
                                    System.IO.FileStream fs = null;
                                    System.IO.StreamWriter tw = null;
                                    try
                                    {
                                        for (; ; )
                                        {
                                            int read = proc.StandardOutput.Read(rbuf, 0, rbuf.Length);
                                            if (read < 1)
                                            {
                                                break;
                                            }
                                            lock (proc)
                                            {
                                                if (IsQizmtExec && -1 == jid && null != jidfinder)
                                                {
                                                    jidfinder.Append(rbuf, 0, read);
                                                    for (int ijf = 0; ijf < jidfinder.Length; ijf++)
                                                    {
                                                        if ('\n' == jidfinder[ijf])
                                                        {
                                                            try
                                                            {
                                                                string sj = jidfinder.ToString();
                                                                jidfinder = null;
                                                                const string JIDLINESTART = "Job Identifier: ";
                                                                if (sj.StartsWith(JIDLINESTART))
                                                                {
                                                                    sj = sj.Substring(JIDLINESTART.Length);
                                                                    int inl = sj.IndexOf('\n');
                                                                    if (-1 != inl)
                                                                    {
                                                                        sj = sj.Substring(0, inl).Trim();
                                                                        jid = long.Parse(sj);
                                                                        if (null != StatusQueue)
                                                                        {
                                                                            ScheduleInfo sched = ScheduleInfo.Load();
                                                                            sched.LastDequeuedJID = jid;
                                                                            sched.Save();
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            catch
                                                            {
                                                            }
                                                            break;
                                                        }
                                                    }
                                                }
                                                if (null != Out)
                                                {
                                                    if (null == tw)
                                                    {
                                                        fs = new System.IO.FileStream(Out, System.IO.FileMode.Append,
                                                            System.IO.FileAccess.Write, System.IO.FileShare.ReadWrite);
                                                        tw = new System.IO.StreamWriter(fs);
                                                    }
                                                    fs.Seek(0, System.IO.SeekOrigin.End);
                                                    tw.Write(rbuf, 0, read);
                                                    tw.Flush();
                                                }
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        if (null != tw)
                                        {
                                            lock (proc)
                                            {
                                                tw.Close();
                                                fs.Close();
                                            }
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    thde = e;
                                }
                            }));
                        outthd.IsBackground = true;
                        outthd.Start();
                    }
                    if (psi.RedirectStandardError)
                    {
                        errthd = new System.Threading.Thread(
                            new System.Threading.ThreadStart(
                            delegate()
                            {
                                try
                                {
                                    char[] rbuf = new char[0x400 * 4];
                                    System.IO.FileStream fs = null;
                                    System.IO.StreamWriter tw = null;
                                    try
                                    {
                                        for (; ; )
                                        {
                                            int read = proc.StandardError.Read(rbuf, 0, rbuf.Length);
                                            if (read < 1)
                                            {
                                                break;
                                            }
                                            lock (proc)
                                            {
                                                if (null == tw)
                                                {
                                                    fs = new System.IO.FileStream(Err, System.IO.FileMode.Append,
                                                        System.IO.FileAccess.Write, System.IO.FileShare.ReadWrite);
                                                    tw = new System.IO.StreamWriter(fs);
                                                }
                                                fs.Seek(0, System.IO.SeekOrigin.End);
                                                tw.Write(rbuf, 0, read);
                                                tw.Flush();
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        if (null != tw)
                                        {
                                            lock (proc)
                                            {
                                                tw.Close();
                                                fs.Close();
                                            }
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    thde = e;
                                }
                            }));
                        errthd.IsBackground = true;
                        errthd.Start();
                    }
                    string jidfile = null;
                    for (int ijidtries = 0; ijidtries < 30; ijidtries++)
                    {
                        lock (proc)
                        {
                            if (-1 != jid)
                            {
                                jidfile = jid.ToString() + ".jid";
                                break;
                            }
                        }
                        System.Threading.Thread.Sleep(1000);
                    }
                    for (; ; )
                    {
                        if (null != StatusQueue)
                        {
                            if (StatusQueue.Count >= 5)
                            {
                                StatusQueue.Dequeue();
                            }
                            StatusQueue.Enqueue("Waiting on JID " + jid);
                        }
                        if (proc.WaitForExit(1000 * 10))
                        {
                            result = proc.ExitCode;
                            break;
                        }
                        if (-1 != jid)
                        {
                            if (!System.IO.File.Exists(jidfile))
                            {
                                // Give it 10 more secs to finish cleanly...
                                if (proc.WaitForExit(1000 * 10))
                                {
                                    result = proc.ExitCode;
                                }
                                else
                                {
                                    result = 42842;
                                }
                                break;
                            }
                        }
                        {
                            bool timeout = false;
                            if (WaitForProcessTimeout != int.MaxValue && WaitForProcessTimeout >= 0)
                            {
                                WaitForProcessTimeout -= 1000 * 10;
                                if (WaitForProcessTimeout <= 0)
                                {
                                    timeout = true;
                                }
                            }
                            if (timeout)
                            {
                                try
                                {
                                    errthd.Abort();
                                }
                                catch
                                {
                                }
                                try
                                {
                                    outthd.Abort();
                                }
                                catch
                                {
                                }
                                // Don't actually Kill the process,
                                // just notify that it took longer than expected.
                                // Plus, it would only kill cmd.exe
                                throw new RunCommandTimeout(proc, jid);
                            }
                        }
                    }
                }
                if (null != outthd)
                {
                    if (!outthd.Join(1000 * 60))
                    {
                        try
                        {
                            errthd.Abort();
                        }
                        catch
                        {
                        }
                        try
                        {
                            outthd.Abort();
                        }
                        catch
                        {
                        }
                        //throw new Exception("Took too long waiting for StandardOutput thread to finish");
                    }
                    outthd = null;
                }
                if (null != errthd)
                {
                    if (!errthd.Join(1000 * 60))
                    {
                        try
                        {
                            errthd.Abort();
                        }
                        catch
                        {
                        }
                        //throw new Exception("Took too long waiting for StandardError thread to finish");
                    }
                    errthd = null;
                }
                if (null != thde)
                {
                    throw thde;
                }
                return result;
            }
            catch (RunCommandTimeout)
            {
                throw;
            }
            catch (Exception e)
            {
                if (null != Err)
                {
                    try
                    {
                        using (System.IO.FileStream fs = new System.IO.FileStream(Err, System.IO.FileMode.Append,
                                           System.IO.FileAccess.Write, System.IO.FileShare.Read))
                        {
                            System.IO.StreamWriter tw = new System.IO.StreamWriter(fs);
                            tw.WriteLine(
                                Environment.NewLine
                                + "----------------------------------------------------------------"
                                + Environment.NewLine
                                + "Exception executing command: " + Cmd
                                + Environment.NewLine
                                + "Time of exception: " + DateTime.Now.ToString()
                                + Environment.NewLine
                                + e.ToString()
                                + Environment.NewLine
                                + "----------------------------------------------------------------"
                                + Environment.NewLine
                                );
                        }
                    }
                    catch
                    {
                    }
                }
                throw;
            }
        }


        public static IList<ScheduleInfo.QEntry> GetQueueSnapshot(out bool IsPaused)
        {
            ScheduleInfo sched = ScheduleInfo.Load();
            IsPaused = sched.QueuePaused;
            return sched.Queued;
        }

        public static IList<ScheduleInfo.QEntry> GetQueueSnapshot()
        {
            bool IsPaused;
            return GetQueueSnapshot(out IsPaused);
        }

        public static IList<ScheduleInfo.SEntry> GetScheduleSnapshot()
        {
            ScheduleInfo sched = ScheduleInfo.Load();
            sched.SortScheduled();
            return sched.Scheduled;
        }


        public static ScheduleInfo.QEntry Enqueue(string[] args, string user)
        {
            ScheduleInfo.QEntry qe = ScheduleInfo.QEntryFromArgs(args, user);
            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                ScheduleInfo.QEntry result = sched.Enqueue_unlocked(qe);
                sched.Save_unlocked();
                return result;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static bool QueueKill(long QueueID)
        {
            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                int index = sched.FindQEntryByID(QueueID);
                if (-1 == index)
                {
                    return false;
                }
                sched.Queued.RemoveAt(index);
                sched.Save_unlocked();
                return true;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static void ClearQueue()
        {
            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                if (sched.Queued.Count < 1)
                {
                    return;
                }
                sched.Queued = new List<ScheduleInfo.QEntry>(0);
                sched.Save_unlocked();
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static bool PauseQueue(bool paused)
        {
            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                sched.QueuePaused = paused;
                sched.Save_unlocked();
                return true;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static long GetLastDequeuedJID()
        {
            return ScheduleInfo.Load().LastDequeuedJID;
        }


        public static ScheduleInfo.SEntry Schedule(string[] args, string user)
        {
            ScheduleInfo.SEntry se = ScheduleInfo.SEntryFromArgs(args, user);
            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                ScheduleInfo.SEntry result = sched.Schedule_unlocked(se);
                sched.Save_unlocked();
                return result;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static bool PauseSchedule(long ScheduleID, bool paused)
        {
            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                int index = sched.FindSEntryByID(ScheduleID);
                if (-1 == index)
                {
                    return false;
                }
                sched.Scheduled[index].Paused = paused;
                if (!paused)
                {
                    sched.Scheduled[index].CalculateNextRun();
                }
                sched.Save_unlocked();
                return true;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static bool Unschedule(long ScheduleID)
        {
            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                int index = sched.FindSEntryByID(ScheduleID);
                if (-1 == index)
                {
                    return false;
                }
                sched.Scheduled.RemoveAt(index);
                sched.Save_unlocked();
                return true;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        public static void ClearSchedule()
        {
            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked();
                if (sched.Scheduled.Count < 1)
                {
                    return;
                }
                sched.Scheduled = new List<ScheduleInfo.SEntry>(0);
                sched.Save_unlocked();
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        // Requires setting a new backup location, but NewBackupDir can be the same as FromBackupDir.
        // NewBackupDir can be null to remove backup.
        // Returns true if restored, false if not.
        public static bool BackupRestore(string FromBackupDir, string RestoreToDir, string NewBackupDir)
        {
            bool result = true;
            if (string.IsNullOrEmpty(FromBackupDir))
            {
                result = false;
            }
            string bfn = ScheduleInfo.SCHEDULEXMLNAME + ".backup";
            string bfp = (null == FromBackupDir) ? null : (FromBackupDir + @"\" + bfn);

            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                if (null != bfp)
                {
                    if (!System.IO.File.Exists(bfp))
                    {
                        //throw new Exception("Scheduler backup not found at: " + FromBackupDir);
                        result = false;
                    }
                    System.IO.File.Copy(bfp, RestoreToDir + @"\" + ScheduleInfo.SCHEDULEXMLNAME, true);
                }
                SetBackupLocation(NewBackupDir, RestoreToDir + @"\" + ScheduleInfo.SCHEDULEXMLNAME);
                return result;
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }


        // dir can be null to clear the backup location, but doesn't delete any backups.
        // Otherwise, dir must be a full network path to a directory, and a backup is written.
        public static void SetBackupLocation(string NewBackupDir, string schedulexmlpath)
        {
            if (string.IsNullOrEmpty(NewBackupDir))
            {
                NewBackupDir = null;
            }
            if (null != NewBackupDir
                && !NewBackupDir.StartsWith(@"\\"))
            {
                throw new Exception("Scheduler.SetBackupLocation: backup location must be a network path to a directory");
            }

            System.Threading.Mutex mu = new System.Threading.Mutex(false, ScheduleInfo.MUTEXNAME);
            try
            {
                mu.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                ScheduleInfo sched = ScheduleInfo.Load_unlocked(schedulexmlpath);
                sched.BackupDirectory = NewBackupDir;
                sched.Save_unlocked(schedulexmlpath); // Saves this change and writes a backup.
            }
            finally
            {
                mu.ReleaseMutex();
                IDisposable dmu = mu;
                dmu.Dispose();
            }
        }

        public static void SetBackupLocation(string NewBackupDir)
        {
            SetBackupLocation(NewBackupDir, APPDIR + @"\" + ScheduleInfo.SCHEDULEXMLNAME);
        }


        internal static readonly string APPDIR = System.IO.Path.GetDirectoryName(
            System.Reflection.Assembly.GetExecutingAssembly().Location);


        public class ScheduleInfo
        {
            public const string SCHEDULEXMLNAME = "schedule.xml";
            public const string MUTEXNAME = "Scheduler{18373967-3C3B-4c37-BB77-6DC23896C883}";


            public string BackupDirectory = null; // null for no backup.


            [System.Xml.Serialization.XmlIgnore]
            public bool FileExists = false;


            public long LastQueueID = 0;

            public long LastScheduleID = 0;

            public long LastDequeuedJID = 0;

            public interface ICommand
            {
                string GetCommand();
                string GetOut();
                string GetError();
            }

            public interface IEntry
            {
                long GetID();
                DateTime GetTimeAdded();
                string GetUserAdded();
            }

            public class QEntry : IEntry, ICommand, IComparable<QEntry>
            {
                public long ID = 0;
                public DateTime TimeAdded; // When this entry was added.
                public string UserAdded; // Who added this entry.
                public string Command;
                public string Out;
                public string Error;
                public int Timeout = 0;
                public string TimeoutCommand;

                public long GetID() { return ID; }
                public DateTime GetTimeAdded() { return TimeAdded; }
                public string GetUserAdded() { return UserAdded; }
                public string GetCommand() { return Command; }
                public string GetOut() { return Out; }
                public string GetError() { return Error; }

                public int CompareTo(QEntry that)
                {
                    long x = this.ID - that.ID;
                    if (x < 0)
                    {
                        return -1;
                    }
                    if (x > 0)
                    {
                        return 1;
                    }
                    return 0;
                }

            }
            public List<QEntry> Queued;

            public bool QueuePaused = false;

            public class SEntry : IEntry, ICommand, IComparable<SEntry>
            {
                public long ID = 0;
                public DateTime TimeAdded = DateTime.MinValue; // When this entry was added.
                public string UserAdded; // Who added this entry.
                public string Command;
                public DateTime NextRun;
                public long Frequency = -1; // Seconds.
                public string texceptions; // <TimeSpec>[-<TimeSpec>]
                public string wexceptions;
                public string wtexceptions; // WT_FORMAT
                public bool Paused = false;

                public string GetNextRunString()
                {
                    if (Paused)
                    {
                        return "<paused>";
                    }
                    return NextRun.ToString();
                }

                public long GetID() { return ID; }
                public DateTime GetTimeAdded() { return TimeAdded; }
                public string GetUserAdded() { return UserAdded; }
                public string GetCommand() { return Command; }
                public string GetOut() { return null; }
                public string GetError() { return null; }

                public class TimeSpec
                {
                    public bool DateSpecified
                    {
                        get
                        {
                            return year != -1
                                || month != -1
                                || day != -1;
                        }
                    }
                    public bool TimeSpecified
                    {
                        get
                        {
                            return hour != -1
                                || minute != -1
                                || second != -1;
                        }
                    }
                    public short year = -1;
                    public sbyte month = -1;
                    public sbyte day = -1;
                    public sbyte weekday = -1;
                    public sbyte hour = -1;
                    public sbyte Get24Hour()
                    {
                        sbyte h = hour;
                        if (h >= 0)
                        {
                            switch (ampm)
                            {
                                case AMPM.NEITHER:
                                    break;
                                case AMPM.AM:
                                    if (12 == h)
                                    {
                                        h = 0;
                                    }
                                    break;
                                case AMPM.PM:
                                    if (12 != h)
                                    {
                                        h += 12;
                                    }
                                    break;
                            }
                        }
                        return h;
                    }
                    public sbyte minute = -1;
                    public sbyte second = -1;
                    public enum AMPM : byte
                    {
                        NEITHER,
                        AM,
                        PM,
                    }
                    public AMPM ampm = AMPM.NEITHER;


                    public const string DATE_FORMAT = "M[/D[/Y]]";
                    public const string TIME_FORMAT = "h:m[:s][AM|PM]";
                    public const string DATE_TIME_FORMAT = "[" + DATE_FORMAT + ".][" + TIME_FORMAT + "]";

                    // Weekday + time format.
                    public const string WT_FORMAT = "<weekday>@<" + TIME_FORMAT + ">[-<" + TIME_FORMAT + ">]";

                    public static TimeSpec Parse(string value)
                    {
                        {
                            int iat = value.IndexOf('@');
                            if (-1 != iat)
                            {
                                DayOfWeek wd = ParseDayOfWeek(value.Substring(0, iat));
                                string wtst = value.Substring(iat + 1);
                                if (-1 != wtst.IndexOf('.'))
                                {
                                    throw new FormatException("Weekday time cannot specify date: unexpected .");
                                }
                                TimeSpec wt = Parse(wtst);
                                wt.weekday = checked((sbyte)wd);
                                return wt;
                            }
                        }

                        string date, time;
                        {
                            int idot = value.IndexOf('.');
                            if (-1 != idot)
                            {
                                date = value.Substring(0, idot);
                                time = value.Substring(idot + 1);
                            }
                            else
                            {
                                if (-1 != value.IndexOf(':'))
                                {
                                    time = value;
                                    date = "";
                                }
                                else
                                {
                                    time = "";
                                    date = value;
                                }
                            }
                        }
                        date = date.Trim();
                        time = time.Trim();

                        TimeSpec ts = new TimeSpec();

                        if (0 != date.Length)
                        {
                            try
                            {
                                string[] slashparts = date.Split('/');
                                switch (slashparts.Length)
                                {
                                    case 1:
                                        ts.month = sbyte.Parse(slashparts[0]);
                                        break;
                                    case 2:
                                        ts.month = sbyte.Parse(slashparts[0]);
                                        ts.day = sbyte.Parse(slashparts[1]);
                                        break;
                                    case 3:
                                        ts.month = sbyte.Parse(slashparts[0]);
                                        ts.day = sbyte.Parse(slashparts[1]);
                                        if (slashparts[2].Length == 4)
                                        {
                                            ts.year = short.Parse(slashparts[2]);
                                        }
                                        else if (slashparts[2].Length == 2)
                                        {
                                            ts.year = (short)(2000 + short.Parse(slashparts[2]));
                                        }
                                        else
                                        {
                                            throw new Exception("Invalid length for year");
                                        }
                                        break;
                                    default:
                                        throw new Exception("Expected 1 to 3 parts separated by / for date: " + DATE_FORMAT);
                                }
                            }
                            catch (Exception e)
                            {
                                throw new FormatException("Date format error: " + e.Message, e);
                            }
                        }

                        if (0 != time.Length)
                        {
                            try
                            {
                                if (time.EndsWith("AM", StringComparison.OrdinalIgnoreCase))
                                {
                                    ts.ampm = AMPM.AM;
                                    time = time.Substring(0, time.Length - 2);
                                }
                                else if (time.EndsWith("PM", StringComparison.OrdinalIgnoreCase))
                                {
                                    ts.ampm = AMPM.PM;
                                    time = time.Substring(0, time.Length - 2);
                                }
                                string[] colonparts = time.Split(':');
                                switch (colonparts.Length)
                                {
                                    case 2:
                                        ts.hour = sbyte.Parse(colonparts[0]);
                                        ts.minute = sbyte.Parse(colonparts[1]);
                                        break;
                                    case 3:
                                        ts.hour = sbyte.Parse(colonparts[0]);
                                        ts.minute = sbyte.Parse(colonparts[1]);
                                        ts.second = sbyte.Parse(colonparts[2]);
                                        break;
                                    default:
                                        throw new FormatException("Expected 2 to 3 parts separated by : for time: " + TIME_FORMAT);
                                }
                            }
                            catch (Exception e)
                            {
                                throw new FormatException("Time format error: " + e.Message, e);
                            }
                        }

                        return ts;

                    }


                    public struct Range
                    {
                        public DateTime first, second; // Inclusive.
                    }


                    /*
                    public static Range ToRange(TimeSpec first, TimeSpec second)
                    {
                        return ToRange(first, second, DateTime.Now);
                    }
                     * */

                    public static Range ToRange(TimeSpec first, TimeSpec second, DateTime basetime)
                    {
                        Range result;
                        {
                            DateTime basetime2;
                            if (!first.DateSpecified)
                            {
                                basetime2 = new DateTime(basetime.Year, basetime.Month, basetime.Day, 0, 0, 0, 0);
                            }
                            else
                            {
                                basetime2 = new DateTime(basetime.Year, 1, 1, 0, 0, 0, 0);
                            }
                            result.first = first.ToDateTime(basetime2);
                        }
                        if (null == second)
                        {
                            second = first;
                        }
                        {
                            DateTime basetime2;
                            if (!first.DateSpecified)
                            {
                                basetime2 = new DateTime(basetime.Year, basetime.Month, basetime.Day, 23, 59, 59, 999);
                            }
                            else
                            {
                                basetime2 = new DateTime(basetime.Year, 12, 31, 23, 59, 59, 999);
                            }
                            result.second = second.ToDateTime(basetime2);
                        }
                        return result;
                    }


                    public DateTime ToDateTime()
                    {
                        DateTime now = DateTime.Now;
                        DateTime basetime;
                        if (!DateSpecified)
                        {
                            basetime = new DateTime(now.Year, now.Month, now.Day, 0, 0, 0, 0);
                        }
                        else
                        {
                            basetime = new DateTime(now.Year, 1, 1, 0, 0, 0, 0);
                        }
                        return ToDateTime(basetime);
                    }

                    public DateTime ToDateTime(DateTime basetime)
                    {
                        int y = year;
                        if (y < 0)
                        {
                            y = basetime.Year;
                        }
                        int mo = month;
                        if (mo < 0)
                        {
                            mo = basetime.Month;
                        }
                        int d = day;
                        if (d < 0)
                        {
                            d = basetime.Day;
                        }
                        int h = Get24Hour();
                        if (h < 0)
                        {
                            h = basetime.Hour;
                        }
                        int mi = minute;
                        if (mi < 0)
                        {
                            mi = basetime.Minute;
                        }
                        int s = second;
                        if (s < 0)
                        {
                            s = basetime.Second;
                        }
                        DateTime result = new DateTime(y, mo, d, h, mi, s);
                        if (-1 != weekday)
                        {
                            DayOfWeek dow = checked((DayOfWeek)weekday);
                            while (result.DayOfWeek != dow)
                            {
                                result = result.AddDays(1);
                            }
                        }
                        return result;
                    }

                    public override string ToString()
                    {
                        return ToDateTime().ToString();
                    }

                }

                /*
                public static List<TimeSpec.Range> ParseTExceptions(string texceptions)
                {
                    return ParseTExceptions(texceptions, DateTime.Now);
                }
                 * */

                public static List<TimeSpec.Range> ParseTExceptions(string texceptions, DateTime basetime)
                {
                    string[] texcs = texceptions.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                    List<TimeSpec.Range> results = new List<TimeSpec.Range>(texcs.Length);
                    foreach (string texc in texcs)
                    {
                        TimeSpec first, second = null;
                        int idash = texc.IndexOf('-');
                        if (-1 == idash)
                        {
                            first = TimeSpec.Parse(texc);
                        }
                        else
                        {
                            first = TimeSpec.Parse(texc.Substring(0, idash));
                            second = TimeSpec.Parse(texc.Substring(idash + 1));
                        }
                        results.Add(TimeSpec.ToRange(first, second, basetime));
                    }
                    return results;
                }

                public static DayOfWeek ParseDayOfWeek(string wday)
                {
                    switch (wday.ToLower())
                    {
                        case "sunday":
                        case "sun":
                            return DayOfWeek.Sunday;
                        case "monday":
                        case "mon":
                            return DayOfWeek.Monday;
                        case "tuesday":
                        case "tue":
                        case "tues":
                            return DayOfWeek.Tuesday;
                        case "wednesday":
                        case "wed":
                            return DayOfWeek.Wednesday;
                        case "thursday":
                        case "thu":
                        case "thurs":
                            return DayOfWeek.Thursday;
                        case "friday":
                        case "fri":
                            return DayOfWeek.Friday;
                        case "saturday":
                        case "sat":
                            return DayOfWeek.Saturday;
                        default:
                            throw new Exception("Unknown day-of-week: " + wday);
                    }
                }


                void FixNextRun()
                {
                    if (NextRun < DateTime.Now)
                    {
                        NextRun = DateTime.Now;
                    }
                    for (bool needfix = true; needfix; )
                    {
                        needfix = false;
                        // Consider time exceptions...
                        if (!string.IsNullOrEmpty(texceptions))
                        {
                            List<TimeSpec.Range> xrs = ParseTExceptions(texceptions, NextRun);
                            foreach (TimeSpec.Range xr in xrs)
                            {
                                if (NextRun >= xr.first
                                    && NextRun <= xr.second)
                                {
                                    NextRun = xr.second.AddSeconds(1);
                                    needfix = true;
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(wtexceptions))
                        {
                            List<TimeSpec.Range> xrs = ParseTExceptions(wtexceptions, NextRun);
                            foreach (TimeSpec.Range xr in xrs)
                            {
                                if (NextRun >= xr.first
                                    && NextRun <= xr.second)
                                {
                                    NextRun = xr.second.AddSeconds(1);
                                    needfix = true;
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(wexceptions))
                        {
                            DayOfWeek nextrunday = NextRun.DayOfWeek;
                            string[] wdays = wexceptions.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                            foreach (string wday in wdays)
                            {
                                if (nextrunday == ParseDayOfWeek(wday))
                                {
                                    DateTime addday = NextRun.AddDays(1);
                                    NextRun = new DateTime(addday.Year, addday.Month, addday.Day, 0, 0, 0);
                                    nextrunday = NextRun.DayOfWeek;
                                    needfix = true;
                                }
                            }
                        }
                    }
                }

                // Returns false if not ever running again.
                public bool CalculateNextRun()
                {
                    if (Frequency < 0)
                    {
                        return false;
                    }
                    NextRun = NextRun + TimeSpan.FromSeconds(Frequency);
                    FixNextRun();
                    return true;
                }

                public void CalculateFirstRun(DateTime start)
                {
                    NextRun = start;
                    FixNextRun();
                }

                public int CompareTo(SEntry that)
                {
                    return this.NextRun.CompareTo(that.NextRun);
                }

            }
            public List<SEntry> Scheduled;


            protected internal void SortScheduled()
            {
                Scheduled.Sort();
            }


            public static SEntry SEntryFromArgs(string[] args, string user)
            {
                SEntry se = new SEntry();
                se.UserAdded = user;
                string sstart = null;
                foreach (string arg in args)
                {
                    string name = arg;
                    string value = "";
                    {
                        int ieq = arg.IndexOf('=');
                        if (-1 != ieq)
                        {
                            name = arg.Substring(0, ieq);
                            value = arg.Substring(ieq + 1);
                        }
                    }
                    name = name.ToLower();
                    switch (name)
                    {
                        case "command":
                        case "cmd":
                            se.Command = value;
                            break;
                        case "start":
                            sstart = value;
                            break;
                        case "frequency":
                        case "freq":
                            se.Frequency = long.Parse(value);
                            if (se.Frequency < 0)
                            {
                                throw new FormatException("Frequency cannot be negative");
                            }
                            break;
                        case "texceptions":
                        case "texc":
                            se.texceptions = value;
                            break;
                        case "wexceptions":
                        case "wexc":
                            se.wexceptions = value;
                            break;
                        case "wtexceptions":
                        case "wtexc":
                            se.wtexceptions = value;
                            break;
                        case "stdout":
                        case "out":
                        case "output":
                            throw new Exception(arg + ": not supported for schedule");
                        case "stderr":
                        case "err":
                        case "error":
                            throw new Exception(arg + ": not supported for schedule");
                    }
                }
                if (string.IsNullOrEmpty(se.Command))
                {
                    throw new InvalidOperationException("Expected command=<value>");
                }
                if (string.IsNullOrEmpty(sstart))
                {
                    throw new InvalidOperationException("Expected start=<time> (where time is <" + SEntry.TimeSpec.DATE_TIME_FORMAT + "> or now)");
                }
                if (!IsQizmtExecCommand(se.Command))
                {
                    throw new InvalidOperationException("Command=<value> must be a Qizmt exec");
                }
                if (0 == string.Compare(sstart, "now", true))
                {
                    se.CalculateFirstRun(DateTime.Now);
                }
                else
                {
                    se.CalculateFirstRun(SEntry.TimeSpec.Parse(sstart).ToDateTime());
                }
                return se;
            }


            protected internal SEntry Schedule_unlocked(SEntry se)
            {
#if DEBUG
                if (0 != se.ID)
                {
                    throw new Exception("DEBUG:  QEntry.Schedule_unlocked: (0 != se.ID) might already be scheduled");
                }
#endif
                se.ID = ++this.LastScheduleID;
                se.TimeAdded = DateTime.Now;
                Scheduled.Add(se);
                return se;
            }


            public static int FindID<Entry>(long id, IList<Entry> entries) where Entry : IEntry
            {
                int count = entries.Count;
                for (int i = 0; i < count; i++)
                {
                    if (id == entries[i].GetID())
                    {
                        return i;
                    }
                }
                return -1;
            }

            public int FindQEntryByID(long id)
            {
                return FindID<QEntry>(id, Queued);
            }

            public int FindSEntryByID(long id)
            {
                return FindID<SEntry>(id, Scheduled);
            }


            public static QEntry QEntryFromArgs(string[] args, string user)
            {
                QEntry qe = new QEntry();
                qe.UserAdded = user;
                foreach (string arg in args)
                {
                    string name = arg;
                    string value = "";
                    {
                        int ieq = arg.IndexOf('=');
                        if (-1 != ieq)
                        {
                            name = arg.Substring(0, ieq);
                            value = arg.Substring(ieq + 1);
                        }
                    }
                    name = name.ToLower();
                    switch (name)
                    {
                        case "command":
                        case "cmd":
                            qe.Command = value;
                            break;
                        case "stdout":
                        case "out":
                        case "output":
                            qe.Out = value;
                            break;
                        case "stderr":
                        case "err":
                        case "error":
                            qe.Error = value;
                            break;
                        case "timeout":
                        case "exectimeout":
                            {
                                long ltimeout;
                                if (!long.TryParse(value, out ltimeout)
                                    || ltimeout <= 0)
                                {
                                    throw new FormatException("ExecTimeout must be a number greater than 0");
                                }
                                if (ltimeout > 2147483) // 2147483 is about 24 days.
                                {
                                    throw new FormatException("ExecTimeout too large");
                                }
                                qe.Timeout = (int)ltimeout;
                            }
                            break;
                        case "ontimeout":
                            qe.TimeoutCommand = value;
                            break;
                    }
                }
                if (string.IsNullOrEmpty(qe.Command))
                {
                    throw new InvalidOperationException("Expected command=<value>");
                }
#if DEBUG
                //System.Threading.Thread.Sleep(1000 * 8);
#endif
                if (null != qe.TimeoutCommand)
                {
                    if (0 == qe.TimeoutCommand.Length)
                    {
                        throw new InvalidOperationException("OnTimeout requires a value");
                    }
                    if (qe.Timeout <= 0)
                    {
                        throw new InvalidOperationException("ExecTimeout=<value> required with OnTimeout");
                    }
                    if (!IsQizmtExecCommand(qe.Command))
                    {
                        throw new InvalidOperationException("Command=<value> must be a Qizmt exec for OnTimeout");
                    }
                }
                if (!IsQizmtExecCommand(qe.Command))
                {
                    throw new InvalidOperationException("Command=<value> must be a Qizmt exec");
                }
                if (qe.Timeout > 0)
                {
                    if (null == qe.TimeoutCommand)
                    {
                        throw new InvalidOperationException("OnTimeout=<tcmd> required with ExecTimeout");
                    }
                }
                return qe;
            }


            protected internal QEntry Enqueue_unlocked(QEntry qe)
            {
#if DEBUG
                if (0 != qe.ID)
                {
                    throw new Exception("DEBUG:  QEntry.Enqueue_unlocked: (0 != qe.ID) might already be enqueued");
                }
#endif
                qe.ID = ++this.LastQueueID;
                qe.TimeAdded = DateTime.Now;
                Queued.Add(qe);
                return qe;
            }


            protected internal static ScheduleInfo Load_unlocked(string fp)
            {
                ScheduleInfo result;
                for (; ; )
                {
                    try
                    {
                        System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(ScheduleInfo));
                        using (System.IO.StreamReader sr = System.IO.File.OpenText(fp))
                        {
                            result = (ScheduleInfo)xs.Deserialize(sr);
                        }
                        result.FileExists = true;
                    }
                    catch (System.IO.FileNotFoundException)
                    {
                        result = new ScheduleInfo();
                    }
                    catch (System.IO.IOException)
                    {
                        System.Threading.Thread.Sleep(500);
                        continue;
                    }
                    break;
                }
                if (null == result.Queued)
                {
                    result.Queued = new List<QEntry>();
                }
                if (null == result.Scheduled)
                {
                    result.Scheduled = new List<SEntry>();
                }
                return result;
            }

            protected internal static ScheduleInfo Load_unlocked()
            {
                return Load_unlocked(APPDIR + @"\" + SCHEDULEXMLNAME);
            }

            public static ScheduleInfo Load()
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
                System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(ScheduleInfo));
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

                if(!string.IsNullOrEmpty(BackupDirectory))
                {
                    string bfn = SCHEDULEXMLNAME + ".backup";
                    string bfp = BackupDirectory + @"\" + bfn;
                    string btempfp = bfp + "$";
                    System.IO.File.Copy(fp, btempfp, true);
                    try
                    {
                        System.IO.File.Delete(bfp);
                    }
                    catch
                    {
                    }
                    System.IO.File.Move(btempfp, bfp);
                }

                this.FileExists = true;
            }

            protected internal void Save_unlocked()
            {
                Save_unlocked(APPDIR + @"\" + SCHEDULEXMLNAME);
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


    }
}
