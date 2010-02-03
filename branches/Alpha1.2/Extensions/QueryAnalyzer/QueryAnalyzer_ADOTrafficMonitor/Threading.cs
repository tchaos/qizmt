using System;
using System.Collections.Generic;
using System.Text;

namespace QueryAnalyzer_ADOTrafficMonitor
{

    public class ThreadTools<TaskItem, ThreadItem>
    {

        internal static void _Parallel(Action<TaskItem> action1, Action<TaskItem, ThreadItem> action2, IList<TaskItem> tasks, IList<ThreadItem> threads)
        {
            int ntasks = tasks.Count;
            int nthreads = threads.Count;
            if (nthreads < 2 || ntasks < 2) // Will only be 1 thread here.
            {
                if (null != action2)
                {
                    for (int i = 0; i < ntasks; i++)
                    {
                        action2(tasks[i], threads[0]);
                    }
                }
                else
                {
                    for (int i = 0; i < ntasks; i++)
                    {
                        action1(tasks[i]);
                    }
                }
            }
            else // More than one thread!
            {
                int tpt = ntasks / nthreads; // Tasks per thread.
                if (0 != (ntasks % nthreads))
                {
                    tpt++;
                }
                List<PTO> ptos = new List<PTO>(nthreads);
                int offset = 0;
                for (int it = 0; offset < ntasks; it++)
                {
                    PTO pto = new PTO();
                    pto.thread = new System.Threading.Thread(new System.Threading.ThreadStart(pto.threadproc));
                    pto.alltasks = tasks;
                    pto.start = offset;
                    offset += tpt;
                    if (offset > ntasks)
                    {
                        offset = ntasks;
                    }
                    pto.stop = offset;
                    pto.action1 = action1;
                    pto.action2 = action2;
                    pto.threaditem = threads[it];
                    ptos.Add(pto);
                    pto.thread.Start();
                }
                for (int i = 0; i < ptos.Count; i++)
                {
                    ptos[i].thread.Join();
                    if (ptos[i].exception != null)
                    {
                        throw ptos[i].exception;
                    }
                }
            }
        }

        /*public static void Parallel(Action<TaskItem> action, IList<TaskItem> tasks, IList<ThreadItem> threads)
        {
            _Parallel(action, null, tasks, threads);
        }*/

        public static void Parallel(Action<TaskItem, ThreadItem> action, IList<TaskItem> tasks, IList<ThreadItem> threads)
        {
            _Parallel(null, action, tasks, threads);
        }


        class PTO
        {
            internal System.Threading.Thread thread;
            internal IList<TaskItem> alltasks;
            internal int start;
            internal int stop;
            internal Action<TaskItem> action1;
            internal Action<TaskItem, ThreadItem> action2;
            internal ThreadItem threaditem;
            internal Exception exception = null;

            internal void threadproc()
            {
                try
                {
                    if (null != action2)
                    {
                        for (int i = start; i < stop; i++)
                        {
                            action2(alltasks[i], threaditem);
                        }
                    }
                    else
                    {
                        for (int i = start; i < stop; i++)
                        {
                            action1(alltasks[i]);
                        }
                    }
                }
                catch (Exception e)
                {
                    exception = e;
#if DEBUG
                    if (!System.Diagnostics.Debugger.IsAttached)
                    {
                        System.Diagnostics.Debugger.Launch();
                    }
#endif
                }
            }

        }

    }


    public class ThreadTools<TaskItem>
    {
        public static void Parallel(Action<TaskItem> action, IList<TaskItem> tasks, int nthreads)
        {
            ThreadTools<TaskItem, int>._Parallel(action, null, tasks, new ListCounter(nthreads));
        }

        public static void Parallel(Action<TaskItem> action, IList<TaskItem> tasks)
        {
            ThreadTools<TaskItem, int>._Parallel(action, null, tasks, new ListCounter(ThreadTools.NumberOfProcessors));
        }
    }


    public class ThreadTools
    {

        public static void Parallel(Action<object> action, IList<object> tasks, int nthreads)
        {
            ThreadTools<object>.Parallel(action, tasks, nthreads);
        }

        public static void Parallel(Action<object> action, IList<object> tasks)
        {
            ThreadTools<object>.Parallel(action, tasks);
        }


        public static void Parallel(Action<int> action, int count, int nthreads)
        {
            ThreadTools<int>.Parallel(action, new ListCounter(count), nthreads);
        }

        public static void Parallel(Action<int> action, int count)
        {
            ThreadTools<int>.Parallel(action, new ListCounter(count));
        }


        public static int NumberOfProcessors
        {
            get
            {
                if (_ncpus < 1)
                {
                    try
                    {
                        _ncpus = int.Parse(Environment.GetEnvironmentVariable("NUMBER_OF_PROCESSORS"));
                    }
                    catch
                    {
                        _ncpus = 1;
                    }
                }
                return _ncpus;
            }
        }
        static int _ncpus = 0;

    }


    #region ListCounter
    class ListCounter : IList<int>
    {
        int _count;
        public ListCounter(int count)
        {
            this._count = count;
        }


        public int IndexOf(int item)
        {
            if (!Contains(item))
            {
                return -1;
            }
            return item;
        }

        public void Insert(int index, int item)
        {
            throw new NotSupportedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotSupportedException();
        }

        public int this[int index]
        {
            get
            {
#if DEBUG
                if (index < 0 || index >= _count)
                {
                    throw new ArgumentException("ListCounter: Out of bounds");
                }
#endif
                return index;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public void Add(int item)
        {
            throw new NotSupportedException();
        }

        public void Clear()
        {
            throw new NotSupportedException();
        }

        public bool Contains(int item)
        {
            return item >= 0 && item < Count;
        }

        public void CopyTo(int[] array, int arrayIndex)
        {
            for (int i = 0; i < _count; i++)
            {
                array[arrayIndex + i] = i;
            }
        }

        public int Count
        {
            get { return _count; }
        }

        public bool IsReadOnly
        {
            get { return true; }
        }

        public bool Remove(int item)
        {
            throw new NotSupportedException();
        }

        public IEnumerator<int> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

    }
    #endregion

}
