using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects5;

namespace MySpace.DataMining.AELight
{
    class DebugArrayComboList : ArrayComboList
    {
        public DebugArrayComboList(string objectname, int keylength)
            : base("Debug_" + objectname, keylength)
        {
            CompilerDebugMode = true;
            CompilerOptions = "/platform:x86";
            AddUnsafe();
        }


        public string MapSource = null;
        public string MapClassName = null;

        public override void DoMapFullSource(IList<string> inputdfsnodes, string code, string classname)
        {
            MapSource = code;
            MapClassName = classname;
        }


        public string ReduceSource = null;
        public string ReduceClassName = null;

        public override ArrayComboListEnumerator[] GetEnumeratorsWithFullSource(string code, string classname)
        {
            ReduceSource = code;
            ReduceClassName = classname;

            {
                ArrayComboListEnumerator[] _dummyresult = new ArrayComboListEnumerator[0];
                return _dummyresult;
            }
        }


        public override void BeforeLoadFullSource(string code, string classname)
        {
            throw new DebugJobException("BeforeLoadFullSource: Cannot debug MapReduce DirectSlaveLoad");
        }

        public override void Open()
        {
            throw new NotSupportedException();
        }

    }


    public class DebugRemote : Remote
    {
        public DebugRemote(string objectname)
            : base("Debug_" + objectname)
        {
            CompilerDebugMode = true;
            CompilerOptions = "/platform:x86";
            AddUnsafe();
        }


        public string RemoteSource = null;
        public string RemoteClassName = null;

        public override void RemoteExecFullSource(string code, string classname)
        {
            RemoteSource = code;
            RemoteClassName = classname;
        }


        protected override int GetNumberOfRemoteOutputFilesCreated(IList<long> appendsizes)
        {
            if (null != appendsizes)
            {
                throw new NotImplementedException("DebugRemote.GetNumberOfRemoteOutputFilesCreated must specify null appendsizes");
            }
            return 1; // ...
        }

        protected override void GetDGlobals()
        {
            //
        }
    }


    public class DebugJobException : Exception
    {
        public DebugJobException(string msg)
            : base(msg)
        {
        }

    }

}
