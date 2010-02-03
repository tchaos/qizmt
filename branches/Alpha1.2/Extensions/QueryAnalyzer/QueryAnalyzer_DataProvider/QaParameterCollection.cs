using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_DataProvider
{
    public class QaParameterCollection: DbParameterCollection
    {
        private List<QaParameter> parameters = new List<QaParameter>();

        public override int IndexOf(object value)
        {
            QaParameter param = (QaParameter)value;
            for (int i = 0; i < parameters.Count; i++)
            {
                if (parameters[i] == value)
                {
                    return i;
                }
            }
            return -1;
        }

        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < parameters.Count; i++)
            {
                if (string.Compare(parameters[i].ParameterName, parameterName, true) == 0)
                {
                    return i;
                }
            }
            return -1;
        }

        public override int Add(object value)
        {
            QaParameter param = (QaParameter)value;
            CheckSetParent(param);
            parameters.Add(param);
            return parameters.Count - 1;
        }

        public override void RemoveAt(int index)
        {           
            if (!CheckRange(index))
            {
                throw new ArgumentOutOfRangeException("index");
            }
            QaParameter param = parameters[index];
            param.ResetParent();           
            parameters.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index == -1)
            {
                throw new ArgumentException("Parameter is not be found.");
            }
            RemoveAt(index);
        }        

        public override void Remove(object value)
        {
            int index = IndexOf(value);
            if (index == -1)
            {
                throw new ArgumentException("Parameter is not be found.");
            }                   
            RemoveAt(index);
        }

        protected override DbParameter GetParameter(int index)
        {
            if (!CheckRange(index))
            {
                throw new ArgumentOutOfRangeException("index");
            }
            return parameters[index];
        }        

        protected override DbParameter GetParameter(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index == -1)
            {
                throw new ArgumentException("Parameter is not found.");
            }
            return parameters[index];
        }

        public override bool Contains(object value)
        {
            return (-1 != IndexOf(value));
        }

        public override bool Contains(string value)
        {
            return (-1 != IndexOf(value));
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            if (!CheckRange(index))
            {
                throw new IndexOutOfRangeException("index");
            }

            QaParameter newparam = (QaParameter)value;
            CheckSetParent(newparam);

            QaParameter oldparam = parameters[index];
            oldparam.ResetParent();

            parameters[index] = newparam;            
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            int index = IndexOf(parameterName);
            SetParameter(index, value);
        }

        public override void Insert(int index, object value)
        {
            QaParameter param = (QaParameter)value;
            CheckSetParent(param);

            parameters.Insert(index, param);
        }

        public override void AddRange(Array values)
        {
            if (values == null)
            {
                throw new ArgumentException("values cannot be null.");
            }

            QaParameter[] newparams = (QaParameter[])values;
            for (int i = 0; i < newparams.Length; i++)
            {
                CheckSetParent(newparams[i]);
            }

            parameters.AddRange(newparams);
        }

        public override void CopyTo(Array array, int index)
        {
            parameters.CopyTo((QaParameter[])array, index);
        }

        public override System.Collections.IEnumerator GetEnumerator()
        {
            return parameters.GetEnumerator();
        }

        public override void Clear()
        {
            for (int i = 0; i < parameters.Count; i++)
            {
                parameters[i].ResetParent();
            }
            parameters.Clear();
        }

        public override int Count
        {
            get 
            {
                return parameters.Count; 
            }
        }

        public override bool IsReadOnly
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsFixedSize
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsSynchronized
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override object SyncRoot
        {
            get 
            { 
                throw new NotImplementedException(); 
            }
        }

        private bool CheckRange(int index)
        {
            return (index >= 0 && index < parameters.Count);
        }

        private void CheckSetParent(QaParameter param)
        {
            if (param.parent != null)
            {
                if (param.parent == this)
                {
                    throw new Exception("This parameter has already been added to this collection.");
                }
                else
                {
                    throw new Exception("This parameter has already been added to another collection.");
                }
            }
            param.parent = this;
        }
    }
}
