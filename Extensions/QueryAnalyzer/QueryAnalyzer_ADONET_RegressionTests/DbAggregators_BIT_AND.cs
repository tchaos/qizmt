using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using MySpace.DataMining.DistributedObjects;

namespace QueryAnalyzer_ADONET_RegressionTests
{
    public partial class Program
    {
        private static void bitop(byte[] x, byte[] y, int whatop)
        {
            for (int ib = 0; ib < x.Length; ib++)
            {
                if (whatop == 1)
                {
                    x[ib] = (byte)(x[ib] & y[ib]);
                }
                else if (whatop == 2)
                {
                    x[ib] = (byte)(x[ib] | y[ib]);
                }
                else
                {
                    x[ib] = (byte)(x[ib] ^ y[ib]);
                }
            }
        }

        private static byte[] bitop(double[] values, int whatop)
        {
            byte[] buf = new byte[9];
            byte[] result = new byte[9];
            for (int i = 0; i < values.Length; i++)
            {
                if (i == 0)
                {
                    Entry.DoubleToBytes(values[i], result, 0);
                    continue;
                }
                Entry.DoubleToBytes(values[i], buf, 0);
                bitop(result, buf, whatop);
            }
            return result;
        }

        private static byte[] bitop(int[] values, int whatop)
        {
            byte[] buf = new byte[4];
            byte[] result = new byte[4];
            for (int i = 0; i < values.Length; i++)
            {
                int x = 0;
                if (i == 0)
                {
                    x = Entry.ToInt32((UInt32)values[i]);
                    Entry.ToBytes(x, result, 0);
                    continue;
                }
                x = Entry.ToInt32((UInt32)values[i]);
                Entry.ToBytes(x, buf, 0);
                bitop(result, buf, whatop);
            }
            return result;
        }

        private static byte[] bitop(long[] values, int whatop)
        {
            byte[] buf = new byte[8];
            byte[] result = new byte[8];
            for (int i = 0; i < values.Length; i++)
            {
                long x = 0;
                if (i == 0)
                {
                    x = Entry.ToInt64((UInt64)values[i]);
                    Entry.LongToBytes(x, result, 0);
                    continue;
                }
                x = Entry.ToInt64((UInt64)values[i]);
                Entry.LongToBytes(x, buf, 0);
                bitop(result, buf, whatop);
            }
            return result;
        }

        private static byte[] bitop(mstring[] values, int whatop)
        {
            byte[] buf = null;
            byte[] result = null;
            for (int i = 0; i < values.Length; i++)
            {
                if (i == 0)
                {
                    ByteSlice bs = values[i].ToByteSliceUTF16();
                    result = bs.ToBytes();
                    continue;
                }

                {
                    ByteSlice bs = values[i].ToByteSliceUTF16();
                    buf = bs.ToBytes();
                }
                bitop(result, buf, whatop);
            }
            return result;
        }

        private static bool compareBytes(byte[] b1, ByteSlice b2)
        {
            if (b1.Length != b2.Length - 1)  //include null front byte
            {
                return false;
            }

            for (int i = 0; i < b1.Length; i++)
            {
                if (b1[i] != b2[i + 1])
                {
                    return false;
                }
            }
            return true;
        }

        public static void DbAggregators_BIT_AND(string tablename)
        {            
            {
                System.Data.Common.DbProviderFactory fact = DbProviderFactories.GetFactory("DSpace_DataProvider");
                DbConnection conn = fact.CreateConnection();
                try
                {
                    conn.ConnectionString = "Data Source = localhost";
                    conn.Open();

                    {
                        Console.WriteLine("Testing BIT_AND(long)...");
                        DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select id, BIT_AND(costl) from " + tablename + " group by id";

                        Dictionary<int, byte[]> expected = new Dictionary<int, byte[]>();
                        List<long> values = new List<long>();
                        values.Add(100);
                        values.Add(200);
                        values.Add(400);
                        expected[10] = bitop(values.ToArray(), 1);

                        values.Clear();
                        values.Add(705);
                        values.Add(900);
                        expected[20] = bitop(values.ToArray(), 1);

                        values.Clear();
                        values.Add(705);
                        values.Add(1000);
                        values.Add(100);   
                        expected[30] = bitop(values.ToArray(), 1);
                        Dictionary<int, int> resultCount = new Dictionary<int, int>();

                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            long op = reader.GetInt64(1);
                            if (!expected.ContainsKey(id))
                            {
                                throw new Exception("id returned from bit_and group by is invalid.");
                            }
                            long expop = Entry.ToInt64((UInt64)Entry.BytesToLong(expected[id], 0));

                            if (op != expop)
                            {
                                throw new Exception("Expected bit_and: " + expop.ToString() + ", but got " + op.ToString() + " instead.");
                            }
                            if (!resultCount.ContainsKey(id))
                            {
                                resultCount[id] = 0;
                            }
                            resultCount[id]++;
                        }
                        reader.Close();

                        int count = 0;
                        foreach (int c in resultCount.Values)
                        {
                            count += c;
                        }

                        if (count != 3)
                        {
                            throw new Exception("Expected row count: 3, but got " + count.ToString() + " instead.");
                        }
                        Console.WriteLine("Expected results received.");
                    }
                }
                finally
                {
                    conn.Close();
                }
            }           
        }
    }
}