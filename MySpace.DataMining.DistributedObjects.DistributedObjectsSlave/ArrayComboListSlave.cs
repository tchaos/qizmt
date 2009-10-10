/**************************************************************************************
 *  MySpace’s Mapreduce Framework is a mapreduce framework for distributed computing  *
 *  and developing distributed computing applications on large clusters of servers.   *
 *                                                                                    *
 *  Copyright (C) 2008  MySpace Inc. <http://qizmt.myspace.com/>                      *
 *                                                                                    *
 *  This program is free software: you can redistribute it and/or modify              *
 *  it under the terms of the GNU General Public License as published by              *
 *  the Free Software Foundation, either version 3 of the License, or                 *
 *  (at your option) any later version.                                               *
 *                                                                                    *
 *  This program is distributed in the hope that it will be useful,                   *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of                    *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                     *
 *  GNU General Public License for more details.                                      *
 *                                                                                    *
 *  You should have received a copy of the GNU General Public License                 *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.             *
***************************************************************************************/

// If extra logging is enabled, enable timings.
//#define ENABLE_TIMING

//#define COOK_TEST_ZMapStreamToZBlocks


using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

using MySpace.DataMining.DistributedObjects;


namespace MySpace.DataMining.DistributedObjects5
{ 
    public class ArrayComboListPart : DistObjectBase
    {
        public class ZBlock
        {
            const long ROGUE_ZBLOCK_SIZE = 8589934592 + 1; // > 8 GB
            const long ZVALUEBLOCK_MAX_BYTES = Int32.MaxValue;
            public const int ZFILE_MAX_BYTES = 1342177280; // 1.25 GB. Actual limit when writing to zkey/zvalue files.

            ArrayComboListPart parent;
            int zblockID; // ZBlock ID (0-based n)

            int keyblockbuflen, valueblockbuflen;
            System.IO.FileStream fzkeyblock;
            System.IO.FileStream fzvalueblock;
            string fzkeyblockfilename;
            string fzvalueblockfilename;
            //int valueaddedbytes = 0; // Does not include length-bytes (the extra 4 for each length).
            long zvalueblocksize = 0;
            long zkeyblocksize = 0;
            int numadded = 0;
            bool addmode = true;
            bool isrogue = false;


            public void Close()
            {
                _justclose();

                if (null != fzkeyblockfilename)
                {
                    try
                    {
                        System.IO.File.Delete(fzkeyblockfilename);
                        fzkeyblockfilename = null;
                    }
                    catch (Exception e)
                    {
                    }
                }
                if (null != fzvalueblockfilename)
                {
                    try
                    {
                        System.IO.File.Delete(fzvalueblockfilename);
                        fzvalueblockfilename = null;
                    }
                    catch (Exception e)
                    {
                    }
                }
            }


            private void _justclose()
            {
                if (null != fzkeyblock)
                {
                    fzkeyblock.Close();
                    fzkeyblock = null;
                }
                if (null != fzvalueblock)
                {
                    fzvalueblock.Close();
                    fzvalueblock = null;
                }
            }


            public void LeaveAddMode(int readkeybuflen, int readvaluebuflen)
            {
                addmode = false;
                keyblockbuflen = readkeybuflen;
                valueblockbuflen = readvaluebuflen;
                _justclose();
            }


            internal static long jid
            {
                get
                {
                    return DistributedObjectsSlave.jid;
                }
            }

            internal static string sjid
            {
                get
                {
                    return DistributedObjectsSlave.sjid;
                }
            }


            internal static string _spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

            internal static string CreateZBlockFileName(int zblockID, string otherinfo)
            {
                return "zblock_" + _spid + "_" + zblockID.ToString() + "_" + otherinfo + ".j" + sjid + ".zb";
            }

            internal static string CreateZBallFileName(int zblockID, string cachename, string otherinfo)
            {
                if(string.IsNullOrEmpty(cachename))
                {
                    throw new Exception("ZBlock.CreateZBallFileName: no cache name");
                }
                return "zsball_" + cachename + "_" + zblockID.ToString() + "_" + otherinfo + ".j" + sjid + ".zsb";
            }


            public static void CleanZBlockFiles(params string[] otherinfos)
            {
                // Clean any potential old zblock files...
                bool found = true;
                for (int i = 0; found; i++)
                {
                    found = false;
                    foreach (string otherinfo in otherinfos)
                    {
                        string fn = ZBlock.CreateZBlockFileName(i, otherinfo);
                        if (System.IO.File.Exists(fn))
                        {
                            System.IO.File.Delete(fn);
                            found = true;
                        }
                    }
                }
            }


            internal void ensurefzblock(bool sorting, bool zballing)
            {
                //----------------------------COOKING--------------------------------
                int cooking_cooksremain = parent.CookRetries;
                for (; ; )
                {
                    try
                    {
                //----------------------------COOKING--------------------------------
                        {
                            System.IO.FileAccess access = System.IO.FileAccess.Read;
                            System.IO.FileMode mode = System.IO.FileMode.Open;
                            if (addmode || sorting || zballing)
                            {
                                access = System.IO.FileAccess.ReadWrite;
                                mode = System.IO.FileMode.Create;
                            }

                            if (null == fzkeyblock)
                            {
                                fzkeyblock = new System.IO.FileStream(fzkeyblockfilename, mode, access, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                            }
                        }

                        if (null == fzvalueblock)
                        {
                            System.IO.FileAccess access = System.IO.FileAccess.Read;
                            System.IO.FileMode mode = System.IO.FileMode.Open;
                            if (addmode)
                            {
                                access = System.IO.FileAccess.ReadWrite;
                                mode = System.IO.FileMode.Create;
                            }
                            if (zballing)
                            {
                                access = System.IO.FileAccess.ReadWrite;
                                mode = System.IO.FileMode.Open;
                            }
                            fzvalueblock = new System.IO.FileStream(fzvalueblockfilename, mode, access, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                        }
                //----------------------------COOKING--------------------------------
                    }
                    catch (Exception e)
                    {
                        if (cooking_cooksremain-- <= 0)
                        {
                            throw new System.IO.IOException("cooked too many times", e);
                        }
                        System.Threading.Thread.Sleep(parent.CookTimeout);
                        continue; // !
                    }
                    break;
                }
                //----------------------------COOKING--------------------------------
            }

            internal void ensurefzblock(bool sorting)
            {
                ensurefzblock(sorting, false);
            }


            internal ZBlock(ArrayComboListPart parent, int zblockID, int addkeybuflen, int addvaluebuflen)
            {
                this.parent = parent;
                this.zblockID = zblockID;

                this.keyblockbuflen = addkeybuflen;
                this.valueblockbuflen = addvaluebuflen;

                fzkeyblockfilename = CreateZBlockFileName(zblockID, "key_unsorted");
                fzvalueblockfilename = CreateZBlockFileName(zblockID, "value");
                ensurefzblock(false);
            }


            public void Flush()
            {
                if (null != fzkeyblock)
                {
                    fzkeyblock.Flush();
                }
                if (null != fzvalueblock)
                {
                    fzvalueblock.Flush();
                }
            }

            internal long getzkeyblocksize()
            {
                return zkeyblocksize;
            }

            public bool Add(byte[] keybuf, int keyoffset, byte[] valuebuf, int valueoffset, int valuelength)
            {
                long x = (long)(parent.keylen + 4) + (long)valuelength;
                if (x > ZFILE_MAX_BYTES)
                {
                    throw new Exception("Key+Value too big; length=" + x.ToString());
                    //parent.SetError("ZBlock.Add: Key+Value too big; length=" + x.ToString());
                    //return false;
                }

                if (zvalueblocksize + valuelength > ZFILE_MAX_BYTES)
                {
                    if (parent.jobfailover)
                    {
                        XLog.failoverlog("x disk ZBlock too big (values)"); // Don't recover.
                    }
                    throw new Exception("Insufficient resources for this job on cluster (ZBlock value file size > ZFILE_MAX_BYTES) (consider increasing sub process count)");
                    //return false;
                }
                if (zkeyblocksize + parent.keylen + 4 > ZFILE_MAX_BYTES)
                {
                    if (parent.jobfailover)
                    {
                        XLog.failoverlog("x disk ZBlock too big (keys)"); // Don't recover.
                    }
                    throw new Exception("Insufficient resources for this job on cluster (ZBlock key file size > ZFILE_MAX_BYTES) (consider increasing sub process count)");
                    //return false;
                }

                fzkeyblock.Write(keybuf, keyoffset, parent.keylen);
                Entry.ToBytes((int)zvalueblocksize, parent._smallbuf, 0);
                fzkeyblock.Write(parent._smallbuf, 0, 4);

                Entry.ToBytes(valuelength, parent._smallbuf, 0);
                fzvalueblock.Write(parent._smallbuf, 0, 4);
                fzvalueblock.Write(valuebuf, valueoffset, valuelength);

                //valueaddedbytes += valuelength;
                numadded++;

                zvalueblocksize += 4 + valuelength;
#if DEBUG
                if (zvalueblocksize != fzvalueblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zvalueblocksize mismatch");
                }
#endif

                zkeyblocksize += parent.keylen + 4;
#if DEBUG
                if (zkeyblocksize != fzkeyblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zkeyblocksize mismatch");
                }
#endif

                return true;
            }

            public bool Add(IList<byte> keybuf, int keyoffset, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                long x = (long)(parent.keylen + 4) + (long)valuelength;
                if (x > ZFILE_MAX_BYTES)
                {
                    throw new Exception("Key+Value too big; length=" + x.ToString());
                    //parent.SetError("ZBlock.Add: Key+Value too big; length=" + x.ToString());
                    //return false;
                }

                if (zvalueblocksize + valuelength > ZFILE_MAX_BYTES)
                {
                    if (parent.jobfailover)
                    {
                        XLog.failoverlog("x disk ZBlock too big (values)"); // Don't recover.
                    }
                    throw new Exception("Insufficient resources for this job on cluster (ZBlock value file size > ZFILE_MAX_BYTES) (consider increasing sub process count)");
                    //return false;
                }
                if (zkeyblocksize + parent.keylen + 4 > ZFILE_MAX_BYTES)
                {
                    if (parent.jobfailover)
                    {
                        XLog.failoverlog("x disk ZBlock too big (keys)"); // Don't recover.
                    }
                    throw new Exception("Insufficient resources for this job on cluster (ZBlock key file size > ZFILE_MAX_BYTES) (consider increasing sub process count)");
                    //return false;
                }

                //fzkeyblock.Write(keybuf, keyoffset, parent.keylen);
                parent.StreamWrite(fzkeyblock, keybuf, keyoffset, parent.keylen);
                Entry.ToBytes((int)zvalueblocksize, parent._smallbuf, 0);
                fzkeyblock.Write(parent._smallbuf, 0, 4);

                Entry.ToBytes(valuelength, parent._smallbuf, 0);
                fzvalueblock.Write(parent._smallbuf, 0, 4);
                //fzvalueblock.Write(valuebuf, valueoffset, valuelength);
                parent.StreamWrite(fzvalueblock, valuebuf, valueoffset, valuelength);

                //valueaddedbytes += valuelength;
                numadded++;

                zvalueblocksize += 4 + valuelength;
#if DEBUG
                if (zvalueblocksize != fzvalueblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zvalueblocksize mismatch");
                }
#endif

                zkeyblocksize += parent.keylen + 4;
#if DEBUG
                if (zkeyblocksize != fzkeyblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zkeyblocksize mismatch");
                }
#endif

                return true;
            }


            void makerogue()
            {
                isrogue = true;

                // ... rename zblock files; don't let it auto delete but have batch file delete it.
            }


            void checkrogue()
            {
                if (!isrogue)
                {
                    //long x = fzkeyblock.Length + fzvalueblock.Length;
                    long x = zkeyblocksize + zvalueblocksize;
                    if (x >= ROGUE_ZBLOCK_SIZE)
                    {
                        makerogue();
                    }
                }
            }


            // buf must be at least 8 bytes.
            // Truncates zblocks at ROGUE_ZBLOCK_SIZE.
            //--A-------------ADDCOOK------------------
            public void CopyInto(List<byte[]> net, List<Entry> entries, ref byte[] ebuf, ref byte[] evaluesbuf)
            {
                //----------------------------COOKING--------------------------------
                int cooking_cooksremain = parent.CookRetries;
                //----------------------------COOKING--------------------------------
                for (; ; )
                {
                    try
                    {
                        ensurefzblock(false);
                        checkrogue();

                        net.Clear();
                        entries.Clear();

                        long kflen = fzkeyblock.Length;
                        int keycount = (int)(kflen / (parent.keylen + 4));

                        long vflen = fzvalueblock.Length;

                        if (vflen > ZVALUEBLOCK_MAX_BYTES)
                        {
                            vflen = ZVALUEBLOCK_MAX_BYTES;
                        }

                        //try
                        {
                            if (evaluesbuf.LongLength < vflen)
                            {
                                evaluesbuf = null;
                                long lnew = Entry.Round2PowerLong(vflen);
                                evaluesbuf = new byte[lnew];
                            }
                            byte[] valuesbuf = evaluesbuf;

                            if (ebuf.LongLength < kflen + vflen)
                            {
                                try
                                {
                                    ebuf = null;
                                    long lnew = Entry.Round2PowerLong(kflen + vflen);
                                    if (lnew > Int32.MaxValue)
                                    {
                                        lnew = Int32.MaxValue;
                                    }
                                    ebuf = new byte[lnew];
                                }
                                catch (Exception e)
                                {
                                    unchecked
                                    {
                                        string better_error = e.ToString();
                                        better_error += System.Environment.NewLine;
                                        better_error += "----------------------------------------------------" + System.Environment.NewLine;
                                        better_error += "zblock combined length: " + (kflen + vflen).ToString() + System.Environment.NewLine;
                                        better_error += "ebuf length: " + ebuf.LongLength.ToString() + System.Environment.NewLine;
                                        better_error += "----------------------------------------------------" + System.Environment.NewLine;
                                        throw new Exception(better_error);
                                    }
                                }
                            }
                            byte[] buf = ebuf;

                            net.Add(buf);

                            int newcap = keycount;
                            /*if (isfirstlist)
                            {
                                isfirstlist = false;
                                newcap *= 2;
                            }*/
                            if (newcap > entries.Capacity)
                            {
                                entries.Capacity = newcap;
                            }

                            fzvalueblock.Seek(0, System.IO.SeekOrigin.Begin);
                            fzvalueblock.Read(valuesbuf, 0, (int)vflen); // NOTE: vflen capped to ZVALUEBLOCK_MAX_BYTES for now!

                            // Read from existing unsorted zblock file into buffer.
                            fzkeyblock.Seek(0, System.IO.SeekOrigin.Begin);
                            long sofar = 0;
                            Entry ent;
                            ent.NetEntryOffset = 0;
                            ent.NetIndex = 0;
                            int offset = 0;
                            unchecked
                            {
                                for (int i = 0; i != keycount; i++)
                                {
                                    int morespace = offset + parent.keylen + 4;
                                    if (morespace < offset || morespace > buf.Length)
                                    {
                                        offset = 0;
                                        buf = new byte[Int32.MaxValue]; // Note: could cache; could calculate size needed.
                                        net.Add(buf);
                                        ent.NetEntryOffset++;
                                    }

                                    ent.NetEntryOffset = offset;
                                    fzkeyblock.Read(buf, offset, parent.keylen);
                                    offset += parent.keylen;

                                    fzkeyblock.Read(parent._smallbuf, 0, 4);
                                    int valueoffset = Entry.BytesToInt(parent._smallbuf);

                                    Buffer.BlockCopy(valuesbuf, valueoffset, buf, offset, 4);
                                    int valuelen = Entry.BytesToInt(buf, offset);
                                    offset += 4;

                                    // Handle truncated valuesbuf...
                                    if ((long)valueoffset + (long)valuelen > valuesbuf.LongLength)
                                    {
                                        offset -= parent.keylen + 4; // Undo.
                                        continue;
                                    }

                                    morespace = offset + valuelen;
                                    if (morespace < offset || morespace > buf.Length)
                                    {
                                        if (sofar + Int32.MaxValue >= ROGUE_ZBLOCK_SIZE)
                                        {
                                            makerogue(); // Should already be set, but just to be sure.
                                            break;
                                        }
                                        else
                                        {
                                            offset = 0;
                                            buf = new byte[Int32.MaxValue]; // Note: could cache; could calculate size needed.
                                            net.Add(buf);
                                            ent.NetEntryOffset++;
                                            i--; // So next iteration does this index again.
                                            continue;
                                        }
                                    }

                                    Buffer.BlockCopy(valuesbuf, valueoffset + 4, buf, offset, valuelen);
                                    offset += valuelen;

                                    sofar += parent.keylen + 4 + valuelen;
                                    if (sofar >= ROGUE_ZBLOCK_SIZE)
                                    {
                                        makerogue(); // Should already be set, but just to be sure.
                                        break;
                                    }

                                    entries.Add(ent);
                                }
                            }
                        }
                        /*catch (Exception e)
                        {
                            net.Clear();
                            entries.Clear();
                            parent.SetError("Enumeration failure; zblock skipped: " + e.ToString());
                        }*/

                        _justclose();
                    }
                    catch(Exception e)
                    {
                        //----------------------------COOKING--------------------------------
                        if (cooking_cooksremain-- <= 0)
                        {
                            throw new System.IO.IOException("cooked too many times", e);
                        }
                        try
                        {
                            _justclose();
                        }
                        catch
                        {
                        }
                        System.Threading.Thread.Sleep(parent.CookTimeout);
                        continue;
                        //----------------------------COOKING--------------------------------
                    }
                    break;
                }
            }


            public struct KeyBlockEntry
            {
                public ByteSlice key;
                public int valueoffset;
            }


            //--B-------------ADDCOOK------------------
            protected void XCopyInto(string keyblockfilename, List<KeyBlockEntry> kentries, ref byte[] ebuf)
            {
                //----------------------------COOKING--------------------------------
                int cooking_cooksremain = parent.CookRetries;
                bool cooking_inIO = false;
                long cooking_seekpos = 0;
                //----------------------------COOKING--------------------------------
                for (; ; )
                {
                    try
                    {
                        cooking_inIO = true;
                        _justclose();
                        using (System.IO.Stream stm = new System.IO.FileStream(keyblockfilename, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                        {
                            if (0 != cooking_seekpos)
                            {
                                stm.Seek(cooking_cooksremain, System.IO.SeekOrigin.Begin);
                            }
                            cooking_inIO = false;
                            
                            int kflen = (int)stm.Length;
                            int keycount = kflen / (parent.keylen + 4);

                            //try
                            {
                                if (ebuf.Length < parent.keylen * keycount + 4)
                                {
                                    ebuf = null;
                                    long lnew = Entry.Round2Power(parent.keylen * keycount + 4);
                                    if (lnew > Int32.MaxValue)
                                    {
                                        lnew = Int32.MaxValue;
                                    }
                                    ebuf = new byte[lnew];
                                }
                                byte[] buf = ebuf;

                                kentries.Clear();

                                int newcap = keycount;
                                /*if (isfirstklist)
                                {
                                    isfirstklist = false;
                                    newcap *= 2;
                                }*/
                                if (newcap > kentries.Capacity)
                                {
                                    kentries.Capacity = newcap;
                                }

                                // Read from existing unsorted zblock file into buffer.
                                cooking_inIO = true;
                                stm.Seek(0, System.IO.SeekOrigin.Begin);
                                cooking_inIO = false;

                                KeyBlockEntry kent;
                                //kent.key = ByteSlice.Create();
                                //kent.valueoffset = 0;
                                int offset = 0;
                                for (int i = 0; i != keycount; i++)
                                {
                                    cooking_inIO = true;
                                    int readsize = parent.keylen + 4;
                                    stm.Read(buf, offset, readsize);
                                    cooking_inIO = false;
                                    cooking_seekpos += readsize;

                                    kent.key = ByteSlice.Create(buf, offset, parent.keylen);
                                    offset += parent.keylen;
                                    kent.valueoffset = Entry.BytesToInt(buf, offset);
                                    kentries.Add(kent);
                                }
                            }
                            /*catch (Exception e)
                            {
                                kentries.Clear();
                                parent.SetError("CopyInfo failure; zblock skipped: " + e.ToString());
                            }*/
                            cooking_seekpos = 0;
                        }
                    }
                    catch(Exception e)
                    {
                        if (!cooking_inIO)
                        {
                            throw;
                        }
                        //----------------------------COOKING--------------------------------
                        if(cooking_cooksremain-- <= 0)
                        {
                            throw new System.IO.IOException("cooked too many times", e);
                        }
                        System.Threading.Thread.Sleep(parent.CookTimeout);
                        continue;
                        //----------------------------COOKING--------------------------------
                    }
                    break;
                }
            }


            public void CopyInto(List<KeyBlockEntry> kentries, ref byte[] ebuf)
            {
                XCopyInto(this.fzkeyblockfilename, kentries, ref ebuf);
            }


            // buf must be at least 8 bytes.
            public void Sort(List<KeyBlockEntry> kentries, ref byte[] ebuf)
            {
                ensurefzblock(false);

                try
                {
                    try
                    {
                        CopyInto(kentries, ref ebuf);
                    }
                    finally
                    {
                        // Delete old (unsorted) file; prepare new (sorted) one.
                        // Keep these together so that there's always one on file; so cleanup sees it and continues.
                        _justclose();
                        System.IO.File.Delete(fzkeyblockfilename);
                        fzkeyblockfilename = CreateZBlockFileName(zblockID, "key_sorted");
                        ensurefzblock(true);
                    }

                    // Sort the sortbuffer.
                    kentries.Sort(new System.Comparison<KeyBlockEntry>(parent._kcmp));

                    fzkeyblock.Seek(0, System.IO.SeekOrigin.Begin);
                    // From (sorted) sortbuffer write into new sorted zblock file.
                    foreach (KeyBlockEntry kent in kentries)
                    {
                        kent.key.CopyTo(parent._smallbuf);
                        fzkeyblock.Write(parent._smallbuf, 0, parent.keylen);
                        Entry.ToBytes(kent.valueoffset, parent._smallbuf, 0);
                        fzkeyblock.Write(parent._smallbuf, 0, 4);
                    }
                }
                finally
                {
                    _justclose();
                }
            }


            // Only valid right after sort!
            // keepzblocks==false is faster if enum/reduce isn't needed.
            internal void CommitZBall(string cachename, string sBlockID, bool keepzblock)
            {
                _justclose();

                string zkeyballname = CreateZBallFileName(zblockID, cachename, sBlockID.PadLeft(4, '0') + "key");
                string zvalueballname = CreateZBallFileName(zblockID, cachename, sBlockID.PadLeft(4, '0') + "value");

                if (keepzblock)
                {
                    System.IO.File.Copy(fzkeyblockfilename, zkeyballname, true); // overwrite=true
                    System.IO.File.Copy(fzvalueblockfilename, zvalueballname, true); // overwrite=true
                }
                else //if (!keepzblock)
                {
                    if(System.IO.File.Exists(zkeyballname))
                    {
                        System.IO.File.Delete(zkeyballname);
                    }
                    System.IO.File.Move(fzkeyblockfilename, zkeyballname);
                    if (System.IO.File.Exists(zvalueballname))
                    {
                        System.IO.File.Delete(zvalueballname);
                    }
                    System.IO.File.Move(fzvalueblockfilename, zvalueballname);

                    fzkeyblockfilename = null;
                    fzvalueblockfilename = null;
                }
            }


            // Only valid right after sort!
            internal bool IntegrateZBall(string cachename, string sBlockID, List<KeyBlockEntry> kentries1, ref byte[] ebuf1, List<KeyBlockEntry> kentries2, ref byte[] ebuf2, ref byte[] evaluesbuf)
            {
                string zkeyballname = CreateZBallFileName(zblockID, cachename, sBlockID.PadLeft(4, '0') + "key");
                string zvalueballname = CreateZBallFileName(zblockID, cachename, sBlockID.PadLeft(4, '0') + "value");

                if (!System.IO.File.Exists(zkeyballname))
                {
                    return true; // Valid case: no cache output to this file.
                }

                CopyInto(kentries1, ref ebuf1);
                _justclose();
                XCopyInto(zkeyballname, kentries2, ref ebuf2);

                // Delete the key block, it'll be rewritten.
                try
                {
                    System.IO.File.Delete(fzkeyblockfilename);
                }
                catch
                {
                }
                zkeyblocksize = 0;
                ensurefzblock(false, true); // zballing!
                // Seek to the end of the value block, it'll be appended-to.
                fzvalueblock.Seek(0, System.IO.SeekOrigin.End);

                long zvalueballsize;
                using (System.IO.FileStream fzvalueball = new System.IO.FileStream(zvalueballname, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.None, FILE_BUFFER_SIZE))
                {
                    zvalueballsize = fzvalueball.Length;
                    if (zvalueballsize > int.MaxValue)
                    {
                        throw new Exception("zvalueballsize too large");
                    }
                    if (zvalueballsize > evaluesbuf.Length)
                    {
                        evaluesbuf = new byte[Entry.Round2Power((int)zvalueballsize)];
                    }
#if DEBUG
                    for (int i = 0; i < evaluesbuf.Length; i++)
                    {
                        evaluesbuf[i] = 42;
                    }
#endif
                    ArrayComboListPart.StreamReadExact(fzvalueball, evaluesbuf, (int)zvalueballsize);
                }
                byte[] zballvaluebuf = evaluesbuf;

                //if (ebuf1.Length < parent.keylen + 4) // Workaround
                {
                    ebuf1 = new byte[Entry.Round2Power(parent.keylen + 4)];
                }
                byte[] buf1 = ebuf1;
                //if (ebuf2.Length < parent.keylen + 4) // Workaround
                {
                    ebuf2 = new byte[Entry.Round2Power(parent.keylen + 4)];
                }
                byte[] buf2 = ebuf2;

                int i1 = 0, i2 = 0;
                for (; ; )
                {
                    int diff;
                    if(i1 >= kentries1.Count)
                    {
                        if (i2 >= kentries2.Count)
                        {
                            break; // Done!
                        }
                        diff = 1;
                    }
                    else if (i2 >= kentries2.Count)
                    {
                        diff = -1;
                    }
                    else
                    {
                        diff = parent._kcmp(kentries1[i1], kentries2[i2]);
                    }
#if DEBUG
                    if (zkeyblocksize != fzkeyblock.Length)
                    {
                        throw new Exception("zkeyblocksize != fzkeyblock.Length");
                    }
#endif
                    zkeyblocksize += parent.keylen + 4;
                    if (diff < 0)
                    {
                        kentries1[i1].key.CopyTo(buf1);
                        Entry.ToBytes(kentries1[i1].valueoffset, buf1, parent.keylen);
                        fzkeyblock.Write(buf1, 0, parent.keylen + 4);
                        i1++;
                    }
                    else //if (diff >= 0)
                    {
                        kentries2[i2].key.CopyTo(buf2);
                        Entry.ToBytes((int)zvalueblocksize + kentries2[i2].valueoffset, buf2, parent.keylen);
                        fzkeyblock.Write(buf2, 0, parent.keylen + 4);
                        i2++;
                    }
                }
                fzvalueblock.Write(zballvaluebuf, 0, (int)zvalueballsize);
                zvalueblocksize += zvalueballsize;

                _justclose();

                return true;
            }

        }


        // Only valid right after sort!
        // keepzblocks==false is faster if enum/reduce isn't needed.
        void CommitZBalls(string cachename, string sBlockID, bool keepzblocks)
        {
            for (int iz = 0; iz < zblocks.Length; iz++)
            {
                zblocks[iz].CommitZBall(cachename, sBlockID, keepzblocks);
            }
        }


        // Only valid right after sort!
        bool IntegrateZBalls(string cachename, string sBlockID)
        {
            List<ZBlock.KeyBlockEntry> kentries2 = new List<ZBlock.KeyBlockEntry>(this.kentries.Capacity);
            byte[] ebuf2 = new byte[this.ebytes.Length];
            for (int iz = 0; iz < zblocks.Length; iz++)
            {
                if (!zblocks[iz].IntegrateZBall(cachename, sBlockID, this.kentries, ref this.ebytes, kentries2, ref ebuf2, ref this.evaluesbuf))
                {
                    return false;
                }
            }
            return true;
        }


        List<byte[]> net;
        internal byte[] ebytes; // Current key/value pairs.
        internal byte[] evaluesbuf; // Whole value-zblock in memory for fast seek.
        List<Entry> entries;
        List<ZBlock.KeyBlockEntry> kentries;
        int keylen;
        int modbytes;
        ArrayComboListEnumerator benum = null;
        internal ZBlock[] zblocks;
        internal byte[] _smallbuf;
        internal byte[] streamwritebuf;

        internal bool compresszmaps = true;
        internal bool compressdfschunks = true;

        internal int CookTimeout = 1000 * 60;
        internal int CookRetries = 64;

        internal int FoilKeySkipFactor = 5000;

        internal int FoilSampleBlockSize = 104857600;

        bool jobfailover = false;


        Compiler _pluginslavecompiler = null;

        Compiler compiler
        {
            get
            {
                if (null == _pluginslavecompiler)
                {
                    _pluginslavecompiler = new Compiler();
                    CommonCs.AddCommonAssemblyReferences(_pluginslavecompiler.AssemblyReferences);
                }
                return _pluginslavecompiler;
            }
        }


        protected internal void StreamWrite(System.IO.Stream stm, IList<byte> buf, int offset, int length)
        {
            if (length > streamwritebuf.Length)
            {
                streamwritebuf = new byte[length * 2];
            }

            for (int i = 0; i < length; i++)
            {
                streamwritebuf[i] = buf[offset + i];
            }

            stm.Write(streamwritebuf, 0, length);
        }


        public static ArrayComboListPart Create(int count_capacity, int estimated_row_capacity, int keylength)
        {
            return new ArrayComboListPart(count_capacity, estimated_row_capacity, keylength);
        }


        protected ArrayComboListPart(int count_capacity, int estimated_row_capacity, int keylength)
            : this(count_capacity, estimated_row_capacity, keylength, true)
        {
        }

        internal ArrayComboListPart(int count_capacity, int estimated_row_capacity, int keylength, bool startzblocks)
        {
            try
            {
                System.Diagnostics.Process.GetCurrentProcess().PriorityClass = System.Diagnostics.ProcessPriorityClass.Normal;
                System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.AboveNormal;
            }
            catch
            {
            }

            count_capacity = 1024;
            estimated_row_capacity = 32;

            ZBlock.CleanZBlockFiles("value", "key_unsorted", "key_sorted");
            ZMapBlock.CleanZMapBlockFiles();

            this.keylen = keylength;
            this.modbytes = keylength;

            if (4 + keylength > 4 + 4)
            {
                _smallbuf = new byte[4 + keylength];
            }
            else
            {
                _smallbuf = new byte[4 + 4];
            }

            net = new List<byte[]>(8);

            entries = new List<Entry>(1);
            kentries = new List<ZBlock.KeyBlockEntry>(1);

            ebytes = new byte[count_capacity * estimated_row_capacity];
            //evaluesbuf = // Moved down.
            net.Add(ebytes);

            this.streamwritebuf = new byte[1048576];

            // Pre-set with defaults.
            int numzblocks = 139;
            int zblockkeybufsize = FILE_BUFFER_SIZE;
            int zblockvaluebufsize = FILE_BUFFER_SIZE;

            if (null != DistributedObjectsSlave.xslave)
            {
                {
                    System.Xml.XmlNode xzblocks = DistributedObjectsSlave.xslave["zblocks"];
                    if (null != xzblocks)
                    {
                        {
                            System.Xml.XmlAttribute xnzb = xzblocks.Attributes["count"];
                            if (null != xnzb)
                            {
                                numzblocks = int.Parse(xnzb.Value);
                                /*{
                                    string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
                                    if (computer_name == "MAPDCMILLER")
                                    {
                                        numzblocks = 3;
                                    }
                                }*/
                            }
                        }
                        /*{
                            System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addbuffersize"];
                            if (null != xzbs)
                            {
                                int x = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                zblockkeybufsize = x;
                                zblockvaluebufsize = x;
                            }
                        }*/
                        /*{
                            System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addkeybuffersize"];
                            if (null != xzbs)
                            {
                                zblockkeybufsize = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                            }
                        }*/
                        /*{
                            System.Xml.XmlAttribute xzbs = xzblocks.Attributes["addvaluebuffersize"];
                            if (null != xzbs)
                            {
                                zblockvaluebufsize = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                            }
                        }*/
                    }
                }

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["CompressZMapBlocks"];
                    if (null != xe)
                    {
                        byte b = byte.Parse(xe.InnerText);
                        if (128 != b) compresszmaps = 1 == b;
                    }
                }

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["CompressDfsChunks"];
                    if (null != xe)
                    {
                        byte b = byte.Parse(xe.InnerText);
                        if (128 != b) compressdfschunks = 1 == b;
                    }
                }

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["CookTimeout"];
                    if (null != xe)
                    {
                        CookTimeout = int.Parse(xe.InnerText);
                    }
                }

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["CookRetries"];
                    if (null != xe)
                    {
                        CookRetries = int.Parse(xe.InnerText);
                    }
                }

                /*{
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["FoilKeySkipFactor"];
                    if (null != xe)
                    {
                        FoilKeySkipFactor = int.Parse(xe.InnerText);
                    }
                }*/

                {
                    System.Xml.XmlNode xe = DistributedObjectsSlave.xslave["FoilSampleBlockSize"];
                    if (null != xe)
                    {
                        FoilSampleBlockSize = int.Parse(xe.InnerText);
                    }
                }

            }

            evaluesbuf = new byte[(count_capacity * estimated_row_capacity) / numzblocks * 2];

            if (XLog.logging)
            {
                XLog.log("Creating " + numzblocks.ToString() + " ZBlock`s");
            }

            if (startzblocks)
            {
                zblocks = new ZBlock[numzblocks];
                for (int i = 0; i != numzblocks; i++)
                {
                    zblocks[i] = new ZBlock(this, i, zblockkeybufsize, zblockvaluebufsize);
                }
            }

        }


        public override byte[] GetValue(byte[] key, out int valuelength)
        {
            throw new Exception("Not supported");
        }


        public static long BytesToModLongSeed(IList<byte> bytes, int seed, int offset, int length)
        {
            unchecked
            {
                long result = seed;
                uint shift = (uint)seed;
                int stop = offset + length;
                for (; offset != stop; offset++)
                {
                    uint x = bytes[offset];
                    x = x << (int)(shift & 0xF); // Bound the shift to first 4 bits (shifts up to 15)
                    result += x;
                    shift += 5;
                }
                return Math.Abs(result);
            }
        }

        public static long BytesToModLongSeed(IList<byte> bytes, int seed)
        {
            return BytesToModLongSeed(bytes, seed, 0, bytes.Count);
        }


        public bool EAdd(byte[] key, int keyoffset, byte[] value, int valueoffset, int valuelen)
        {
            int zbid;
            if (null != distro)
            {
                //zbid = distro.Distro(key, keyoffset, this.keylen).MinorID;
                zbid = distro.Distro(key, keyoffset, modbytes).MinorID;
            }
            else
            {
                zbid = (int)(BytesToModLongSeed(key, 0, keyoffset, modbytes) % zblocks.Length);
            }

            return zblocks[zbid].Add(key, keyoffset, value, valueoffset, valuelen);
        }

        public bool EAdd(IList<byte> key, int keyoffset, IList<byte> value, int valueoffset, int valuelen)
        {
            int zbid;
            if (null != distro)
            {
                //zbid = distro.Distro(key, keyoffset, this.keylen).MinorID;
                zbid = distro.Distro(key, keyoffset, modbytes).MinorID;
            }
            else
            {
                zbid = (int)(BytesToModLongSeed(key, 0, keyoffset, modbytes) % zblocks.Length);
            }

            return zblocks[zbid].Add(key, keyoffset, value, valueoffset, valuelen);
        }


        public bool TimedAdd(byte[] key, int keyoffset, byte[] value, int valueoffset, int valuelength)
        {
            bool result = false;

#if ENABLE_TIMING
            long start = 0;
            if (XLog.logging)
            {
                QueryPerformanceCounter(out start);
            }
#endif

            result = EAdd(key, keyoffset, value, valueoffset, valuelength);

#if ENABLE_TIMING
            if (XLog.logging)
            {
                long stop;
                QueryPerformanceCounter(out stop);
                long freq;
                if (QueryPerformanceFrequency(out freq))
                {
                    long secs = (stop - start) / freq;
                    if (secs > 4)
                    {
                        XLog.log("ArrayComboListPart add seconds: " + secs.ToString());
                    }
                }
            }
#endif

            return result;
        }

        //public bool TimedAdd(IList<byte> key, IList<byte> value, int valuelength)
        public bool TimedAdd(byte[] key, byte[] value, int valuelength)
        {
            return TimedAdd(key, 0, value, 0, valuelength);
        }


        // Adds key and value from buf starting at offset.
        // Returns number of bytes this key/value used (i.e. number of bytes to skip to next).
        protected int TimedAddKVBuf(byte[] buf, int offset)
        {
            int valuelen = Entry.BytesToInt(buf, offset + this.keylen);
            TimedAdd(buf, offset, buf, offset + this.keylen + 4, valuelen);
            return this.keylen + 4 + valuelen;
        }


        // Warning: key needs to be immutable!
        public override void CopyAndSetValue(byte[] key, byte[] value, int valuelength)
        {
            TimedAdd(key, value, valuelength);
        }


        public void CloseZBlocks()
        {
            if (null != zblocks)
            {
                foreach (ZBlock zb in zblocks)
                {
                    zb.Close();
                }
                zblocks = null;
            }
        }

        public void CloseZMapBlocks()
        {
            if (null != zmblocks)
            {
                foreach (ZMapBlock zmb in zmblocks)
                {
                    zmb.Close();
                }
            }
        }


        private static string _spid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

        public string CreateDllFileName(string part)
        {
            return "ylib_" + part + "_" + _spid + ".ylib";
        }

        public string CreateXlibFileName(string part)
        {
            return "xlib_" + part + "_" + _spid + ".xlib";
        }


        internal class ACLEntriesOutput : EntriesOutput
        {
            internal ACLEntriesOutput(byte[] buf, NetworkStream nstm)
             : base(buf)
            {
                this.nstm = nstm;
            }


            public override bool SendBatchedEntriesOutput(byte[] buf, int length)
            {
                if (sentany)
                {
                    return false;
                }

                nstm.WriteByte((byte)'+');
                XContent.SendXContent(nstm, buf, length);

                sentany = true;
                return true;
            }


            // Returns false if at end, true if more.
            public override bool EndBatch()
            {
                if (!sentany)
                {
                    nstm.WriteByte((byte)'-');
                    return false;
                }
                sentany = false; // For next time around!
                return true;
            }


            bool sentany = false;
            NetworkStream nstm;
        }


        internal EntriesOutput CreatePluginOutput()
        {
            return new ArrayComboListPart.ACLEntriesOutput(new byte[this.buf.Length], this.nstm);
        }


        internal class SampleMapOutput : MySpace.DataMining.DistributedObjects.MapOutput
        {
            internal List<IList<byte>> samples;
            internal System.IO.Stream cacheout = null;
            internal ArrayComboListPart parent;


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != parent.keylen)
                {
                    throw new Exception("Key length mismatch for SampleMapOutput.Add():  got " + keylength.ToString() + " bytes, expected " + parent.keylen.ToString());
                }
#if DEBUG
                if (parent.modbytes > keylength)
                {
                    throw new Exception("SampleMapOutput.Add():  parent.modbytes{" + parent.modbytes.ToString() + "} > keylength{" + keylength.ToString() + "}");
                }

                if (keyoffset + parent.modbytes > keybuf.Count)
                {
                    throw new Exception("SampleMapOutput.Add():  keyoffset{" + keyoffset.ToString() + "} + parent.modbytes{" + parent.modbytes.ToString() + "} > keybuf.Count{" + keybuf.Count.ToString() + "}");
                }
#endif

                List<byte> newkey = new List<byte>(parent.modbytes);
                for (int i = 0; i < parent.modbytes; i++)
                {
                    newkey.Add(keybuf[keyoffset + i]);
                }
                //lock (samples)
                {
                    AddSampleToList(newkey, samples);
                    if (null != cacheout)
                    {
                        for (int ik = 0; ik < parent.modbytes; ik++)
                        {
                            cacheout.WriteByte(keybuf[keyoffset + ik]);
                        }
                    }
                }
            }
        }


        internal class SampleMapInput : MySpace.DataMining.DistributedObjects.MapInput
        {
            //public string Name; // Not necessarily a file name; for tracking purposes.
            //public System.IO.Stream Stream; // Relative to this input; may be a logical EndOfStream!

            public override void Reopen(long position)
            {
                throw new Exception("method not supported");
            }

            // Position from the original source file as loaded into DFS.
            public override long Position
            {
                get
                {
                    // TODO...
                    throw new Exception("SampleMapInput.Position not supported");
                }
            }


            // Length of this input, in bytes. e.g. number of bytes that can be read from Stream.
            public override long Length
            {
                get
                {
                    throw new Exception("SampleMapInput.Length not supported");
                }
            }


            public override long Fixup(long position, long length, byte[] buf, int bufoffset)
            {
                throw new Exception("SampleMapInput.Fixup() not supported");
            }

        }


        internal class FoilMapOutput : MySpace.DataMining.DistributedObjects.MapOutput, IComparer<List<byte>>
        {
            internal System.IO.Stream sampleout = null; // Actual kept samples.
            internal ArrayComboListPart parent;

            internal List<List<byte>> curmapoutput; // Current chunk's map output.
            internal int curmapoutputlen = 0; // Don't clear curmapoutput, but set this to 0 and reuse curmapoutput.
            internal int curmapoutputsize = 0; // curmapoutputlen*keymajor


            int IComparer<List<byte>>.Compare(List<byte> x, List<byte> y)
            {
#if DEBUG
                if (x.Count != y.Count)
                {
                    throw new Exception("FoilMapOutput sort compare: key length mismatch");
                }
#endif
                for (int i = 0; i < x.Count; i++)
                {
                    int diff = x[i] - y[i];
                    if (0 != diff)
                    {
                        return diff;
                    }
                }
                return 0;
            }


            int startpoint = 0;

            private void _ProcessMapOutputs()
            {
                // Process curmapoutput 0..curmapoutputlen

#if DEBUG
                if (curmapoutputsize != curmapoutputlen * parent.modbytes)
                {
#if DEBUG
                    //System.Threading.Thread.Sleep(1000 * 8);
#endif
                    throw new Exception("FoilMapOutput._ProcessMapOutputs: (curmapoutputsize != curmapoutputlen * parent.modbytes)");
                }
#endif

                curmapoutput.Sort(0, curmapoutputlen, this);

#if asdff
                //lock (samples)
                {
                    if (null != sampleout)
                    {
                        for (int ik = 0; ik < parent.modbytes; ik++)
                        {
                            sampleout.WriteByte(keybuf[keyoffset + ik]);
                        }
                    }
                }
#endif
                {
                    int curpoint = startpoint;
                    for (; curpoint < curmapoutputlen; curpoint += parent.FoilKeySkipFactor)
                    {
                        List<byte> key = curmapoutput[curpoint];
                        for (int ik = 0; ik < key.Count; ik++)
                        {
                            sampleout.WriteByte(key[ik]);
                        }
                    }
                    startpoint = curpoint - curmapoutputlen;
                }

#if DEBUG
                for (int i = 0; i < curmapoutputlen; i++)
                {
                    // Clear for debug so we can catch key length problem if used.
                    curmapoutput[i].Clear();
                }
#endif
                curmapoutputlen = 0;
                curmapoutputsize = 0;
            }


            internal void _ChunkDone()
            {
                //_ProcessMapOutputs(); // Not done here anymore.
            }


            internal void _AllChunksDone()
            {
                // Do the remainder.
                _ProcessMapOutputs();
            }


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != parent.keylen)
                {
                    throw new Exception("Key length mismatch for FoilMapOutput.Add():  got " + keylength.ToString() + " bytes, expected " + parent.keylen.ToString());
                }
#if DEBUG
                if (parent.modbytes > keylength)
                {
                    throw new Exception("FoilMapOutput.Add():  parent.modbytes{" + parent.modbytes.ToString() + "} > keylength{" + keylength.ToString() + "}");
                }

                if (keyoffset + parent.modbytes > keybuf.Count)
                {
                    throw new Exception("FoilMapOutput.Add():  keyoffset{" + keyoffset.ToString() + "} + parent.modbytes{" + parent.modbytes.ToString() + "} > keybuf.Count{" + keybuf.Count.ToString() + "}");
                }
#endif

                List<byte> newkey;
                if (curmapoutputlen >= curmapoutput.Count)
                {
                    newkey = new List<byte>(parent.modbytes);
                    curmapoutput.Add(newkey);
                    curmapoutputlen++;
                }
                else
                {
                    newkey = curmapoutput[curmapoutputlen++];
                    newkey.Clear();
                }
                for (int i = 0; i < parent.modbytes; i++)
                {
                    newkey.Add(keybuf[keyoffset + i]);
                }
                curmapoutputsize += parent.modbytes;

                if (curmapoutputsize >= parent.FoilSampleBlockSize)
                {
                    _ProcessMapOutputs();
                }

            }

        }


        internal class FoilMapInput : ACLMapInput
        {
            internal FoilMapInput(ArrayComboListPart acl)
                : base(acl)
            {
            }
        }


        static void AddSampleToList(IList<byte> sample, List<IList<byte>> list)
        {
            list.Add(sample);
        }


        static Random samprand = new Random(unchecked(System.DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));

        void _MapSampleFromFn(string fn, IMap mpsamp, MapInput sampin, MapOutput sampout)
        {
            try
            {
                using (System.IO.Stream fs = new MySpace.DataMining.AELight.DfsFileNodeStream(fn, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                {
                    sampin.Name = fn;
                    sampin.Stream = fs;
                    mpsamp.OnMap(sampin, sampout);
                }
            }
            catch (System.IO.IOException e)
            {
                throw new System.IO.IOException("I/O Error while map sampling; integrity of sampling is critical for synchronized distribution", e);
            }
            catch (Exception e)
            {
                throw new Exception("Error while map sampling; integrity of sampling is critical for synchronized distribution", e);
            }
        }

        void _MapDfsFileNodeFromFn(string fn, IMap mpsamp, ACLMapInput sampin, MapOutput sampout, bool isLastFileChunk)
        {
            try
            {
                sampin.Name = fn;
                sampin._compression = compressdfschunks;
                sampin._open();
                StaticGlobals.DSpace_InputBytesRemain = (isLastFileChunk ? sampin.Stream.Length - sampin.Stream.Position : Int64.MaxValue);
                mpsamp.OnMap(sampin, sampout);
                sampin._close();
            }
            catch (System.IO.IOException e)
            {
                throw new System.IO.IOException("I/O Error while foil sampling; integrity of sampling is critical for synchronized distribution", e);
            }
            catch (Exception e)
            {
                throw new Exception("Error while foil sampling; integrity of sampling is critical for synchronized distribution", e);
            }
        }


        void LoadFoilSamples(IMap mpsamp, string samplesoutputfn, string[] dfsfiles, string[] dfsfilenames, int[] nodesoffsets)
        {
            if (System.IO.File.Exists(samplesoutputfn))
            {
                return;
            }
            using (System.IO.FileStream samplesoutputfs = new System.IO.FileStream(samplesoutputfn, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
            {
                FoilMapInput sampin = new FoilMapInput(this);
                FoilMapOutput sampout = new FoilMapOutput();
                sampout.parent = this;
                sampout.sampleout = samplesoutputfs;
                sampout.curmapoutput = new List<List<byte>>(0x400 * 0x400);               

                if (dfsfiles.Length > 0)
                {
                    StaticGlobals.DSpace_Last = false;
                    int fi = 0;
                    int curoffset = nodesoffsets[fi];  
                    for (int i = 0; i < dfsfiles.Length; i++)
                    {
                        if (i == curoffset)
                        {
                            StaticGlobals.DSpace_InputFileName = dfsfilenames[fi++];
                            if (fi < nodesoffsets.Length)
                            {
                                curoffset = nodesoffsets[fi];
                            }
                        }
                        _MapDfsFileNodeFromFn(dfsfiles[i], mpsamp, sampin, sampout, i == dfsfiles.Length - 1);
                        sampout._ChunkDone();
                    }
                    sampout._AllChunksDone();                    
                }
            }
        }


        void LoadSamples(IMap mpsamp)
        {
            List<IList<byte>> samples = new List<IList<byte>>(0x400 * 0x40);

#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
#endif

            if ("FoilDistro" == sortclass)
            {
                for (int i = 0; i < samplefns.Length; i++)
                {
                    try
                    {
                        //----------------------------COOKING--------------------------------
                        int cooking_cooksremain = CookRetries;
                        bool cooking_inIO = false;
                        long cooking_seekpos = 0;
                        for (; ; )
                        {
                            try
                            {
                                //----------------------------COOKING--------------------------------
                                cooking_inIO = true;
                                using (System.IO.FileStream fs = new System.IO.FileStream(samplefns[i], System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                                {
                                    if (0 != cooking_seekpos)
                                    {
                                        fs.Seek(cooking_seekpos, System.IO.SeekOrigin.Begin);
                                    }
                                    cooking_inIO = false;
                                    for (; ; )
                                    {
                                        byte[] modkeybuf = new byte[modbytes];
                                        cooking_inIO = true;
                                        if (StreamReadLoop(fs, modkeybuf, modbytes) != modbytes)
                                        {
                                            break;
                                        }
                                        cooking_inIO = false;
                                        cooking_seekpos += modbytes;
                                        samples.Add(modkeybuf);
                                    }
                                }
                                cooking_seekpos = 0;
                                //----------------------------COOKING--------------------------------
                            }
                            catch (Exception e)
                            {
                                if (!cooking_inIO)
                                {
                                    throw;
                                }
                                {
                                    if (cooking_cooksremain-- <= 0)
                                    {
                                        throw new System.IO.IOException("cooked too many times", e);
                                    }
                                    System.Threading.Thread.Sleep(CookTimeout);
                                    continue; // !
                                }
                            }
                            break;
                        }
                        //----------------------------COOKING--------------------------------
                    }
                    catch (System.IO.IOException e)
                    {
                        throw new System.IO.IOException("I/O Error while loading distribution index; integrity of sampling is critical for synchronized distribution", e);
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Error while loading distribution index; integrity of sampling is critical for synchronized distribution", e);
                    }
                }
            }
            else
            {
                SampleMapInput sampin = new SampleMapInput();
                SampleMapOutput sampout = new SampleMapOutput();
                sampout.parent = this;
                sampout.samples = samples;

                if (samplefns.Length >= 1 && '>' == samplefns[0][0])
                {
                    // Read or write to sample cache (depending if it exists).
                    // It's always local.
                    string sampcachefile = IOUtils.SafeTextPath(samplefns[0].Substring(1));
                    // Use a mutex on the file name, this way only one slave generates the cache, and the rest just load it!
                    // Otherwise the snowball file name would have to include the blockid... all for redundant data.
                    using (System.Threading.Mutex sampmutex = new System.Threading.Mutex(false, "DO5_Sample_" + sampcachefile))
                    {
                        try
                        {
                            sampmutex.WaitOne();
                        }
                        catch (System.Threading.AbandonedMutexException)
                        {
                        }
                        if (System.IO.File.Exists(sampcachefile))
                        {
                            sampmutex.ReleaseMutex(); // Don't need to stay in the mutex at this point.

                            // Load cache...
                            // Note: samplefns might contain more files, even ones that weren't in the original cache; they are ignored.

                            if (buf.Length < 0x400 * 0x40)
                            {
                                buf = new byte[0x400 * 0x40];
                            }

                            try
                            {
                                using (System.IO.FileStream fs = new System.IO.FileStream(sampcachefile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                {
                                    byte[] recbuf = new byte[modbytes];
                                    for (; ; )
                                    {
                                        int rd = fs.Read(recbuf, 0, modbytes);
                                        if (rd <= 0)
                                        {
                                            break;
                                        }
                                        if (rd != modbytes)
                                        {
                                            throw new Exception("Sample cache file not of expected size. Consider recreate input or delete cache file.");
                                        }
                                        List<byte> rec = new List<byte>(modbytes);
                                        for (int ir = 0; ir < modbytes; ir++)
                                        {
                                            rec.Add(recbuf[ir]);
                                        }
                                        AddSampleToList(rec, samples);
                                    }
                                }
                            }
                            catch (System.IO.IOException e)
                            {
                                throw new System.IO.IOException("I/O Error while loading cached mapped samples; integrity of sampling is critical for synchronized distribution", e);
                            }
                            catch (Exception e)
                            {
                                throw new Exception("Error while loading cached mapped samples; integrity of sampling is critical for synchronized distribution", e);
                            }
                        }
                        else
                        {
                            try
                            {
                                // Create & write cache...
                                using (System.IO.FileStream cacheout = new System.IO.FileStream(sampcachefile, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.None, FILE_BUFFER_SIZE))
                                {
                                    sampout.cacheout = cacheout;

                                    // Need to start at index 1, since 0 is the target.
                                    int pivot = 1;
                                    if (samplefns.Length > 1)
                                    {
                                        pivot += samprand.Next() % (samplefns.Length - 1);
                                    }
                                    string samplefn = "";
                                    try
                                    {
                                        for (int i = pivot; i < samplefns.Length; i++)
                                        {
                                            samplefn = samplefns[i];
                                            _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                        }
                                        for (int i = 1; i < pivot; i++)
                                        {
                                            samplefn = samplefns[i];
                                            _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                        }
                                    }
                                    catch (System.IO.IOException e)
                                    {
                                        throw new System.IO.IOException("I/O Problem while loading samples from file '" + samplefn + "' (also writing cache); integrity of sampling is critical for synchronized distribution", e);
                                    }
                                    catch (Exception e)
                                    {
                                        throw new Exception("Problem while loading samples from file '" + samplefn + "' (also writing cache); integrity of sampling is critical for synchronized distribution", e);
                                    }
                                }
                            }
                            catch (System.IO.IOException e)
                            {
                                throw new System.IO.IOException("I/O Error while map sampling and creating cached mapped samples; integrity of sampling is critical for synchronized distribution", e);
                            }
                            catch (Exception e)
                            {
                                throw new Exception("Error while map sampling and creating cached mapped samples; integrity of sampling is critical for synchronized distribution", e);
                            }
                            sampmutex.ReleaseMutex();
                        }
                    }
                }
                else
                {
                    string samplefn = "";
                    try
                    {
                        if (samplefns.Length > 0)
                        {
                            if (btreeCapSize == 0)
                            {
                                int pivot = samprand.Next() % samplefns.Length;
                                for (int i = pivot; i < samplefns.Length; i++)
                                {
                                    samplefn = samplefns[i];
                                    _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                }
                                for (int i = 0; i < pivot; i++)
                                {
                                    samplefn = samplefns[i];
                                    _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                }
                            }
                            else
                            {
                                ulong aSampleSize = 0;

                                _MapSampleFromFn(samplefns[0], mpsamp, sampin, sampout);

                                if (samples.Count > 0)
                                {
                                    aSampleSize = (ulong)(samples.Count * samples[0].Count);
                                }

                                int filesToSample = samplefns.Length;

                                if ((aSampleSize * (ulong)samplefns.Length) >= btreeCapSize)
                                {
                                    filesToSample = (int)(btreeCapSize / aSampleSize);
                                }

                                if (filesToSample > 1)
                                {
                                    int factor = -1;
                                    bool isSkip = false;
                                    int filesCount = 0;

                                    if (filesToSample < samplefns.Length)
                                    {
                                        factor = samplefns.Length / filesToSample;
                                        filesCount = filesToSample;

                                        if (factor == 1)
                                        {
                                            filesCount = samplefns.Length - filesToSample;
                                            factor = samplefns.Length / filesCount;
                                            isSkip = true;
                                        }
                                    }

                                    int pivot = samprand.Next() % samplefns.Length;
                                    for (int i = pivot; i < samplefns.Length; i++)
                                    {
                                        if (IncludeSample(i, factor, isSkip, filesCount))
                                        {
                                            samplefn = samplefns[i];
                                            _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                        }
                                    }
                                    for (int i = 0; i < pivot; i++)
                                    {
                                        if (IncludeSample(i, factor, isSkip, filesCount))
                                        {
                                            samplefn = samplefns[i];
                                            _MapSampleFromFn(samplefns[i], mpsamp, sampin, sampout);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (System.IO.IOException e)
                    {
                        throw new System.IO.IOException("I/O Problem while loading samples from file '" + samplefn + "'; integrity of sampling is critical for synchronized distribution", e);
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Problem while loading samples from file '" + samplefn + "'; integrity of sampling is critical for synchronized distribution", e);
                    }
                }
            }

            switch (sortclass)
            {
                case "MdoDistro":
                    distro = new MdoDistro(samples, sampleblockcount, zblocks.Length);
                    break;
                case "DsDistro":
                    distro = new DsDistro(samples, sampleblockcount, zblocks.Length);
                    break;
                case "FoilDistro":
                    distro = new FoilDistro(samples, sampleblockcount, zblocks.Length);
                    break;
                case "Distro2":
                    if (2 != modbytes)
                    {
                        throw new Exception("Distro2 (hashsorted/rhashsorted) expects modbytes (KeyMajor) 2");
                    }
                    distro = new Distro2(sampleblockcount, zblocks.Length);
                    break;
                case "Distro1":
                    if (1 != modbytes)
                    {
                        throw new Exception("Distro1 (hashsorted/rhashsorted) expects modbytes (KeyMajor) 1");
                    }
                    distro = new Distro1(sampleblockcount, zblocks.Length);
                    break;
                default:
                    throw new Exception("No such sort class '" + sortclass + "' (OutputMethod)");
            }

            XLog.log("ArrayComboList: " + samples.Count.ToString() + " samples produced from sample-map on this sub process; using sort class " + sortclass + "; memory usage " + distro.GetMemoryUsage());
        }

        private bool IncludeSample(int index, int factor, bool isSkipFactor, int filesCount)
        {
            if (index == 0)
            {
                return false;
            }

            if (factor == -1)
            {
                return true;
            }

            if (isSkipFactor)
            {
                return !(((index - 1) % factor == 0) && (index < factor * filesCount));
            }
            else
            {
                return ((index % factor == 0) && (index < factor * filesCount));
            }
        }

        internal class ACLLoadOutput: LoadOutput
        {
            internal ACLLoadOutput(ArrayComboListPart acl)
            {
                this.acl = acl;
            }


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != acl.keylen)
                {
                    throw new Exception("Key length mismatch for LoadOutput; got " + keylength.ToString() + " bytes, expected " + acl.keylen.ToString());
                }
                acl.EAdd(keybuf, keyoffset, valuebuf, valueoffset, valuelength);
            }


            ArrayComboListPart acl;
        }


        internal void ZMEAdd(IList<byte> key, int keyoffset, IList<byte> value, int valueoffset, int valuelen)
        {
#if _ZMEbwa
            if (keyoffset + modbytes > key.Count)
            {
                throw new Exception("keyoffset and modbytes are out of bounds for ZMEAdd (MapOutput key buffer) [" + ((null != distro) ? "distro" : "mod") + "]");
            }
#endif

            int zmbid;
            if (null != distro)
            {
                zmbid = distro.Distro(key, keyoffset, modbytes).MajorID;
            }
            else
            {
                zmbid = (int)(BytesToModLongSeed(key, 8934745, keyoffset, modbytes) % zmblocks.Length);
            }

            if (zmbid < 0 || zmbid >= zmblocks.Length)
            {
                StringBuilder keysb = new StringBuilder(keylen);
                for (int ki = 0; ki < keylen; ki++)
                {
                    if (keysb.Length > 0)
                    {
                        keysb.Append(',');
                    }
                    keysb.Append((byte)key[keyoffset + ki]);
                }
                throw new Exception("key mapped to invalid zMapBlock for ZMEAdd (MapOutput zMapBlockID=" + zmbid.ToString() + ") [" + ((null != distro) ? "distro" : "mod") + "] {key=" + keysb.ToString() + "}");
            }

            zmblocks[zmbid].Add(key, keyoffset, value, valueoffset, valuelen);
        }


        internal class ACLMapOutput : MapOutput
        {
            internal ACLMapOutput(ArrayComboListPart acl)
            {
                this.acl = acl;
            }


            public override void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (keylength != acl.keylen)
                {
                    throw new Exception("Key length mismatch for MapOutput; got " + keylength.ToString() + " bytes, expected " + acl.keylen.ToString());
                }
                if (keyoffset + keylength > keybuf.Count)
                {
                    throw new Exception("Key length and key offset are out of bounds for MapOutput");
                }


                if(!StaticGlobals.DSpace_OutputDirection_ascending)
                {
                    if (flipbuf == null)
                    {
                        flipbuf = new byte[keylength];
                    }
                    for (int i = 0; i < keylength; i++)
                    {
                        flipbuf[i] = (byte)(~keybuf[i + keyoffset]);
                    }
                    acl.ZMEAdd(flipbuf, 0, valuebuf, valueoffset, valuelength);
                }
                else
                {
                    acl.ZMEAdd(keybuf, keyoffset, valuebuf, valueoffset, valuelength);
                }                
            }

            ArrayComboListPart acl;
            private static byte[] flipbuf = null;
        }


        internal class ACLMapInput : MapInput
        {
            internal ACLMapInput(ArrayComboListPart acl)
            {
                this.acl = acl;
            }

            public override long Position
            {
                get
                {
                    throw new Exception("MapInput.Position not implemented");
                }
            }

            public override long Length
            {
                get
                {
                    throw new Exception("MapInput.Length not implemented");
                }
            }

            public override long Fixup(long position, long length, byte[] buf, int bufoffset)
            {
                throw new Exception("MapInput.Fixup() not implemented");
            }


            internal bool _compression = false;
            internal static byte[] openbuf = new byte[32]; // Shared!

            // Depends on Name being the full file path.
            internal void _open()
            {
                //----------------------------COOKING--------------------------------
                int cooking_cooksremain = acl.CookRetries;
                bool cooking_stream_is_open = false;
#if DEBUGcook
                int tester = 2;
#endif
                for (; ; )
                {
                    try
                    {
                        //----------------------------COOKING--------------------------------
#if DEBUGcook
                        if (tester-- == -2)
                        {
                            throw new NotFiniteNumberException();
                        }
#endif
                        //Stream = new System.IO.FileStream(Name, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, DISTBUF_SIZE);
                        Stream = new MySpace.DataMining.AELight.DfsFileNodeStream(Name, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                        cooking_stream_is_open = true;
                        if (_compression)
                        {
                            Stream = new System.IO.Compression.GZipStream(Stream, System.IO.Compression.CompressionMode.Decompress);
                        }
#if DEBUGcook
                        if (tester-- == 0)
                        {
                            throw new NotFiniteNumberException();
                        }
#endif
                        {
                            // Skip the dfs-chunk file-header...
                            int headerlength = 0;
                            {
                                if (4 != Stream.Read(openbuf, 0, 4))
                                {
                                    return;
                                }
                                {
                                    headerlength = Entry.BytesToInt(openbuf);
                                    if (headerlength > 4)
                                    {
                                        int remain = headerlength - 4;
                                        if (remain > openbuf.Length)
                                        {
                                            openbuf = new byte[Entry.Round2Power(remain)];
                                        }
                                        StreamReadExact(Stream, openbuf, remain);
                                    }
                                }
                            }
                            // Future: exclude dfs-chunk file-header from MapInput.Length
                        }
                //----------------------------COOKING--------------------------------
                    }
                    catch(Exception e)
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif
                        if (cooking_cooksremain-- <= 0)
                        {
                            throw new System.IO.IOException("cooked too many times", e);
                        }
                        if (cooking_stream_is_open)
                        {
                            Stream.Close();
                            Stream = null;
                            cooking_stream_is_open = false;
                        }
                        System.Threading.Thread.Sleep(acl.CookTimeout);
                        continue; // !
                    }
                    break;
                }
                //----------------------------COOKING--------------------------------
            }

            internal void _close()
            {
                Stream.Close();
                Stream = null;
            }

            public override void Reopen(long position)
            {
                _close();
                _open(); // Skips header!
                Stream.Seek(position, System.IO.SeekOrigin.Current); // SeekOrigin.Current because the position passed in is relative to after-header.
            }


            ArrayComboListPart acl;
        }


        RangeDistro distro;
        int sampleblockcount = 0; // Only valid if sampling.
        string[] samplefns = null;
        ulong btreeCapSize = 0;
        string sortclass = null;

        ZMapBlock[] zmblocks = null;

        public class ZMapBlock
        {
            ArrayComboListPart parent;

            int zmblockID; // ZMapBlock ID (0-based n)
            internal string fzmblockfilename = null;
            long zmblocksize = 0;
            System.IO.Stream fzmblock;
            bool xchg = false;


            public ZMapBlock(ArrayComboListPart parent, int zmblockID, string zmapblockbasename)
            {
                this.parent = parent;
                this.zmblockID = zmblockID;

                //fzmblockfilename = CreateZMapBlockFileName(zmblockID);
                fzmblockfilename = zmapblockbasename.Replace("%n", zmblockID.ToString());

                {
                    fzmblock = new System.IO.FileStream(fzmblockfilename, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                    if (parent.compresszmaps)
                    {
                        fzmblock = new System.IO.Compression.GZipStream(fzmblock, System.IO.Compression.CompressionMode.Compress);
                    }
                }
            }


            public void PrepareForExchange()
            {
                if (xchg)
                {
                    throw new Exception("Cannot exchange ZMapBlock more than once");
                }
                xchg = true;

                //fzmblock.Seek(0, System.IO.SeekOrigin.Begin);
                fzmblock.Close();
            }


            public bool Add(IList<byte> keybuf, int keyoffset, IList<byte> valuebuf, int valueoffset, int valuelength)
            {
                if (xchg)
                {
                    throw new Exception("Cannot add to ZMapBlock while/after exchange");
                }

                parent.StreamWrite(fzmblock, keybuf, keyoffset, parent.keylen);
                Entry.ToBytes(valuelength, parent._smallbuf, 0);
                fzmblock.Write(parent._smallbuf, 0, 4);
                parent.StreamWrite(fzmblock, valuebuf, valueoffset, valuelength);

                zmblocksize += parent.keylen + 4 + valuelength;
                /* // Can't do .Length on gzip stream...
#if DEBUG
                if (zmblocksize != fzmblock.Length)
                {
                    throw new Exception("DEBUG ERROR: zmblocksize mismatch");
                }
#endif
                 * */

                return true;
            }


            /*
            internal static string CreateZMapBlockFileName(int zmblockID)
            {
                return "zmap_" + ZBlock._spid + "_" + zmblockID.ToString() + ".zm";
            }
             */


            public static void CleanZMapBlockFiles()
            {
                /*
                // Clean any potential old zmblock files...
                bool found = true;
                for (int i = 0; found; i++)
                {
                    found = false;
                    string fn = CreateZMapBlockFileName(i);
                    if (System.IO.File.Exists(fn))
                    {
                        System.IO.File.Delete(fn);
                        found = true;
                    }
                }
                 * */
            }


            public void Close()
            {
                _justclose();

                if (null != fzmblockfilename)
                {
                    try
                    {
                        System.IO.File.Delete(fzmblockfilename);
                        fzmblockfilename = null;
                    }
                    catch (Exception e)
                    {
                    }
                }
            }


            private void _justclose()
            {
                if (null != fzmblock)
                {
                    fzmblock.Close();
                    fzmblock = null;
                }
            }
        }


        static bool IsValidsBlockID(string sBlockID)
        {
            bool gotany = false;
            for (int i = 0; i < sBlockID.Length; i++)
            {
                if (!char.IsLetterOrDigit(sBlockID[i]))
                {
                    return false;
                }
                gotany = true;
            }
            return gotany;
        }

        static string GetSafeZballName(string sBlockID)
        {
            string reason = "";
            if (MySpace.DataMining.AELight.dfs.IsBadFilename(sBlockID, out reason))
            {
                throw new Exception("Invalid delta cache name: " + reason);
            }
            return sBlockID;
        }


        protected override void ReloadConfig()
        {
            base.ReloadConfig();

            //_loadconfig();
        }


        protected override void ProcessCommand(NetworkStream nstm, char tag)
        {
            //string s;
            int len;

            switch (tag)
            {
                case 'F': // 'F' for BeforeLoad (first)
                    {
                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("load");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            //if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("load");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            System.IO.FileStream stm = System.IO.File.Create(dllfn);
                            stm.Write(buf, 0, len);
                            stm.Close();
                        }

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IBeforeReduce plugin named " + xclassname + " for before-load: " + dllfn);
                        }

                        IBeforeLoad bl = LoadBeforeLoadPlugin(dllfn, classname);
                        LoadOutput loadoutput = new ACLLoadOutput(this);
                        bl.OnBeforeLoad(loadoutput);
                    }
                    break;

                /*
                case '': // Compression...
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        // 0 means no compression, 1 means gzip compression, and 128 means skip.
                        if (len > 0 && 128 != buf[0]) compresszmaps = 1 == buf[0]; // [0] is for zmaps.
                        if (len > 1 && 128 != buf[1]) compressdfschunks = 1 == buf[1]; // [1] is for DFS chunks.
                    }
                    break;
                 * */

                case 'f': // 'f' for foil sampling
                    {

#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("foil");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            //if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("foil");
                        System.Reflection.Assembly asm = null;
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (0 == len)
                            {
                                asm = compiler.CompileSourceFile(xlibfn, false, dllfn);
                            }
                            else
                            {
                                System.IO.FileStream stm = System.IO.File.Create(dllfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string samplesoutputfn = XContent.ReceiveXString(nstm, buf);

                        string[] dfsfiles = null;// XContent.ReceiveXString(nstm, buf).Split(';');
                        string[] dfsfilenames = null;
                        int[] nodesoffsets = null;
                        {
                            string xfiles = XContent.ReceiveXString(nstm, buf);
                            if (xfiles.Length > 0)
                            {
                                int pipe = xfiles.IndexOf('|');
                                dfsfiles = xfiles.Substring(0, pipe).Split(';');
                                string[] xoffsets = xfiles.Substring(pipe + 1).Split(';');
                                dfsfilenames = new string[xoffsets.Length];
                                nodesoffsets = new int[xoffsets.Length];
                                for (int xi = 0; xi < xoffsets.Length; xi++)
                                {
                                    string xoffset = xoffsets[xi];
                                    pipe = xoffset.IndexOf('|');
                                    int offset = Int32.Parse(xoffset.Substring(0, pipe));
                                    string fname = xoffset.Substring(pipe + 1);
                                    dfsfilenames[xi] = fname;
                                    nodesoffsets[xi] = offset;
                                }
                            }
                            else
                            {
                                dfsfiles = new string[0];
                            }
                        }

                        if (dfsfiles.Length == 1 && 0 == dfsfiles[0].Length)
                        {
                            dfsfiles = new string[0];
                        }

                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        if (len >= 4)
                        {
                            FoilKeySkipFactor = Entry.BytesToInt(buf, 0);
                        }

#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IMap plugin named " + xclassname + " for foil: " + dllfn);
                        }

                        //if (!string.IsNullOrEmpty(sortclass))
                        {
                            string sampleclassname = classname + "_Sample";
                            //IMap mpsamp = LoadPluginInterface<IMap>(dllfn, sampleclassname);
                            IMap mpsamp = null == asm ? LoadPluginInterface<IMap>(dllfn, sampleclassname) : LoadPluginInterface<IMap>(asm, dllfn, sampleclassname);
                            LoadFoilSamples(mpsamp, samplesoutputfn, dfsfiles, dfsfilenames, nodesoffsets);
                        }

                    }
                    break;

                case 'C': // Compile!
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif

                        string code = XContent.ReceiveXString(nstm, buf);

                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        bool exe = len > 0 && 0 != buf[0];
                        bool debugmode = len > 1 && 0 != buf[1];

                        string outputname = XContent.ReceiveXString(nstm, buf);

                        bool _olddebugmode = compiler.DebugMode;
                        try
                        {
                            try
                            {
                                compiler.DebugMode = debugmode;

                                compiler.CompileSource(code, exe, outputname);
                            }
                            catch (BadImageFormatException)
                            {
                            }

                            nstm.WriteByte((byte)'+');
                        }
                        catch(Exception ec)
                        {
                            nstm.WriteByte((byte)'-');
                            XContent.SendXContent(nstm, ec.GetType().Name);
                            XContent.SendXContent(nstm, ec.ToString());
                        }
                        compiler.DebugMode = _olddebugmode;

                    }
                    break;

                case 'M': // 'M' for Map
                    {
                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("map");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            //if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("map");
                        System.Reflection.Assembly asm = null;
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (0 == len)
                            {
                                asm = compiler.CompileSourceFile(xlibfn, false, dllfn);
                            }
                            else
                            {
                                System.IO.FileStream stm = System.IO.File.Create(dllfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string zmapblockbasename = XContent.ReceiveXString(nstm, buf);

                        string[] dfsfiles = null;// XContent.ReceiveXString(nstm, buf).Split(';');
                        string[] dfsfilenames = null;
                        int[] nodesoffsets = null;
                        {
                            string xfiles = XContent.ReceiveXString(nstm, buf);
                            if (xfiles.Length > 0)
                            {
                                int pipe = xfiles.IndexOf('|');
                                dfsfiles = xfiles.Substring(0, pipe).Split(';');
                                string[] xoffsets = xfiles.Substring(pipe + 1).Split(';');
                                dfsfilenames = new string[xoffsets.Length];
                                nodesoffsets = new int[xoffsets.Length];
                                for (int xi = 0; xi < xoffsets.Length; xi++)
                                {
                                    string xoffset = xoffsets[xi];
                                    pipe = xoffset.IndexOf('|');
                                    int offset = Int32.Parse(xoffset.Substring(0, pipe));
                                    string fname = xoffset.Substring(pipe + 1);
                                    dfsfilenames[xi] = fname;
                                    nodesoffsets[xi] = offset;
                                }
                            }
                            else
                            {
                                dfsfiles = new string[0];
                            }
                        }

                        XContent.ReceiveXBytes(nstm, out len, buf); // !
                        int numzmblocks = 0;
                        if (len >= 4)
                        {
                            numzmblocks = Entry.BytesToInt(buf);
                        }

                        if (numzmblocks <= 0)
                        {
                            throw new Exception("Invalid number of ZMapBlocks: " + numzmblocks.ToString());
                        }
                        if (null != zmblocks)
                        {
                            throw new Exception("Map already called (zmblocks is set)");
                        }
                        zmblocks = new ZMapBlock[numzmblocks];
                        for (int i = 0; i < numzmblocks; i++)
                        {
                            zmblocks[i] = new ZMapBlock(this, i, zmapblockbasename);
                        }

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IMap plugin named " + xclassname + " for map: " + dllfn);
                        }

#if DEBUGdistrotests
                        System.Threading.Thread.Sleep(1000 * 10);
                        {
                            Distro2 xyz = new Distro2(1816, 271);
                            int highest = 0;
                            Dictionary<int, int> majors = new Dictionary<int, int>();
                            Dictionary<int, int> minors = new Dictionary<int, int>();
                            byte[] bb = new byte[2];
                            for (int y = 0; y <= ushort.MaxValue; y++)
                            {
                                bb[0] = (byte)(y >> 8);
                                bb[1] = (byte)y;
                                RangeNode rn = xyz.Distro(bb, 0, 2);
                                if (majors.ContainsKey(rn.MajorID))
                                {
                                    majors[rn.MajorID]++;
                                }
                                else
                                {
                                    majors[rn.MajorID] = 1;
                                }
                                if (minors.ContainsKey(rn.MinorID))
                                {
                                    minors[rn.MinorID]++;
                                }
                                else
                                {
                                    minors[rn.MinorID] = 1;
                                }
                            }
                            foreach (KeyValuePair<int, int> kvp in majors)
                            {
                                if (kvp.Value > highest)
                                {
                                    highest = kvp.Value;
                                }
                            }
                        }
                        {
                            Distro2 xyz = new Distro2(1816, 1);
                            int highest = 0;
                            Dictionary<int, int> sdf = new Dictionary<int, int>();
                            byte[] bb = new byte[2];
                            for (int y = 0; y <= ushort.MaxValue; y++)
                            {
                                bb[0] = (byte)(y >> 8);
                                bb[1] = (byte)y;
                                int major = xyz.Distro(bb, 0, 2).MajorID;
                                if (sdf.ContainsKey(major))
                                {
                                    sdf[major]++;
                                }
                                else
                                {
                                    sdf[major] = 1;
                                }
                            }
                            foreach (KeyValuePair<int, int> kvp in sdf)
                            {
                                if (kvp.Value > highest)
                                {
                                    highest = kvp.Value;
                                }
                            }
                        }
                        {
                            Distro2 xyz = new Distro2(17, 171);
                            int highest = 0;
                            Dictionary<int, int> sdf = new Dictionary<int, int>();
                            byte[] bb = new byte[2];
                            for (int y = 0; y <= ushort.MaxValue; y++)
                            {
                                bb[0] = (byte)(y >> 8);
                                bb[1] = (byte)y;
                                int major = xyz.Distro(bb, 0, 2).MajorID;
                                if (sdf.ContainsKey(major))
                                {
                                    sdf[major]++;
                                }
                                else
                                {
                                    sdf[major] = 1;
                                }
                            }
                            foreach (KeyValuePair<int, int> kvp in sdf)
                            {
                                if (kvp.Value > highest)
                                {
                                    highest = kvp.Value;
                                }
                            }
                        }
#endif

                        if (!string.IsNullOrEmpty(sortclass))
                        {
                            string sampleclassname = classname + "_Sample";
                            //IMap mpsamp = LoadPluginInterface<IMap>(dllfn, sampleclassname);
                            IMap mpsamp = null == asm ? LoadPluginInterface<IMap>(dllfn, sampleclassname) : LoadPluginInterface<IMap>(asm, dllfn, sampleclassname);
                            LoadSamples(mpsamp);
                        }

                        //System.Threading.Thread.Sleep(1000 * 10);
                        //IMap mp = LoadPluginInterface<IMap>(dllfn, classname);
                        IMap mp = null == asm ? LoadPluginInterface<IMap>(dllfn, classname) : LoadPluginInterface<IMap>(asm, dllfn, classname);
                        ACLMapInput mapinput = new ACLMapInput(this);
                        ACLMapOutput mapoutput = new ACLMapOutput(this);
                        int maxerrors = 10;
                        StaticGlobals.DSpace_Last = false;
                        int curoffset = 0;
                        int fi = 0;
                        if (dfsfiles.Length > 0)
                        {
                            curoffset = nodesoffsets[fi];
                        }
                        for (int i = 0; i < dfsfiles.Length; i++)
                        {
                            if (i == curoffset)
                            {
                                StaticGlobals.DSpace_InputFileName = dfsfilenames[fi++];
                                if (fi < nodesoffsets.Length)
                                {
                                    curoffset = nodesoffsets[fi];
                                }
                            }
                            if (0 == dfsfiles[i].Length)
                            {
                                continue;
                            }
                            try
                            {
                                mapinput.Name = dfsfiles[i];
                                mapinput._compression = compressdfschunks;
                                mapinput._open(); // Depends on MapInput.Name being the full file path.                              
                                StaticGlobals.DSpace_InputBytesRemain = (i == dfsfiles.Length - 1 ? mapinput.Stream.Length - mapinput.Stream.Position : Int64.MaxValue);                                
                                mp.OnMap(mapinput, mapoutput);
                                mapinput._close();
                            }
                            catch (Exception e)
                            {
                                if (jobfailover
                                    && null != e as System.IO.IOException)
                                {
                                    throw new System.IO.IOException("I/O Problem during Map with file '" + dfsfiles[i] + "'", e);
                                }
                                if (maxerrors <= 0)
                                {
                                    throw new Exception("Too may problems during Map", e);
                                }
                                maxerrors--;
                                //throw new Exception("Problem during Map with file '" + dfsfiles[i] + "'", e);
                                SetError("Problem during Map with file '" + dfsfiles[i] + "': " + e.ToString());
                            }
                        }
                    }
                    break;

                case 'd': // Get current directory.
                    nstm.WriteByte((byte)'+');
                    XContent.SendXContent(nstm, Environment.CurrentDirectory);
                    break;

                case 'D': // Enumerator DLL binary.
                    {
                        string classname = XContent.ReceiveXString(nstm, buf);

                        string xlibfn = CreateXlibFileName("enum");
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            //if (0 != len)
                            {
                                System.IO.FileStream stm = System.IO.File.Create(xlibfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        string dllfn = CreateDllFileName("enum");
                        System.Reflection.Assembly asm = null;
                        {
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (0 == len)
                            {
                                asm = compiler.CompileSourceFile(xlibfn, false, dllfn);
                            }
                            else
                            {
                                System.IO.FileStream stm = System.IO.File.Create(dllfn);
                                stm.Write(buf, 0, len);
                                stm.Close();
                            }
                        }

                        if (XLog.logging)
                        {
                            string xclassname = classname;
                            if (null == xclassname)
                            {
                                xclassname = "<null>";
                            }
                            XLog.log("Loading IBeforeReduce plugin named " + xclassname + " for enumeration: " + dllfn);
                        }
                        //IBeforeReduce plugin = LoadPluginInterface<IBeforeReduce>(dllfn, classname);
                        IBeforeReduce plugin = null == asm ? LoadPluginInterface<IBeforeReduce>(dllfn, classname) : LoadPluginInterface<IBeforeReduce>(asm, dllfn, classname);
                        benum = new ArrayComboListEnumerator(this, plugin);
                    }
                    break;

                case 'c':
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        int n = Entry.BytesToInt(buf);
                        int count = 0;
                        if (null != benum)
                        {
                            IReducedToFile irf = benum.plugin as IReducedToFile;
                            if (null != irf)
                            {
                                List<long> appendsizes = new List<long>();
                                count = irf.GetReducedFileCount(n, appendsizes);
                                if (buf.Length < 4 + 8 * appendsizes.Count)
                                {
                                    buf = new byte[Entry.Round2Power(4 + 8 * appendsizes.Count)];
                                }
                                Entry.ToBytes(count, buf, 0);
                                int offset = 4;
                                for (int i = 0; i < appendsizes.Count; i++, offset += 8)
                                {
                                    Entry.LongToBytes(appendsizes[i], buf, offset);
                                }
                                XContent.SendXContent(nstm, buf, 4 + 8 * appendsizes.Count);
                                break; // !
                            }
                        }
                        Entry.ToBytes(count, buf, 0);
                        XContent.SendXContent(nstm, buf, 4);
                    }
                    break;

                case 'e': // Batch 'get next' enumeration.
                    {
                        try
                        {
                            if (null == benum)
                            {
                                benum = new ArrayComboListEnumerator(this, new EntryEnumerator());
                            }

                            benum.Go();
                        }
                        catch
                        {
                            if (!benum.IsFinished)
                            {
                                // Only send if not finished yet;
                                // because if finished, '-' was sent already.
                                nstm.WriteByte((byte)'-');
                            }
                            throw;
                        }
                    }
                    break;

                case 's':
                    try
                    {
                        // Sort..

                        try
                        {
                            System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.BelowNormal;
                        }
                        catch
                        {
                        }

#if ENABLE_TIMING
                        long start = 0;
                        if(XLog.logging)
                        {
                            QueryPerformanceCounter(out start);
                        }
#endif

                        int readkeybuflen = 1048576;
                        int readvaluebuflen = 1048576;
                        if (null != DistributedObjectsSlave.xslave)
                        {
                            System.Xml.XmlNode xzblocks = DistributedObjectsSlave.xslave["zblocks"];
                            if (null != xzblocks)
                            {
                                {
                                    System.Xml.XmlAttribute xzbs = xzblocks.Attributes["readbuffersize"];
                                    if (null != xzbs)
                                    {
                                        int x = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                        readkeybuflen = x;
                                        readvaluebuflen = x;
                                    }
                                }
                                {
                                    System.Xml.XmlAttribute xzbs = xzblocks.Attributes["readkeybuffersize"];
                                    if (null != xzbs)
                                    {
                                        readkeybuflen = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                    }
                                }
                                {
                                    System.Xml.XmlAttribute xzbs = xzblocks.Attributes["readvaluebuffersize"];
                                    if (null != xzbs)
                                    {
                                        readvaluebuflen = DistributedObjectsSlave.ParseCapacity(xzbs.Value);
                                    }
                                }
                            }
                        }

                        foreach (ZBlock zb in zblocks)
                        {
                            zb.LeaveAddMode(readkeybuflen, readvaluebuflen);
                        }

                        foreach (ZBlock zb in zblocks)
                        {
                            zb.Sort(kentries, ref ebytes);
                        }

                        kentries = new List<ZBlock.KeyBlockEntry>(1); // Non-null, but release larger kentries.

#if ENABLE_TIMING
                        if(XLog.logging)
                        {
                            long stop;
                            QueryPerformanceCounter(out stop);
                            long freq;
                            if(QueryPerformanceFrequency(out freq))
                            {
                                long secs = (stop - start) / freq;
                                if(secs > 10)
                                {
                                    XLog.log("ArrayComboListPart sort seconds: " + secs.ToString());
                                }
                            }
                        }
#endif

                        nstm.WriteByte((byte)'+');
                    }
                    catch
                    {
                        nstm.WriteByte((byte)'-');
                        throw;
                    }
                    break;

                case 'p': // Batch put/publish...
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        uint gbfree = (uint)(DistributedObjectsSlave.GetCurrentDiskFreeBytes() / 1073741824);
#if DEBUG
                        {
                            string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
									 if (computer_name == "MAPDDRULE" || computer_name == "MAPDCMILLER" || computer_name == "MAPDCLOK")
                            {
                                gbfree = uint.MaxValue;
                            }
                        }
#endif
                        if (gbfree > 20)
                        {
                            for (int y = 0; y < len; )
                            {
                                /*if (y + this.keylen + 4 > len)
                                {
                                    SetError("batch publish about to step out of bounds: offset " + y.ToString() + " into " + len.ToString() + " length buffer");
                                    break;
                                }*/
                                y += TimedAddKVBuf(buf, y);
                            }
                        }
                        else
                        {
                            if (!nofreedisklog)
                            {
                                nofreedisklog = true;
                                SetError("Low free disk space; now dropping keys/values.");
                                if (jobfailover)
                                {
                                    XLog.failoverlog("x disk Low free disk space"); // Don't recover.
                                }
                            }
                        }
                    }
                    break;

                case 'z': // CommitZBalls
                    try
                    {
                        try
                        {
                            System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.Lowest;
                        }
                        catch
                        {
                        }

                        string zballname = XContent.ReceiveXString(nstm, buf);
                        string sBlockID = XContent.ReceiveXString(nstm, buf);
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        bool b = buf.Length >= 0 && 0 != buf[0];
                        if (!IsValidsBlockID(sBlockID))
                        {
                            throw new Exception("Invalid sBlockID");
                        }
                        CommitZBalls(GetSafeZballName(zballname), sBlockID, b);
                        nstm.WriteByte((byte)'+');
                    }
                    catch
                    {
                        nstm.WriteByte((byte)'-');
                        throw;
                    }
                    break;

                case 'Z': // IntegrateZBalls
                    try
                    {
                        try
                        {
                            System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.Lowest;
                        }
                        catch
                        {
                        }

                        string zballname = XContent.ReceiveXString(nstm, buf);
                        string sBlockID = XContent.ReceiveXString(nstm, buf);
                        if (!IsValidsBlockID(sBlockID))
                        {
                            throw new Exception("Invalid sBlockID");
                        }
                        if (IntegrateZBalls(GetSafeZballName(zballname), sBlockID))
                        {
                            nstm.WriteByte((byte)'+');
                        }
                        else
                        {
                            nstm.WriteByte((byte)'-');
                        }
                    }
                    catch
                    {
                        nstm.WriteByte((byte)'-');
                        throw;
                    }
                    break;

                case 'x': // Start ZMapBlockServer.
                    try
                    {
                        int xport = StartZMapBlockServer();
                        Entry.ToBytes(xport, buf, 0);
                        XContent.SendXContent(nstm, buf, 4);
                    }
                    catch
                    {
                        Entry.ToBytes(0, buf, 0); // Port 0 on error.
                        XContent.SendXContent(nstm, buf, 4);
                        throw;
                    }
                    break;

                case 'X': // Stop ZMapBlockServer.
                    StopZMapBlockServer();
                    break;

                case 'k':
                    buf = XContent.ReceiveXBytes(nstm, out len, buf);
                    if (buf.Length >= 4)
                    {
                        modbytes = Entry.BytesToInt(buf);
                    }
                    break;

                case 'j':
                    buf = XContent.ReceiveXBytes(nstm, out len, buf);
                    if (buf.Length >= 4)
                    {
                        jobfailover = 0 != Entry.BytesToInt(buf);
                    }
                    break;

                case 'W': // Overwrite compiler assembly references and options...
                    {
                        string wstr = XContent.ReceiveXString(nstm, buf);
                        int splitpos = wstr.IndexOf(";;");
                        string refs = wstr, opts = "";
                        if (-1 != splitpos)
                        {
                            refs = wstr.Substring(0, splitpos);
                            opts = wstr.Substring(splitpos + 2);
                        }
                        compiler.AssemblyReferences.Clear(); // 'W' overwrites!
                        foreach (string sref in refs.Split(';'))
                        {
                            if (sref.Length > 0)
                            {
                                compiler.AssemblyReferences.Add(sref);
                            }
                        }
                        compiler.CompilerOptions = opts;
                    }
                    break;

                case 'K': // Samples!
                    try
                    {
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        sampleblockcount = 0;
                        if (len >= 4)
                        {
                            sampleblockcount = Entry.BytesToInt(buf, 0);
                        }
                        if(sampleblockcount <= 0)
                        {
                            throw new Exception("Samples K failure (invalid sampleblockcount of " + sampleblockcount.ToString() + ")");
                        }

                        {
                            string ss = XContent.ReceiveXString(nstm, buf);
                            if (ss.Length > 0)
                            {
                                samplefns = ss.Split(';');
                            }
                            else
                            {
                                samplefns = new string[0];
                            }
                        }

                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        btreeCapSize = 0;
                        if (len >= 8)
                        {
                            btreeCapSize = Entry.BytesToULong(buf, 0);
                        }

                        sortclass = XContent.ReceiveXString(nstm, buf);

                        nstm.WriteByte((byte)'+');
                    }
                    catch
                    {
                        nstm.WriteByte((byte)'-');
                        throw;
                    }
                    break;

                case 'y': // ZMapBlock Exchange.
                    {                        
                        string lastxfile = null;
                        try
                        {
                            try
                            {
                                System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.Normal;
                            }
                            catch
                            {
                            }

                            int ymaxerrorprint = 5;

                            int rangelen;
                            byte[] rangebuf = XContent.ReceiveXBytes(nstm, out rangelen, null);
                            List<int> zmranges = new List<int>(rangelen / 4); // My ranges.
                            for (int ioffset = 0; ioffset < rangelen; ioffset += 4)
                            {
                                zmranges.Add(Entry.BytesToInt(rangebuf, ioffset));
                            }
                            string hostsandports = XContent.ReceiveXString(nstm, buf).Trim();
                            if (0 != hostsandports.Length)
                            {
                                string[] otherbasepaths = hostsandports.Split(';');
                                {
                                    foreach (string basepath in otherbasepaths)
                                    {
                                        try
                                        {
                                            for (int ir = 0; ir < zmranges.Count; ir++)
                                            {
                                                string fn = basepath.Replace("%n", zmranges[ir].ToString());
                                                lastxfile = fn;
                                                //----------------------------COOKING--------------------------------
                                                int cooking_cooksremain = CookRetries;
                                                bool cooking_stream_is_open = false;
                                                for (; ; )
                                                {
                                                    try
                                                    {
                                                    //----------------------------COOKING--------------------------------
                                                        using (System.IO.FileStream fs = new System.IO.FileStream(fn, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                                        {
                                                            cooking_stream_is_open = true;
                                                            System.IO.Stream gzs = fs;
                                                            if (compresszmaps)
                                                            {
                                                                gzs = new System.IO.Compression.GZipStream(gzs, System.IO.Compression.CompressionMode.Decompress);
                                                            }
                                                            ZMapStreamToZBlocks(gzs, -1, fn, FILE_BUFFER_SIZE, compresszmaps);
                                                        }
                                                    //----------------------------COOKING--------------------------------
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        if (cooking_cooksremain-- <= 0)
                                                        {
                                                            throw new System.IO.IOException("cooked too many times", e);
                                                        }
                                                        if (cooking_stream_is_open)
                                                        {
                                                            throw;
                                                        }
                                                        System.Threading.Thread.Sleep(CookTimeout);
                                                        continue; // !
                                                    }
                                                    break;
                                                }
                                                //----------------------------COOKING--------------------------------
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            if (jobfailover
                                                && null != e as System.IO.IOException)
                                            {
                                                throw new System.IO.IOException("I/O Problem loading remote zMapBlock '" + lastxfile + "'", e);
                                            }
                                            if (ymaxerrorprint > 0)
                                            {
                                                ymaxerrorprint--;
                                                SetError("Problem loading remote zMapBlock '" + lastxfile + "': " + e.ToString());
                                            }
                                        }
                                    }
                                }
                                lastxfile = null;
                            }

                            // Now process the zmapblocks of mine that I own.
                            for (int i = 0; i < zmranges.Count; i++)
                            {
                                string zmfn = zmblocks[zmranges[i]].fzmblockfilename;
                                try
                                {
                                    using (System.IO.FileStream fs = new System.IO.FileStream(zmfn, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                    {
                                        System.IO.Stream gzs = fs;
                                        if (compresszmaps)
                                        {
                                            gzs = new System.IO.Compression.GZipStream(gzs, System.IO.Compression.CompressionMode.Decompress);
                                        }
                                        ZMapStreamToZBlocks(gzs, -1, zmfn, FILE_BUFFER_SIZE, compresszmaps);
                                    }
                                }
                                catch (Exception e)
                                {
                                    if (jobfailover
                                        && null != e as System.IO.IOException)
                                    {
                                        throw new System.IO.IOException("I/O Problem loading local zMapBlock '" + zmfn + "'", e);
                                    }
                                    if (ymaxerrorprint != 0)
                                    {
                                        ymaxerrorprint--;
                                        SetError("Problem loading local zMapBlock '" + zmfn + "': " + e.ToString());
                                    }
                                }
                            }

                            nstm.WriteByte((byte)'+');
                        }
                        catch(Exception e)
                        {
                            nstm.WriteByte((byte)'-');
                            if (null != lastxfile)
                            {
                                if (null != e as System.IO.IOException)
                                {
                                    throw new System.IO.IOException("Problem during ZBlockExchange with file '" + lastxfile + "'", e);
                                }
                                throw new Exception("Problem during ZBlockExchange with file '" + lastxfile + "'", e);
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }
                    break;

                case '.': // Ping?
                    nstm.WriteByte((byte)','); // Pong!
                    break;

                default:
                    base.ProcessCommand(nstm, tag);
                    break;
            }
        }


        internal static int StreamReadLoop(System.IO.Stream stm, byte[] buf, int len)
        {
            int sofar = 0;
            while (sofar < len)
            {
                int xread = stm.Read(buf, sofar, len - sofar);
                if (xread <= 0)
                {
                    break;
                }
                sofar += xread;
            }
            return sofar;
        }

        internal static void StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
        {
            if (len != StreamReadLoop(stm, buf, len))
            {
                throw new System.IO.IOException("Unable to read from stream");
            }
        }


        void ZMapStreamToZBlocks(System.IO.Stream stm, long len, string sfn, int iFILE_BUFFER_SIZE, bool bcompresszmaps)
        {
            // Relies on _smallbuf being at least keylen+4
            if (len < 0)
            {
                len = long.MaxValue;
            }

#if COOK_TEST_ZMapStreamToZBlocks
            int throwon = 2;
#endif
            //----------------------------COOKING--------------------------------
            bool cooking_is_cooked = false;
            int cooking_cooksremain = CookRetries;
            bool cooking_is_read = false;
            long cooking_pos = 0; // Last known good position between records/lines.
            //----------------------------COOKING--------------------------------

            while (true)
            {
                try
                {
                    //----------------------------COOKING--------------------------------
                    cooking_is_read = true; // Important!

                    if (cooking_is_cooked)
                    {
                        cooking_is_cooked = false;
                        System.Threading.Thread.Sleep(CookTimeout);
                        stm.Close();
                        stm = new System.IO.FileStream(sfn, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, iFILE_BUFFER_SIZE);
                        {
                            if (compresszmaps)
                            {
                                stm = new System.IO.Compression.GZipStream(stm, System.IO.Compression.CompressionMode.Decompress);
                            }
                        }
                        stm.Seek(cooking_pos, System.IO.SeekOrigin.Begin);
                    }
                    //----------------------------COOKING--------------------------------

                    if (0 == StreamReadLoop(stm, _smallbuf, keylen + 4))
                    {
                        break;
                    }

                    int valuelen = Entry.BytesToInt(_smallbuf, keylen);
                    if (valuelen > buf.Length)
                    {
                        buf = new byte[Entry.Round2Power(valuelen)];
                    }
#if COOK_TEST_ZMapStreamToZBlocks
                    if (throwon > 0 && --throwon == 0)
                    {
                        throw new Exception("COOK_TEST_ZMapStreamToZBlocks");
                    }
#endif
                    StreamReadExact(stm, buf, valuelen);
                    //----------------------------COOKING--------------------------------
                    cooking_pos += keylen + 4 + valuelen;
                    cooking_is_read = false;
                    //----------------------------COOKING--------------------------------

                    EAdd(_smallbuf, 0, buf, 0, valuelen);

                }
                catch (Exception e)
                {
                    //----------------------------COOKING--------------------------------
                    if (!cooking_is_read)
                    {
                        throw;
                    }
                    if (cooking_cooksremain-- <= 0)
                    {
                        throw new System.IO.IOException("cooked too many times", e);
                    }
                    cooking_is_cooked = true;
                    continue;
                    //----------------------------COOKING--------------------------------
                }
            }
        }


        // Returns the port number.
        int StartZMapBlockServer()
        {
            lock (this)
            {
                for (int i = 0; i < zmblocks.Length; i++)
                {
                    zmblocks[i].PrepareForExchange();
                }

                return 0;
            }
        }

        void StopZMapBlockServer()
        {
            CloseZMapBlocks(); // !
        }


        static bool nofreedisklog = false;


        public override void ProcessCommands(NetworkStream nstm)
        {
            try
            {
                base.ProcessCommands(nstm);

                CloseZBlocks();
                CloseZMapBlocks();

                if (jobfailover)
                {
                    XLog.failoverlog("d done");
                }
            }
            catch (OutOfMemoryException e)
            {
                SetError("ArrayComboList Sub Process exception: " + e.ToString() + "  {" + CurrentCommand.ToString() + "}");
                if (jobfailover)
                {
                    XLog.failoverlog("x " + e.Message); // Non-recoverable.
                }
                throw;
            }
            catch (Exception e)
            {
                uint gbfree = (uint)(DistributedObjectsSlave.GetCurrentDiskFreeBytes() / 1073741824);
                if (gbfree <= 20)
                {
                    Exception newe = new Exception("Free disk space too low", e);
                    SetError("ArrayComboList Sub Process exception: " + newe.ToString() + "  {" + CurrentCommand.ToString() + "}");
                    if (jobfailover)
                    {
                        XLog.failoverlog("x " + e.Message); // Non-recoverable; other exception.
                        //throw new DistributedObjectsSlave.DistObjectAbortException(newe); // No.. let it attempt to resume...
                    }
                    throw newe;
                }
                else
                {
                    SetError("ArrayComboList Sub Process exception: " + e.ToString() + "  {" + CurrentCommand.ToString() + "}");
                    if (jobfailover)
                    {
                        if (DistributedObjectsSlave.CanSlaveExceptionFailover(e))
                        {
                            XLog.failoverlog("r " + e.Message); // Recoverable.
                            throw new DistributedObjectsSlave.DistObjectAbortException(e);
                        }
                        else
                        {
                            XLog.failoverlog("x " + e.Message); // Non-recoverable; other exception.
                        }
                    }
                    throw;
                }
            }
        }


        int _kcmp(ZBlock.KeyBlockEntry x, ZBlock.KeyBlockEntry y)
        {
            for (int i = 0; i != this.keylen; i++)
            {
                int diff = (int)x.key[i] - (int)y.key[i];
                //Console.WriteLine("Comparing {0} with {1} = {2}", (int)kbuf1[k1 + i], (int)kbuf2[k2 + i], diff);
                if (0 != diff)
                {
                    return diff;
                }
            }
            return 0;
        }


#if USE_DfsMapper
        public class DfsMapper : MySpace.DataMining.DistributedObjects.IMap
        {
            List<byte> _mapbuf = new List<byte>();
            public void OnMap(MapInput input, MapOutput output)
            {
                for (; ; )
                {
                    _mapbuf.Clear();
                    int ib;
                    for (; ; )
                    {
                        ib = input.Stream.ReadByte();
                        if (-1 == ib)
                        {
                            if (0 != _mapbuf.Count)
                            {
                                Map(ByteSlice.Create(_mapbuf), output);
                            }
                            return; // !
                        }
                        _mapbuf.Add((byte)ib);
                        if ('\n' == ib)
                        {
                            break;
                        }
                    }
                    Map(ByteSlice.Create(_mapbuf), output);
                }
            }

            //----------

            // Helper function for OnBeforeLoad.
            void AddWordHelper(List<byte> word, List<byte> valuebuf, MapOutput output)
            {
                if (word.Count > 0)
                {
                    if (word.Count >= 16)
                    {
                        word.Clear();
                        return; // Long words skipped.
                    }
                    for (int i = word.Count; i < 16; i++)
                    {
                        word.Add(0); // Pad with 0s.
                    }
                    output.Add(ByteSlice.Create(word), ByteSlice.Create(valuebuf));
                    word.Clear();
                }
            }

            public virtual void Map(ByteSlice line, MapOutput output)
            {
                List<byte> valuebuf = new List<byte>(); // Preallocate value buffer.
                Entry.ToBytesAppend(1, valuebuf); // Set with 1 for all values, since they're the same.

                List<byte> word = new List<byte>(); // Preallocate word buffer.
                for (int i = 0; i < line.Length; i++)
                {
                    byte ib = line[i];
                    if (ib >= 0x80
                        || (!char.IsLetterOrDigit((char)ib) && (char)ib != '-' && (char)ib != '_' && (char)ib != '\'')) // Find word break.
                    {
                        AddWordHelper(word, valuebuf, output);
                    }
                    else
                    {
                        word.Add((byte)char.ToLower((char)ib)); // Keep track of word characters.
                    }
                }
                AddWordHelper(word, valuebuf, output);
            }
            //----------

        }
#endif


        public class LoadPluginException : Exception
        {
            public LoadPluginException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }

            public LoadPluginException(string msg)
                : base(msg)
            {
            }
        }

        public static Iface LoadPluginInterface<Iface>(System.Reflection.Assembly assembly, string name, string classname)
        {
            try
            {
                Iface plugin = default(Iface);
                string ifacename = typeof(Iface).FullName;
                foreach (Type type in assembly.GetTypes())
                {
                    if (type.IsClass)
                    {
                        if (null == classname
                            || 0 == string.Compare(type.Name, classname))
                        {
                            if (type.GetInterface(ifacename) != null)
                            {
                                plugin = (Iface)System.Activator.CreateInstance(type);
                                break;
                            }
                            if (null != classname)
                            {
                                throw new LoadPluginException("Class " + classname + " was found, but does not implement interface " + typeof(Iface).Name);
                            }
                        }
                    }
                }
                if (null == plugin)
                {
                    throw new LoadPluginException("Plugin from '" + name + "' not found");
                }
                return plugin;
            }
            catch (System.Reflection.ReflectionTypeLoadException e)
            {
                string x = "";
                foreach (Exception ex in e.LoaderExceptions)
                {
                    x += "\n\t";
                    x += ex.Message;
                }
                throw new LoadPluginException("ReflectionTypeLoadException error(s) with plugin '" + name + "': " + x);
            }
        }

        public static Iface LoadPluginInterface<Iface>(System.Reflection.Assembly assembly, string classname)
        {
            return LoadPluginInterface<Iface>(assembly, assembly.FullName, classname);
        }

        public static Iface LoadPluginInterface<Iface>(string dllfilepath, string classname)
        {
            System.Reflection.Assembly assembly = System.Reflection.Assembly.LoadFrom(dllfilepath);
            return LoadPluginInterface<Iface>(assembly, dllfilepath, classname);
        }


        protected internal IBeforeLoad LoadBeforeLoadPlugin(string dllfilepath, string classname)
        {
            return LoadPluginInterface<IBeforeLoad>(dllfilepath, classname);
        }


        internal EntriesInput GetEntriesInput()
        {
            EntriesInput input = new EntriesInput();
            input.entries = this.entries;
            input.KeyLength = this.keylen;
            input.net = this.net;
            return input;
        }


#if ENABLE_TIMING
        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceCounter(out long lpPerformanceCount);

        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceFrequency(out long lpFrequency);
#endif

    }


    public class ArrayComboListEnumerator
    {
        int curzblock = -1;


        internal ArrayComboListEnumerator(ArrayComboListPart acl, IBeforeReduce plugin)
        {
            this.acl = acl;
            this.input = acl.GetEntriesInput();
            this.output = acl.CreatePluginOutput();
            this.plugin = plugin;
            this.ordplugin = plugin as IBeforeReduceOrd;
        }


        bool LoadNextZBlock()
        {
            if (curzblock + 1 >= acl.zblocks.Length)
            {
                input.entries.Clear();
                input.net.Clear();
                return false;
            }
            curzblock++;
            acl.zblocks[curzblock].CopyInto(input.net, input.entries, ref acl.ebytes, ref acl.evaluesbuf);
            if (null != this.ordplugin)
            {
                this.ordplugin.SetReduceOrd(curzblock);
            }
            return true;
        }

        bool EnsureStarted()
        {
            if (curzblock < 0)
            {
                try
                {
                    System.Threading.Thread.CurrentThread.Priority = System.Threading.ThreadPriority.Lowest;
                }
                catch
                {
                }

                return LoadNextZBlock();
            }
            return true;
        }


        internal void Go()
        {            
            if (EnsureStarted())
            {                
                int lastnonemptyzblock = 0;
                {
                    for (int iz = acl.zblocks.Length - 1; iz >= 0; iz--)
                    {
                        if (acl.zblocks[iz].getzkeyblocksize() > 0)
                        {
                            lastnonemptyzblock = iz;
                            break;
                        }
                    }
                }
                StaticGlobals.DSpace_Last = false;
                for (; ; )
                {
                    StaticGlobals.DSpace_InputBytesRemain = (curzblock == lastnonemptyzblock ? 0 : 1);
                    bool cont = plugin.OnGetEnumerator(input, output);
                    if (!cont)
                    {
                        cont = LoadNextZBlock();
                    }
                    if (output.CheckFlushedReset())
                    {
                        break;
                    }
                    if (!cont)
                    {
                        break;
                    }
                }
            }   
            output.Finish();
            if (!output.EndBatch())
            {
                if (!finished)
                {
                    finished = true;
                    IAfterReduce after = plugin as IAfterReduce;
                    if (null != after)
                    {
                        after.OnEnumeratorFinished();
                    }
                }
            }            
            
           
                   
        }


        public bool IsFinished
        {
            get
            {
                return finished;
            }
        }


        bool finished = false;
        private ArrayComboListPart acl;
        private EntriesInput input;
        private EntriesOutput output;
        internal IBeforeReduce plugin;
        internal IBeforeReduceOrd ordplugin;
    }

}
