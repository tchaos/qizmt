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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{

    public class GlyphNode
    {

        public const byte SearchType_FLAT = 0;
        public const byte SearchType_BINARY = 1;
        public const byte SearchType_HASH = 1;

        public static List<GlyphNode> leaf_nodes;


        public IList<byte> leaf_word_ref;
        public List<GlyphNode> child_glyphs;
        public List<short> hash;

        public ushort iMajorId;
        public ushort iMinorId;
        public byte Glyph;
        public byte nodtype; // NodeSearchType


        internal int _GetMemoryUsage()
        {
            checked
            {
                int mu = IntPtr.Size * 3 + 2 + 2 + 1 + 1;
                mu += IntPtr.Size; // GC.
                if (null != child_glyphs)
                {
                    mu += child_glyphs.Capacity * IntPtr.Size;
                    for (int cg = 0; cg < child_glyphs.Count; cg++)
                    {
                        if (null != child_glyphs[cg])
                        {
                            mu += child_glyphs[cg]._GetMemoryUsage();
                        }
                    }
                }
                return mu;
            }
        }


        public static GlyphNode Prepare(byte newGlyph)
        {
            return GlyphNode.Prepare(newGlyph, ushort.MaxValue, ushort.MaxValue);
        }

        public static GlyphNode Prepare(byte newGlyph, ushort major, ushort minor)
        {
            GlyphNode gn = new GlyphNode();
            gn.hash = null;
            gn.leaf_word_ref = null;
            gn.Glyph = newGlyph;
            gn.iMajorId = major;
            gn.iMinorId = minor;
            gn.nodtype = SearchType_FLAT;
            gn.child_glyphs = null;
            gn.hash = null;
            return gn;
        }

        public bool IsLeafNode()
        {
            return child_glyphs == null;
        }

        enum SearchState { HIGH, MIDDLE, LOW }

        public GlyphNode FindWord(GlyphNode current, IList<byte> word, int keyoffset, int keylength)
        {
            SearchState pSearchState = SearchState.MIDDLE;
            for (int i = 0; i < keylength; i++)
            {
                byte curGlyph = word[keyoffset + i];
                if (pSearchState == SearchState.LOW)
                {
                    current = current.child_glyphs[0];
                }
                else if (pSearchState == SearchState.HIGH)
                {
                    current = current.child_glyphs[current.child_glyphs.Count - 1];
                }
                else
                {
                    if (null == current.child_glyphs)
                    {
                        return current;
                    }
                    current = current.SelectChild(curGlyph);
                    if (curGlyph > current.Glyph)
                    {
                        pSearchState = SearchState.HIGH;
                    }
                    else if (curGlyph < current.Glyph)
                    {
                        pSearchState = SearchState.LOW;
                    }
                }
            }
            return current;
        }

        static public void IndexNodes(GlyphNode current)
        {
            VisitNode(current);
            if (current.child_glyphs != null)
            {
                int count = current.child_glyphs.Count;
                for (int i = 0; i < count; i++)
                {
                    IndexNodes(current.child_glyphs[i]);
                }
            }
        }

        static public void VisitNode(GlyphNode current)
        {
            if (current.child_glyphs != null)
            {
                current.SortChildren();
                if (current.child_glyphs.Count > 1)
                {
                    current.HashChildren();
                }
            }
        }

        public void EatWord(IList<byte> word, int index)
        {
            if (nodtype != SearchType_FLAT)
            {
                throw new Exception("node type must be FLAT for this operation");
            }
            if (index >= word.Count)
            {
                return;
            }
            else
            {
                byte newGlyph = word[index];
                bool contains_ch = ContainsChild(newGlyph);
                if (!contains_ch)
                {
                    bool is_leaf = false;
                    if (index == word.Count - 1)
                    {
                        is_leaf = true;
                    }
                    AppendChild(newGlyph, is_leaf, word);
                }
                GlyphNode newGlyphNode = SelectChild(newGlyph);
                newGlyphNode.EatWord(word, index + 1);
            }
        }

        public bool ContainsChild(byte childGlyph)
        {
            if (child_glyphs == null)
            {
                return false;
            }
            if (nodtype == SearchType_FLAT)
            {
                int childCnt = child_glyphs.Count;
                for (int i = 0; i < childCnt; i++)
                {
                    if (child_glyphs[i].Glyph == childGlyph)
                    {
                        return true;
                    }
                }
                return false;
            }
            return true;
        }

        public GlyphNode SelectChild(byte childGlyph)
        {
            if (nodtype == SearchType_FLAT)
            {
                int childCnt = child_glyphs.Count;
                for (int i = 0; i < childCnt; i++)
                {
                    if (child_glyphs[i].Glyph == childGlyph)
                    {
                        return child_glyphs[i];
                    }
                }
                throw new Exception("child not found while parent in flat mode");
            }
            else if (nodtype == SearchType_BINARY)
            {
                int location = BinarySearch(child_glyphs, 0, child_glyphs.Count, childGlyph);
                GlyphNode foundGlyphNode = child_glyphs[location];
                return foundGlyphNode;
            }
            else if (nodtype == SearchType_HASH)
            {
                int location = hash[(int)childGlyph];
                GlyphNode foundGlyphNode = child_glyphs[location];
                return foundGlyphNode;
            }
            throw new Exception("unkown node type");
        }

        private int BinarySearch(List<GlyphNode> all_nodes, int start, int length, byte searchGlyph)
        {
            if (length == 1)
            {
                return start;
            }
            else
            {
                int halflength = length / 2;
                if (searchGlyph <= all_nodes[start].Glyph)
                {
                    return start;
                }
                if (searchGlyph >= all_nodes[start].Glyph && searchGlyph < all_nodes[start + halflength].Glyph)
                {
                    return BinarySearch(all_nodes, start, halflength, searchGlyph);
                }
                else
                {
                    return BinarySearch(all_nodes, start + halflength, length - halflength, searchGlyph);
                }
            }
        }

        public void AppendChild(byte childGlyph, bool is_leaf, IList<byte> word)
        {
            if (nodtype != SearchType_FLAT)
            {
                throw new Exception("node type must be FLAT for this operation");
            }
            if (child_glyphs == null)
            {
                child_glyphs = new List<GlyphNode>();
            }
            GlyphNode pNewChildNode = GlyphNode.Prepare(childGlyph);
            child_glyphs.Add(pNewChildNode);
            if (is_leaf)
            {
                pNewChildNode.leaf_word_ref = word;
                if (leaf_nodes == null)
                {
                    leaf_nodes = new List<GlyphNode>();
                }
                leaf_nodes.Add(pNewChildNode);
            }
        }

        public void SortChildren()
        {
            if (nodtype != SearchType_FLAT)
            {
                throw new Exception("node type must be FLAT for this operation");
            }
            child_glyphs.Sort(new cmprGlyphNodeNoUnEat());
            nodtype = SearchType_BINARY;
        }

        public void HashChildren()
        {
            if (SearchType_BINARY != nodtype)
            {
                throw new Exception("node type must be binary to hash children");
            }
            int glyphcount = child_glyphs.Count;
            hash = new List<short>(256);
            for (int i = 0; i < 256; i++)
            {
                short location = (short)BinarySearch(child_glyphs, 0, child_glyphs.Count, (byte)i);
                hash.Add(location);
            }
            //hash.Sort();//??why
            nodtype = SearchType_HASH;
        }

    }
}
