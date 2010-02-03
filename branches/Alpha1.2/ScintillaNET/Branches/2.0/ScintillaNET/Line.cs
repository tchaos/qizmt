using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace ScintillaNet
{
	public class Line : ScintillaHelperBase
	{
		private int _number = 0;
		protected internal Line(Scintilla scintilla, int number) : base(scintilla) 
		{
			_number = number;
		}

		public int Number
		{
			get
			{
				return _number;
			}
			set
			{
				_number = value;
			}
		}

		public string Text
		{
			get
			{
				string s;
				NativeScintilla.GetLine(_number, out s);
				return s;
			}
			set
			{
				NativeScintilla.SetTargetStart(StartPosition);
				NativeScintilla.SetTargetEnd(EndPosition);
				NativeScintilla.ReplaceTarget(-1, value);
			}
		}

		public Range Range
		{
			get
			{
				return Scintilla.GetRange(StartPosition, EndPosition);
			}
		}

		public void Select()
		{
			NativeScintilla.SetSel(StartPosition, EndPosition);
		}

		public int StartPosition
		{
			get
			{
				return NativeScintilla.PositionFromLine(_number);
			}
		}

		public int EndPosition
		{
			get
			{
				return NativeScintilla.GetLineEndPosition(_number);
			}
		}

		public int Length
        {
			get				
			{
				return NativeScintilla.LineLength(_number);
			}
        }

		public int Height
		{
			get
			{
				return NativeScintilla.TextHeight(_number);
			}
		}

		public void Goto()
		{
			NativeScintilla.GotoLine(_number);
		}

		public int LineState
		{
			get
			{
				return NativeScintilla.GetLineState(_number);
			}
			set
			{
				NativeScintilla.SetLineState(_number, value);
			}
		}

		public int Indentation
		{
			get
			{
				return NativeScintilla.GetLineIndentation(_number);
			}
			set
            {
				NativeScintilla.SetLineIndentation(_number, value);
            }
		}

		public int IndentPosition
		{
			get
			{
				return NativeScintilla.GetLineIndentPosition(_number);
			}
		}

		public int SelectionStartPosition
		{
			get
			{
				return NativeScintilla.GetLineSelStartPosition(_number);
			}
		}

		public int SelectionEndPosition
		{
			get
			{
				return NativeScintilla.GetLineSelEndPosition(_number);
			}
		}

		public MarkerInstance AddMarker(int markerNumber)
		{
			return new MarkerInstance(Scintilla, new Marker(Scintilla, markerNumber), NativeScintilla.MarkerAdd(_number, markerNumber));
		}

		public MarkerInstance AddMarker(Marker marker)
		{
			return new MarkerInstance(Scintilla, marker, NativeScintilla.MarkerAdd(_number, marker.Number));
		}

		public void AddMarkerSet(uint markerMask)
		{
			NativeScintilla.MarkerAddSet(_number, markerMask);
		}

		public void AddMarkerSet(IEnumerable<Marker> markers)
		{
			AddMarkerSet(Utilities.GetMarkerMask(markers));
		}

		public void AddMarkerSet(IEnumerable<int> markers)
		{
			AddMarkerSet(Utilities.GetMarkerMask(markers));
		}

		public void DeleteMarker(int markerNumber)
		{
			NativeScintilla.MarkerDelete(_number, markerNumber);
		}

		public void DeleteMarkerSet(IEnumerable<int> markerNumbers)
		{
			foreach (int markerNumber in markerNumbers)
				NativeScintilla.MarkerDelete(_number, markerNumber);
		}

		public void DeleteMarkerSet(IEnumerable<Marker> markers)
		{
			foreach (Marker m in markers)
				NativeScintilla.MarkerDelete(_number, m.Number);
		}

		public void DeleteAllMarkers()
		{
			DeleteMarker(-1);
		}

		public int GetMarkerMask()
		{
			return NativeScintilla.MarkerGet(_number);
		}

		public List<Marker> GetMarkers()
		{
			List<Marker> ret = new List<Marker>();
			int mask = GetMarkerMask();
			for (int i = 0; i < 32; i++)
				if ((mask & i) == i)
					ret.Add(new Marker(Scintilla, i));

			return ret;
		}

		public Line FindNextMarker(uint markerMask)
		{
			int foundLine = NativeScintilla.MarkerNext(_number + 1, markerMask);
			if (foundLine < 0)
				return null;

			return Scintilla.Lines[foundLine];
		}

		public Line FindNextMarker(Marker marker)
		{
			return FindNextMarker((uint)marker.Number);
		}

		public Line FindNextMarker(IEnumerable<int> markers)
		{
			return FindNextMarker(Utilities.GetMarkerMask(markers));
		}

		public Line FindNextMarker(IEnumerable<Marker> markers)
		{
			return FindNextMarker(Utilities.GetMarkerMask(markers));
		}

		public Line FindPreviousMarker(uint markerMask)
		{
			int foundLine = NativeScintilla.MarkerPrevious(_number - 1, markerMask);
			if (foundLine < 0)
				return null;

			return Scintilla.Lines[foundLine];
		}

		public Line FindPreviousMarker(Marker marker)
		{
			return FindPreviousMarker((uint)marker.Number);
		}

		public Line FindPreviousMarker(IEnumerable<int> markers)
		{
			return FindPreviousMarker(Utilities.GetMarkerMask(markers));
		}

		public Line FindPreviousMarker(IEnumerable<Marker> markers)
		{
			return FindPreviousMarker(Utilities.GetMarkerMask(markers));
		}

		public int VisibleLineNumber
		{
			get
			{
				return NativeScintilla.VisibleFromDocLine(_number);
			}
		}

		public bool IsVisible
		{
			get
			{
				return NativeScintilla.GetLineVisible(_number);
			}
			set
            {
				if (value)
					NativeScintilla.ShowLines(_number, _number);
				else
					NativeScintilla.HideLines(_number, _number);
            }
		}

		public void EnsureVisible()
		{
			NativeScintilla.EnsureVisible(_number);
		}

		public int FoldLevel
		{
			get
			{
				return (int)(NativeScintilla.GetFoldLevel(_number) & Constants.SC_FOLDLEVELNUMBERMASK);
			}
			set
			{
				uint flags = NativeScintilla.GetFoldLevel(_number) & (Constants.SC_FOLDLEVELHEADERFLAG | Constants.SC_FOLDLEVELWHITEFLAG);
				NativeScintilla.SetFoldLevel(_number, (uint)value | flags);
			}
		}

		public bool IsFoldPoint
		{
			get
			{
				return (NativeScintilla.GetFoldLevel(_number) & Constants.SC_FOLDLEVELHEADERFLAG) == Constants.SC_FOLDLEVELHEADERFLAG;
			}
			set
			{
				if (value)
					NativeScintilla.SetFoldLevel(_number, NativeScintilla.GetFoldLevel(_number) | Constants.SC_FOLDLEVELHEADERFLAG);
				else
					NativeScintilla.SetFoldLevel(_number, NativeScintilla.GetFoldLevel(_number) & ~Constants.SC_FOLDLEVELHEADERFLAG);
			}
		}

		public bool IsFoldWhitespace
		{
			get
			{
				return (NativeScintilla.GetFoldLevel(_number) & Constants.SC_FOLDLEVELWHITEFLAG) == Constants.SC_FOLDLEVELWHITEFLAG;
			}
			set
			{
				if (value)
					NativeScintilla.SetFoldLevel(_number, NativeScintilla.GetFoldLevel(_number) | Constants.SC_FOLDLEVELWHITEFLAG);
				else
					NativeScintilla.SetFoldLevel(_number, NativeScintilla.GetFoldLevel(_number) & ~Constants.SC_FOLDLEVELWHITEFLAG);
			}
		}

		public void ToggleFoldExpanded()
		{
			NativeScintilla.ToggleFold(_number);
		}

		public bool FoldExpanded
		{
			get
			{
				return NativeScintilla.GetFoldExpanded(_number);
			}
			set
            {
				NativeScintilla.SetFoldExpanded(_number, value);
            }
		}

		public Line GetLastFoldChild()
		{
			return GetLastFoldChild(-1);
		}

		public Line GetLastFoldChild(int level)
		{
			int num = NativeScintilla.GetLastChild(_number, level);
			if (num < 0)
				return null;

			return new Line(Scintilla, num);
		}

		public Line FoldParent
		{
			get
			{
				int num = NativeScintilla.GetFoldParent(_number);
				if (num < 0)
					return null;

				return new Line(Scintilla, num);
			}
		}

		public int WrapCount
		{
			get
			{
				return NativeScintilla.WrapCount(_number);
			}
		}

		public override bool Equals(object obj)
		{
			Line l = obj as Line;
			if (l == null)
				return false;

			return l.Scintilla == Scintilla && l._number == _number;
		}

		public override string ToString()
		{
			return "Line " + _number.ToString();
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		public Line Next
		{
			get
			{
				return new Line(Scintilla, _number + 1);
			}
		}

		public Line Previous
		{
			get
			{
				return new Line(Scintilla, _number - 1);
			}
		}
	}
}
