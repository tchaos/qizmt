using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace Graphing
{
	public partial class Graph : Form
	{
		private int m_boxSize = 500;
		private string m_filePath = "";
		private bool m_suppressInitialCentroids = false;
		private int m_gridCount = 0;

		public Graph(string filePath, int gridCount, bool suppressInitialCentroids)
		{
			m_filePath = filePath;
			m_gridCount = (gridCount > 0) ? gridCount : 10;
			m_suppressInitialCentroids = suppressInitialCentroids;
			InitializeComponent();
		}

		public static string GetUsage()
		{
			return @"Usage:

Graphing <file path> [<actions> [<arguemnts>]]

Actions:
    -s                           suppress the display of initial centroids
    -g <gridCount>       set the number of grids in x-direction
";
		}

		protected override void OnPaint(PaintEventArgs e)
		{
			Graphics g = e.Graphics;

			//Draw grid lines
			int delta = m_boxSize / m_gridCount;

			for (int i = 0; i <= m_boxSize; i = i + delta)
				g.DrawLine(System.Drawing.Pens.Blue, 0, i, m_boxSize, i);

			for (int i = 0; i <= m_boxSize; i = i + delta)
				g.DrawLine(System.Drawing.Pens.Blue, i, 0, i, m_boxSize);

			StreamReader reader = new StreamReader(m_filePath);

			while (reader.Peek() > 0)
			{
				string line = reader.ReadLine();

				MyPoint p = MyPoint.Create(line.Substring(2));
				int x = ConvertToPixel(p.X);
				int y = m_boxSize - ConvertToPixel(p.Y);

				if (line.StartsWith("V"))			//Vertex	
					g.DrawLine(new Pen(Color.White), x, y, x, y + 1);
				else if (line.StartsWith("F"))				//Final centroid
					g.DrawEllipse(new Pen(Color.Yellow, 3), new Rectangle(x, y, 5, 5));
				else if (!this.m_suppressInitialCentroids && line.StartsWith("I"))		//Initial centroid
					g.DrawEllipse(new Pen(Color.Red, 3), new Rectangle(x, y, 5, 5));
			}

			reader.Close();
			base.OnPaint(e);
		}

		private int ConvertToPixel(double d)
		{
			return Convert.ToInt32(Math.Round(d, 2) * m_boxSize);
		}
	}

	public struct MyPoint
	{
		public double X;
		public double Y;

		public static MyPoint Create(double x, double y)
		{
			MyPoint p;
			p.X = x;
			p.Y = y;
			return p;
		}

		public static MyPoint Create(string s)
		{
			MyPoint p;
			string[] coords = s.Split(new Char[] { ',' });
			p.X = Convert.ToDouble(coords[0]);
			p.Y = Convert.ToDouble(coords[1]);
			return p;
		}

		public static double GetDistance(MyPoint p1, MyPoint p2)
		{
			return Math.Sqrt(Math.Pow((p1.X - p2.X), 2) + Math.Pow((p1.Y - p2.Y), 2));
		}

		public override string ToString()
		{
			return string.Format("{0},{1}", X, Y);
		}
	}
}