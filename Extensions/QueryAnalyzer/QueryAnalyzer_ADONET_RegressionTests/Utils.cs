using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_ADONET_RegressionTests
{
    class Utils
    {
        public static bool IsEqual(double x, double y)
        {
            if (double.IsInfinity(x) || double.IsNaN(x) || double.IsNegativeInfinity(x) || double.IsPositiveInfinity(x) ||
                double.IsInfinity(y) || double.IsNaN(y) || double.IsNegativeInfinity(y) || double.IsPositiveInfinity(y))
            {
                return x.CompareTo(y) == 0;
            }
            else
            {
                return Math.Abs(x - y) < 0.0000000001;
            }
        }
    }
}
