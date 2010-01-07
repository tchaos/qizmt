using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_ADONET_RegressionTests
{
    public partial class Program
    {
        public static void Main(string[] args)
        {
            List<KeyValuePair<string, bool>> AllTests = new List<KeyValuePair<string, bool>>();
            
            if (args.Length > 0 && "-rindexmutable" == args[0])
            {
                goto rindexmutable;
            }
            if (args.Length > 0 && "-connopen" == args[0])
            {
                goto connopen;
            }
            if (args.Length > 0 && "-selectportion" == args[0])
            {
                goto selectportion;
            }
            if (args.Length > 0 && "-rselectwhereor" == args[0])
            {
                goto rselectwhereor;
            }
            if (args.Length > 0 && "-rindex" == args[0])
            {
                goto rindex;
            }            
            if (args.Length > 0 && "-rindexotherdatatypes" == args[0])
            {
                goto rindexotherdatatypes;
            }
            if (args.Length > 0 && "-rindexpinmemoryhash" == args[0])
            {
                goto rindexpinmemoryhash;
            }
            if (args.Length > 0 && "-rindexonintpinhash" == args[0])
            {
                goto rindexonintpinhash;
            }
            if (args.Length > 0 && "-selnestedfuncs" == args[0])
            {
                goto selnestedfuncs;
            }
            if (args.Length > 0 && "-innerjoin" == args[0])
            {
                goto innerjoin;
            }
            if (args.Length > 0 && "-outerjoin" == args[0])
            {
                goto outerjoin;
            }
            if (args.Length > 0 && "-insertintoselecttop" == args[0])
            {
                goto insertintoselecttop;
            }
            if(!(args.Length > 0 && "-skipaggregators" == args[0]))
            {
                string tablename = DbAggregators_CreateTable();
                
                  {
                      string testname = "DbAggregators_AVG";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_AVG(tablename);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }

                  {
                      string testname = "DbAggregators_STD";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_STD(tablename, false);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }

                  {
                      string testname = "DbAggregators_STD_SAMP";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_STD(tablename, true);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }                 
              
                  {
                      string testname = "DbAggregators_VAR_POP";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_VAR(tablename, false);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }

                  {
                      string testname = "DbAggregators_VAR_SAMP";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_VAR(tablename, true);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }                

                  {
                      string testname = "DbAggregators_COUNT";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_COUNT(tablename);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }
                
                  {
                      string testname = "DbAggregators_COUNTDISTINCT";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_COUNTDISTINCT(tablename);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }                

                  {
                      string testname = "DbAggregators_FIRST";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_FIRST(tablename);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }                

                  {
                      string testname = "DbAggregators_LAST";
                      Console.WriteLine("*** Running test {0}...", testname);
                      try
                      {
                          DbAggregators_LAST(tablename);
                          Console.WriteLine("[PASSED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                      }
                      catch (Exception e)
                      {
                          Console.Error.WriteLine(e.ToString());
                          Console.WriteLine("[FAILED] - {0}", testname);
                          AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                      }
                      Console.WriteLine();
                  }                  

                {
                    string testname = "DbAggregators_MAX";
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        DbAggregators_MAX(tablename);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }                 

                {
                    string testname = "DbAggregators_MIN";
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        DbAggregators_MIN(tablename);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }

                {
                    string testname = "DbAggregators_SUM";
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        DbAggregators_SUM(tablename);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }
                
                {
                    string testname = "DbAggregators_CHOOSERND";
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        DbAggregators_CHOOSERND(tablename);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }


                {
                    string testname = "DbAggregators_BIT_AND";
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        DbAggregators_BIT_AND(tablename);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }

                {
                    string testname = "DbAggregators_BIT_OR";
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        DbAggregators_BIT_OR(tablename);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }

                {
                    string testname = "DbAggregators_BIT_XOR";
                    Console.WriteLine("*** Running test {0}...", testname);
                    try
                    {
                        DbAggregators_BIT_XOR(tablename);
                        Console.WriteLine("[PASSED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.ToString());
                        Console.WriteLine("[FAILED] - {0}", testname);
                        AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                    }
                    Console.WriteLine();
                }

                DbAggregators_DropTable(tablename);
            }

            {
                string testname = "ShellCmdLossless";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    ShellCmdLossless();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "SelectDistinct";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    SelectDistinct();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            rindex:
            {
                string testname = "RIndexPin";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexPin();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "RIndexPooled";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexPooled();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "RSelect";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RSelect();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            rselectwhereor:
            {
                // first: QueryAnalyzer_ADONET_RegressionTests.exe -rselectwhereor SavedTable quitrswo
                // next:  QueryAnalyzer_ADONET_RegressionTests.exe -rselectwhereor SavedTable NoCreateTable quitrswo
                // last:  QueryAnalyzer_ADONET_RegressionTests.exe -rselectwhereor NoCreateTable quitrswo
                string testname = "RSelectWhereOR";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    bool SavedTable = false;
                    bool NoCreateTable = false;
                    bool quitrswo = false;
                    foreach (string arg in args)
                    {
                        if (-1 != arg.IndexOf("SavedTable", StringComparison.OrdinalIgnoreCase))
                        {
                            SavedTable = true;
                            Console.WriteLine("SavedTable");
                        }
                        if (-1 != arg.IndexOf("NoCreateTable", StringComparison.OrdinalIgnoreCase))
                        {
                            NoCreateTable = true;
                            Console.WriteLine("NoCreateTable");
                        }
                        if (-1 != arg.IndexOf("quitrswo", StringComparison.OrdinalIgnoreCase))
                        {
                            quitrswo = true;
                        }
                    }
                    if (quitrswo)
                    {
                        System.Threading.Thread.Sleep(1000 * 12);
                    }
                    try
                    {
                        RSelectWhereOR(SavedTable, NoCreateTable);
                    }
                    catch
                    {
                        if (quitrswo)
                        {
                            return;
                        }
                        throw;
                    }
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                    if (quitrswo)
                    {
                        return;
                    }
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "RSelectLotsOfRows";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RSelectLotsOfRows();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "BatchingOff";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    BatchingOff();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            connopen:
            {
                string testname = "ConnOpen";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    ConnOpen();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "ConnClose";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    ConnClose();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "BatchingOn";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    BatchingOn();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "CmdExecuteNonQuery";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    CmdExecuteNonQuery();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "CmdExecuteReader";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    CmdExecuteReader();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "AlterTableRenameSwap";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    AlterTableRenameSwap();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "Aggregate GROUP BY";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    AggregateGroupBy();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            selnestedfuncs:
            {
                string testname = "SELECT Nested Functions";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    SelectNestedFunctions();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            outerjoin:
            {
                string testname = "OUTER JOIN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    OuterJoin();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            innerjoin:
            {
                string testname = "INNER JOIN";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    InnerJoin();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "Drop";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    Drop();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "PinFor";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    PinFor();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "Param";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    Param();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "NullParam";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    NullParam();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "Truncate";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    Truncate();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "Update";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    Update();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "Delete";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    Delete();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            selectportion:
            {
                string testname = "SelectPortion";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    SelectPortion();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            insertintoselecttop:
            {
                string testname = "InsertIntoSelectTop";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    InsertIntoSelectTop();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "RIndexType";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexType();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            rindexotherdatatypes:
            {
                string testname = "RIndexOnLong";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnLong();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOnDateTime";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnDateTime();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOnDouble";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnDouble();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOnInt";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnInt();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOnChar";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnChar();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            rindexpinmemoryhash:
            {
                string testname = "RIndexOnLongPinHash";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnLongPinHash();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOnDateTimePinHash";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnDateTimePinHash();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOnDoublePinHash";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnDoublePinHash();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }            
            {
                string testname = "RIndexOnCharPinHash";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnCharPinHash();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
           
            {
                string testname = "RIndexKeepValueOrder";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexKeepValueOrder();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "RIndexAutoBatch";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexAutoBatch();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "RSelectSpillageTest1";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RSelectSpillageTest1();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "RSelectSpillageTest2";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RSelectSpillageTest2();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            {
                string testname = "RIndexFailover";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexFailover();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexReplication";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexReplication();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }            

            rindexonintpinhash:
            {
                string testname = "RIndexOnIntPinHash";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOnIntPinHash();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }

            rindexmutable:
            {
                string testname = "RIndexMutable";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexMutable();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexMutable_SpecialCase_1(keepvalueorder)";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexMutable_SpecialCase_1(true);
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexMutable_SpecialCase_1()";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexMutable_SpecialCase_1(false);
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexMutable_SpecialCase_2(keepvalueorder)";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexMutable_SpecialCase_2(true);
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexMutable_SpecialCase_2()";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexMutable_SpecialCase_2(false);
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexMutable_SpecialCase_3()";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexMutable_SpecialCase_3();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOutlierDelete";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOutlierDelete();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOutlierFifo";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOutlierFifo();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            {
                string testname = "RIndexOutlierRandom";
                Console.WriteLine("*** Running test {0}...", testname);
                try
                {
                    RIndexOutlierRandom();
                    Console.WriteLine("[PASSED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, true));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.ToString());
                    Console.WriteLine("[FAILED] - {0}", testname);
                    AllTests.Add(new KeyValuePair<string, bool>(testname, false));
                }
                Console.WriteLine();
            }
            
            // - Display final output -
            Console.WriteLine("--STARTRESULTS--");
            foreach (KeyValuePair<string, bool> test in AllTests)
            {
                DSpace_LogResult(test.Key, test.Value);
            }
            Console.WriteLine("--ENDRESULTS--");

#if DEBUG
            //Console.Read();
#endif
        }

        public static void DSpace_LogResult(string name, bool passed)
        {
            if (passed)
            {
                Console.Write("[");
                ConsoleColor fc = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.Write("PASSED");
                Console.ForegroundColor = fc;
                Console.WriteLine("] - " + name);
            }
            else
            {
                Console.Write("[");
                ConsoleColor fc = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write("FAILED");
                Console.ForegroundColor = fc;
                Console.WriteLine("] - " + name);
            }
        }
    }
}
