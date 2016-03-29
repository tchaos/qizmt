<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Qizmt Tutorial](MySpaceQizmtTutorial.md) or <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md)



# Cluster Assembly Cache (CAC) #
.Net DLLs may be made accessible to mapreduce, local and remote jobs by simply putting them into MR.DFS then referencing them from the jobs. `<file>.DLLs` put into MR.DFS is redundantly copied to every machine in the cluster.

<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_CAC.png' alt='CAC' />


### Referencing a Custom .Net DLL in a Mapreducer ###
If you want to build a C# DLL and reference it, you can build a regular .net DLL and use Qizmt put to put it into MR.DFS then reference it in the job. Qizmt put automatically treats `<file>.dll` files differently as other files and makes a redundant copy of the DLL on every machine in the cluster. After putting a DLL into a Qizmt cluster, it can be referenced from within the job:

```
      <Job Name="testdll" Custodian="" email="">
      <Add Reference="Qizmt-AddUsingReferences_testdll.dll" Type="dfs" /> <!- custom C# DLL put into MR.DFS ->
      <Add Reference="Microsoft.VisualBasic.dll" Type="system" /> <!-- system DLL already in .NET -->
      <Using>testdll</Using> <!-- using namespace -->
      <Using>Microsoft.VisualBasic</Using><!-- using namespace -->
       .
       .
       .
```


`*Qizmt put <name>.dll*` puts a dll into MR.DFS and makes it available to all job types; local, remote and mapreduce. There are two reference types supported **dfs** and **system**.

Can use type **dfs** for custom DLLs or DLLs that are referenced by custom DLLs used in local, remote or mapreducer jobs.

Can use type **system** for referencing system DLLs that are not referenced by default. Here are the system DLLs that are referenced by default:
  * System.dll
  * System.Xml.dll
  * System.Data.dll
  * System.Drawing.dll
  * System.Core.dll

e.g.
```
      <Add Reference="regression_test_testdll.dll" Type="dfs" />
      <Add Reference="Microsoft.VisualBasic.dll" Type="system" />
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              if(32 != Microsoft.VisualBasic.Strings.Asc(' '))
              {
                  throw new Exception("Map: (32 != Microsoft.VisualBasic.Strings.Asc(' '))");
              }
```
