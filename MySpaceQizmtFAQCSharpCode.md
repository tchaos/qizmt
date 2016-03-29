<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - C# Code #

## How do I get a C# function that accessible to a Qizmt mapreducer? ##

---

There are a few ways:
  1. The C# function, struct or class declaration after “<![CDATA[“ and before  in map() or reduce() clause that will use it.<br />This will allow for debugging the functionality while it is being executed by the mapreducer.<br /><br />
  1. Put `*`.dll – will put a DLL into MR.DFS and reference it from a mapreducer job with.<br />e.g.<br />
```
<Job …><Add Reference="regression_test_testdll.dll" Type="dfs" />
```

## How to put my own C# DLL available to Qizmt jobs? ##

---

If you want to build a C# DLL and reference it, you can build a regular C# dll and use Qizmt put to put it into MR.DFS then reference it in
the job. Qizmt put automatically treats `*`.dll files differently as other files and makes a redundant copy of the DLL on every machine in the
cluster. After putting a DLL into a Qizmt cluster , it can be referenced from within the job:
```
<Job Name="testdll" Custodian="" email="">
  <Add Reference="Qizmt-AddUsingReferences_testdll.dll" Type="dfs" /> <!- custom C# DLL put into MR.DFS ->
  <Add Reference="Microsoft.VisualBasic.dll" Type="system" /> <!-- system DLL already in .NET -->
  <Using>testdll</Using> <!-- using namespace -->
  <Using>Microsoft.VisualBasic</Using><!-- using namespace -->
  .
  .
  .
</Job>
```

``qizmt put `*`.dll`` puts a dll into MR.DFS and makes it available to all job types; local, remote and mapreduce. There are two reference
types supported **dfs** and **system**.

Can use type **dfs** for custom dlls or dlls that are referenced by custom dlls used in local, remote or mapreducer jobs.

Can use type **system** for referencing system dlls that are not referenced by default. Here are the system dlls that are referenced by

default:

> System.dll<br />
> System.Xml.dll<br />
> System.Data.dll<br />
> System.Drawing.dll<br />
> System.Core.dll<br />

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
    //...
  }
  ]]>
```

Also, dlls put into MR.DFS may reference each other.

When a dll is put into MR.DFS it is copied to every machine in the cluster so that all job types can load it locally.


## Is there a global namespace?  I.e., if I want to define a field delimiter constant and make it available to a 'remote' job and both members of a 'mapreduce' job, how would I do this? ##

---

Global can be created and muted with DGlobals.Add(“`<name>`”, “`<value>`”); in local jobs only. And are read only in subsequent remote and mapreduce jobs.


Another option for sharing information between jobs which can be used to share mutable files between all job types is to use a global critical section, however this is a critical section shared by all processes in the cluster so should be used sparingly, e.g. on first map iteration, reduceinitialize and reducefinalize.


There is example in built in examples: Qizmt-ClusterLock.xml
```
using(GlobalCriticalSection.GetLock())
{  
  //cluster wide critical section
}
```

## I have a need to use a binary tree for lookups in the map portion of a mapreduce job.  Is it possible to reference external DLLs in Qizmt?  It would be for read-only purposes, and could thus be a global variable. ##

---

Yes, this is in Quck  Start Guide. There is also example in qizmt examples command: Qizmt-AddUsingReferences.xml


**Referencing a Custom .Net DLL in a Mapreducer**

If you want to build a C# DLL and reference it, you can build a regular .net DLL and use **Qizmt put** to put it into MR.DFS then reference it in the job. Qizmt put automatically treats `*`.dll files differently as other files and makes a redundant copy of the DLL on every machine in the cluster. After putting a DLL into a Qizmt cluster, it can be referenced from within the job:

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


**qizmt put `*`.dll** puts a dll into MR.DFS and makes it available to all job types; local, remote and mapreduce. There are two reference types supported **dfs** and **system**.


Can use type **dfs** for custom DLLs or DLLs that are referenced by custom DLLs used in local, remote or mapreducer jobs.
Can use type **system** for referencing system DLLs that are not referenced by default. Here are the system DLLs that are referenced by default:

> System.dll
> System.Xml.dll
> System.Data.dll
> System.Drawing.dll
> System.Core.dll


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
    //...
  }
  ]]>
```