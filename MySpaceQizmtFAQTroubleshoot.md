<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Troubleshoot #

## When I try to open a text file in MR.DFS with  "qizmt edit WordCount\_Output.txt" I get an error. ##

---

Currently only support editing job files (the green ones)
But the top of the file can be viewed with the **head** command.<br />
e.g.<br />
`qizmt head WordCount_Output.txt`<br />
`qizmt head WordCount_Output.txt 100`   (to view 100 lines etc.)
<br /><br />
Also, can pull the file out of mr.dfs and view it:<br /><br />
`qizmt get WordCount_Output.txt \\<hostname>\D$\folder\WordCount_Output.txt && notepad \\<hostname>\D$\folder\WordCount_Output.txt`

## Killall does not work in production. ##

---

When there is a permission issue on some production clusters killall may not work. In this case can use an alternative way to killall distributed processes in the cluster:

By the way, to killall on production machine is **qizmt killall proxy –f**  then issue **qizmt health** until it says **100% healthy**

## System.Exception:Insufficient resources for this job on cluster (ZBlock value file size `>` ZFILE\_MAX\_BYTES) (consider increasing sub process count) ##

---

Under **IOSettings** of a grouped mapreduce job can add a **Setting** to increase the number of intermediate blocks so that they are smaller and not too much tries to load at once per core:

e.g.
```
<IOSettings>
  <Setting name="Subprocess_IntermediateBlockPrime" value="499" />
  ...
</IOSettings>
```

The default zblock count is 271, which is how many chunks per core in the cluster a file is broken into for the sort phase.
**Subprocess\_IntermediateBlockPrime** must be set to a prime number.

There is a qizmt command **nearprime** which can be used to get a prime  number near some value, e.g.

D:\>**qizmt nearprime** 500

500 is not prime

499 is nearest prime less than 500

503 is nearest prime greater than 500


Note also, regardless of increasing **Subprocess\_IntermediateBlockPrime** count, all identical keys go into the same zblock. If this is the case, can have the key also include a random number between 0 and 100 so that all identical keys are copped into at least 100 different keys.


In the case that it’s too much going to a single key, here is an example of breaking it up with random number in key. But if it is not too much going into a single key, can just increase the **Subprocess\_IntermediateBlockPrime** so that less keys get loaded at once into the reducers.

```
<KeyLength>*int*,int,double</KeyLength>
  .
  .
  .
  <![CDATA[
  public Random random = new Random();
  public virtual void Map(ByteSlice line, MapOutput output)
  {
    mstring sLine = mstring.Prepare(line);
    int year = sLine.NextItemToInt(',');
    mstring title = sLine.NextItemToString(',');
    double size = sLine.NextItemToDouble(',');
    long pixel = sLine.NextItemToLong(',');
                
    recordset rKey = recordset.Prepare();
    rKey.PutInt(random.Next(100));
    .
    .
    .
  }
  ]]>
 </Map>
 <Reduce>
  <![CDATA[
  public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
  {
    recordset rKey = recordset.Prepare(key);
    int discard_random = rKey.GetInt();
    .
    .
    .
   }
   ]]>
```

## System.Exception: mstring cannot accommodate your string operations.  Please use C# string or StringBuilder instead. ##

---

If doing an extensive amount of appending, then declare a StringBuilder outside of the map() or reduce() and reuse it in-between each
iteration.

Can just use ToString, no need to use mstring.

The exception is related to out of memory, so on your dev machine it will occur sooner, in production it will be able to grow to the max of `byte[]` for all mstrings in the current itteration. Can use qizmt ps to see available memory and any other running jobs that may be using memory.

## It looks like my job is hanging. Is there a way to say what happening to it? ##

---

If there are errors happening, you can see them early with:

`qizmt slaveloglist`

can also clear slave logs with:

`qizmt slavelogdelete`

## Error: Unable to check user logs on ‘SOMEHOST’ ##

---

It looks like there is too much going tout to Qizmt\_Log() this is only for debugging not for transporting gigabytes of data. Can fix by throttling to only let each reduce or mapper log up to a handful of Qizmt\_Log() calls.


Could do something like:
```
public int logfive = 5;
public virtual void Map(ByteSlice line, MapOutput output)
{
  .
  .
  .
  if(error_condition)
  {
    if(logfive > 0)
    {
      Qizmt_Log(someerror);
    }
    --logfive;
  }
```
Or, you can configure qizmt to allow only a maximum number of Qizmt\_Log calls for each job type by:
**qizmt maxuserlogsupdate `<integer>`**


If there are millions of error conditions in the data that you need to list, then don’t use Qizmt\_Log but rather can make a separate mapreducer to find these.
```
public virtual void Map(ByteSlice line, MapOutput output)
  {
    .
    .
    .
    if(error_condition)
    {
      output.add(… some error in data
    }
```

## Why am I getting "access denied" errors accessing resources from a job even though permission has been granted? ##

---

If permissions have been modified on the account that Qizmt was installed under, a Qizmt killall command will need to be issued to update the access token.