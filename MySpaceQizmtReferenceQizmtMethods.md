<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Qizmt`_``*` Methods #


## `Qizmt_Log` ##
`public static void Qizmt_Log(string msg)`

Displays a message line to the console of the machine that executed.
#### Remarks ####
This is helpful for debugging purposes.  Logs are displayed after the job is completed, not real time.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    Qizmt_Log("key=" + sKey);
} 
```


<img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Log.png' alt='Qizmt Log' />