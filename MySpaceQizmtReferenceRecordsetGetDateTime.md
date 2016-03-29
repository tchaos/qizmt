<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `GetDateTime` #
`public DateTime GetDateTime()`

Get the next DateTime item from the recordset.

### Example ###
```
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    recordset rkey = recordset.Prepare(key);
    DateTime dt = rkey.GetDateTime();
    output.Add(mstring.Prepare(dt.ToString("yyyy-MM-dd hh:mm:ss")));
} 
```