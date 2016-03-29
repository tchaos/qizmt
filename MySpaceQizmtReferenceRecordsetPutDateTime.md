<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `PutDateTime` #
`public void PutDateTime(DateTime x)`

Put the DateTime item x into the recordset.
### Remarks ###
The order of putting and getting items from the recordset must be the same.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    DateTime dt = DateTime.Parse(sLine);
    recordset key = recordset.Prepare();
    key.PutDateTime(dt);
    output.Add(key, recordset.Prepare());
} 
```