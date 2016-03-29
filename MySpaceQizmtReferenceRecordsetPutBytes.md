<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `PutBytes` #
`public void PutBytes(IList<byte> bytes, int byteIndex, int byteCount)`

Put bytes into the recordset, starting at byteIndex, with byteCount number of bytes to put.

### Example ###
```
byte[] buf = new byte[4];
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    int x = sLine.NextItemToInt(',');
    Entry.ToBytes(x, buf, 0);
    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutBytes(buf, 0, 2);

    output.Add(rKey, rValue);
} 
```