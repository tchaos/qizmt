<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `ToByteSlice` #
`public ByteSlice ToByteSlice()`

Converts the buffer of mstring to UTF-8 and returns a new instance of `ByteSlice`.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);
    mstring sKey = ms.MSubstring(0, 2);
    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```