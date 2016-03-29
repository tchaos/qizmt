<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `ToString` #
`public override string ToString()`

Returns a C# string.
### Remarks ###
This results a heap allocation.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);
    Qizmt_Log(ms.ToString());
} 
```