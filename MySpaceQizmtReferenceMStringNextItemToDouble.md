<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `NextItemToDouble` #
`public double NextItemToDouble(char delimiter)`

Get the next double item, delimited by the char delimiter.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring sCoords = sLine.MSubstring(2);

    double x = sCoords.NextItemToDouble(',');
    double y = sCoords.NextItemToDouble(',');

    double kx = x * (double)5;
    double ky = y * (double)5;

    kx = Math.Truncate(kx);
    ky = Math.Truncate(ky);

    mstring sKey = mstring.Prepare(kx);
    sKey = sKey.MAppend(',').MAppend(ky);

    output.Add(sKey, sLine);
} 
```