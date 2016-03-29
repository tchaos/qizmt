<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `BytesToInt` #
`public static Int32 BytesToInt(IList<byte> x)`

Converts bytes, which are in big endian byte ordering, to int.

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    int num = Entry.BytesToInt(key.ToBytes());
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static Int32 BytesToInt(IList<byte> x, int offset)`

Converts bytes, which is in big endian byte ordering, starting at offset to int.

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    int num = Entry.BytesToInt(key.ToBytes(), 0);
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```