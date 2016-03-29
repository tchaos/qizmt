<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `BytesToLong` #
`public static Int64 BytesToLong(IList<byte> x)`

Converts bytes, which are in big endian byte ordering, to long.

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    Int64 num = Entry.BytesToLong(key.ToBytes());
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static long BytesToLong(IList<byte> x, int offset)`

Converts bytes, which are in big endian byte ordering, starting at offset to long.

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    Int64 num = Entry.BytesToLong(key.ToBytes(), 0);
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```