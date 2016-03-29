<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Methods of `ReduceEntry` #



## `CopyKey` ##
`public void CopyKey(byte[] buffer)`

Copies this key into buffer.

#### Example ####
```
byte[] myBuffer = new byte[Qizmt_KeyLength];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{

    ReduceEntry entry = values[0];

    //This is equivalent to:
    //key.CopyTo(myBuffer);
    entry.CopyKey(myBuffer);

    //Code to examine the first byte of key.
    if (myBuffer[0] == (byte)'a')
    {
        //Do something...

    }
    else
    {
        //Do something else...

    }
} 
```

---




`public void CopyKey(byte[] buffer, int bufferOffset)`

Copies this key into buffer starting at bufferOffset.

#### Example ####
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    ReduceEntry entry = values[0];

    //This is equivalent to:
    //key.CopyTo(myBuffer);
    entry.CopyKey(myBuffer, 0);

    //Code to examine the first byte of key.
    if (myBuffer[0] == (byte)'a')
    {
        //Do something...

    }
    else
    {
        //Do something else...

    }
} 
```

---




## `AppendKey` ##
`public void AppendKey(List<byte> list)`

Appends this key to list.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    ReduceEntry entry = values[0];

    myBuffer.Clear();

    //This is equivalent to:
    //key.AppendTo(myBuffer);
    entry.AppendKey(myBuffer);

    //Code to examine the first byte of key.
    if (myBuffer[0] == 0x61)
    {
        //Do something...
        output.Add(ByteSlice.Prepare("xxx"));
    }
    else
    {
        //Do something else...

    }
} 
```

---




## `CopyValue` ##
`public void CopyValue(byte[] buffer)`

Copies this value into buffer.

#### Example ####
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{

    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        entry.CopyValue(myBuffer);

        if (myBuffer[0] == 0x61)
        {
            //Do something...
        }
        else
        {
            //Do something else...
        }
    }

} 
```

---




`public void CopyValue(byte[] buffer, int bufferOffset)`

Copies this value into buffer starting at bufferOffset.

#### Example ####
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{

    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        entry.CopyValue(myBuffer, 0);

        if (myBuffer[0] == 0x61)
        {
            //Do something...
        }
        else
        {
            //Do something else...
        }
    }

} 
```

---




## `AppendValue` ##
`public void AppendValue(List<byte> list)`

Appends this value to a byte list.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{

    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        myBuffer.Clear();
        entry.AppendValue(myBuffer);

        if (myBuffer[0] == 0x61)
        {
            //Do something...
            output.Add(ByteSlice.Prepare("xxx"));
        }
        else
        {
            //Do something else...
        }
    }

} 
```