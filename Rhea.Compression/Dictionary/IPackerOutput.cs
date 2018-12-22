using System;

namespace Rhea.Compression.Dictionary
{
    public interface IPackerOutput
    {
        void EncodeLiteral(byte aByte, object? context = null);
        void EncodeSubstring(int offset, int length, object? context = null);
        void EndEncoding(object? context = null);
    }
}