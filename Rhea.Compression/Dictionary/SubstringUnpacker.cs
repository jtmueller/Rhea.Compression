// -----------------------------------------------------------------------
//  <copyright file="StringUnpacker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using Collections.Pooled;

namespace Rhea.Compression.Dictionary
{
    public class SubstringUnpacker : IDisposable
    {
        private readonly ReadOnlyMemory<byte> _dictionary;
        private readonly PooledList<byte> _buffer = new PooledList<byte>();

        public SubstringUnpacker(ReadOnlyMemory<byte> dictionary)
        {
            _dictionary = dictionary;
        }

        public void Reset()
        {
            _buffer.Clear();
            _buffer.TrimExcess();
        }

        public Span<byte> UncompressedData()
        {
            return _buffer.Span;
        } 

        public void EncodeLiteral(byte aByte)
        {
            _buffer.Add(aByte);
        }

        public void EncodeSubstring(int offset, int length)
        {
            var currentIndex = _buffer.Count;
            if (currentIndex + offset < 0)
            {
                int startDict = currentIndex + offset + _dictionary.Length;
                int endDict = startDict + length;
                int end = 0;

                if (endDict > _dictionary.Length)
                {
                    end = endDict - _dictionary.Length;
                    endDict = _dictionary.Length;
                }

                if (endDict - startDict > 0)
                {
                    _buffer.AddRange(_dictionary.Span.Slice(startDict, endDict - startDict));
                }

                if (end > 0)
                {
                    var bytes = _buffer.Span;
                    for (int i = 0; i < end; i++)
                    {
                        _buffer.Add(bytes[i]);
                    }
                }
            }
            else
            {
                var bytes = _buffer.Span;
                for (int i = 0; i < length; i++)
                {
                    _buffer.Add(bytes[i + currentIndex + offset]);
                }
            }
        }

        public void Dispose()
        {
            _buffer.Dispose();
        }
    }

}