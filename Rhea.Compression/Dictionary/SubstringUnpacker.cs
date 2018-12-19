// -----------------------------------------------------------------------
//  <copyright file="StringUnpacker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Text;

namespace Rhea.Compression.Dictionary
{

    public class SubstringUnpacker
    {
        private readonly byte[] _dictionary;
        private readonly MemoryStream _buffer = new MemoryStream();

        public SubstringUnpacker(byte[] dictionary)
        {
            _dictionary = dictionary ?? Array.Empty<byte>();
        }

        public void Reset()
        {
            _buffer.SetLength(0);
        }

        public byte[] UncompressedData()
        {
	        return _buffer.ToArray();
        } 

        public void EncodeLiteral(byte aByte)
        {
            _buffer.WriteByte(aByte);
        }

        public void EncodeSubstring(int offset, int length)
        {
            var currentIndex = (int)_buffer.Length;
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
                    _buffer.Write(_dictionary, startDict, endDict - startDict);
                }

                if (end > 0)
                {
                    var bytes = _buffer.GetBuffer();
                    for (int i = 0; i < end; i++)
                    {
                        _buffer.WriteByte(bytes[i]);
                    }
                }
            }
            else
            {
                var bytes = _buffer.GetBuffer();
                for (int i = 0; i < length; i++)
                {
                    _buffer.WriteByte(bytes[i + currentIndex + offset]);
                }
            }
        }
    }

}