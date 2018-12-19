// -----------------------------------------------------------------------
//  <copyright file="PrefixHash.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;

namespace Rhea.Compression.Dictionary
{
    public class PrefixHash
    {
        public static int PrefixLength = 4;

#if NETSTANDARD2_1 || NETCOREAPP2_1
        private readonly ReadOnlyMemory<byte> _buffer;
#else
        private readonly byte[] _buffer;
#endif
        private readonly int[] _hash;
        private readonly int[] _heap;

#if NETSTANDARD2_1 || NETCOREAPP2_1
        public PrefixHash(ReadOnlyMemory<byte> buf, bool addToHash)
#else
        public PrefixHash(byte[] buf, bool addToHash)
#endif
        {
            _buffer = buf;
            _hash = new int[(int)(1.75 * buf.Length)];
            for (int i = 0; i < _hash.Length; i++)
            {
                _hash[i] = -1;
            }
            _heap = new int[buf.Length];
            for (int i = 0; i < _heap.Length; i++)
            {
                _heap[i] = -1;
            }
            if (addToHash)
            {
                for (int i = 0, count = buf.Length - PrefixLength; i < count; i++)
                {
                    Put(i);
                }
            }
        }

        public void DumpState(TextWriter output)
        {
            output.WriteLine("Hash:");
            for (int i = 0; i < _hash.Length; i++)
            {
                if (_hash[i] == -1)
                    continue;
                output.WriteLine("hash[{0,3}] = {1,3};", i, _hash[i]);
            }

            output.WriteLine("Heap:");
            for (int i = 0; i < _heap.Length; i++)
            {
                if (_heap[i] == -1)
                    continue;
                output.WriteLine("heap[{0,3}] = {1,3};", i, _heap[i]);
            }
        }

#if NETSTANDARD2_1 || NETCOREAPP2_1
        private int HashIndex(ReadOnlySpan<byte> buf, int i)
#else
        private int HashIndex(byte[] buf, int i)
#endif
        {
            int code = (buf[i] & 0xff) | ((buf[i + 1] & 0xff) << 8) | ((buf[i + 2] & 0xff) << 16) |
                       ((buf[i + 3] & 0xff) << 24);
            return (code & 0x7fffff) % _hash.Length;
        }

        public void Put(int index)
        {
#if NETSTANDARD2_1 || NETCOREAPP2_1
            int hi = HashIndex(_buffer.Span, index);
#else
            int hi = HashIndex(_buffer, index);
#endif
            _heap[index] = _hash[hi];
            _hash[hi] = index;
        }

#if NETSTANDARD2_1 || NETCOREAPP2_1
        public Match GetBestMatch(int index, ReadOnlySpan<byte> targetBuf)
#else
        public Match GetBestMatch(int index, byte[] targetBuf)
#endif
        {
            int bestMatchIndex = 0;
            int bestMatchLength = 0;

            int bufLen = _buffer.Length;

            if (bufLen == 0)
            {
                return new Match(0, 0);
            }

#if NETSTANDARD2_1 || NETCOREAPP2_1
            ReadOnlySpan<byte> bufferBytes = _buffer.Span;
#else
            byte[] bufferBytes = _buffer;
#endif
            int targetBufLen = targetBuf.Length;

            int maxLimit = Math.Min(255, targetBufLen - index);

            int targetHashIndex = HashIndex(targetBuf, index);
            int candidateIndex = _hash[targetHashIndex];
            while (candidateIndex >= 0)
            {
                int distance;
#if NETSTANDARD2_1 || NETCOREAPP2_1
                if (targetBuf.Length != bufferBytes.Length || !targetBuf.SequenceEqual(bufferBytes))
#else
                if (targetBuf != bufferBytes)
#endif
                {
                    distance = index + bufLen - candidateIndex;
                }
                else
                {
                    distance = index - candidateIndex;
                }
                if (distance > (2 << 15) - 1)
                {
                    // Since we are iterating over nearest offsets first, once we pass 64k
                    // we know the rest are over 64k too.
                    break;
                }

                int maxMatchJ = index + Math.Min(maxLimit, bufLen - candidateIndex);
                int j, k;
                for (j = index, k = candidateIndex; j < maxMatchJ; j++, k++)
                {
                    if (bufferBytes[k] != targetBuf[j])
                    {
                        break;
                    }
                }

                int matchLength = j - index;
                if (matchLength > bestMatchLength)
                {
                    bestMatchIndex = candidateIndex;
                    bestMatchLength = matchLength;
                }
                candidateIndex = _heap[candidateIndex];
            }

            return new Match(bestMatchIndex, bestMatchLength);
        }

        public readonly struct Match
        {
            public readonly int BestMatchIndex;
            public readonly int BestMatchLength;

            public Match(int bestMatchIndex, int bestMatchLength)
            {
                BestMatchIndex = bestMatchIndex;
                BestMatchLength = bestMatchLength;
            }
        }
    }
}