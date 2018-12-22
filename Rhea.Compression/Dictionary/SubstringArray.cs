/**
 *   Copyright 2011 Garrick Toubassi
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

using System;
using System.Buffers;

namespace Rhea.Compression.Dictionary
{
    public class SubstringArray
    {
        private static ArrayPool<int> s_pool = ArrayPool<int>.Shared;

        private int _capacity;
        private int[] _indexes;
        private int[] _lengths;
        private int[] _scores;
        private int _size;

        public SubstringArray(int capacity)
        {
            _capacity = capacity;
            _indexes = new int[capacity];
            _lengths = new int[capacity];
            _scores = new int[capacity];
        }

        public int Size => _size;

        public void Sort()
        {
            var histogramBuffer = s_pool.Rent(256);
            var histogram = histogramBuffer.AsSpan(0, 256);
            var working = new SubstringArray(_size);

            for (int bitOffset = 0; bitOffset <= 24; bitOffset += 8)
            {
                if (bitOffset > 0)
                {
                    for (int j = 0; j < histogram.Length; j++)
                    {
                        histogram[j] = 0;
                    }
                }
                int i, count, rollingSum;
                for (i = 0, count = _size; i < count; i++)
                {
                    int sortValue = _scores[i];
                    int sortByte = (sortValue >> bitOffset) & 0xff;
                    histogram[sortByte]++;
                }

                for (i = 0, count = histogram.Length, rollingSum = 0; i < count; i++)
                {
                    int tmp = histogram[i];
                    histogram[i] = rollingSum;
                    rollingSum += tmp;
                }

                for (i = 0, count = _size; i < count; i++)
                {
                    int sortValue = _scores[i];
                    int sortByte = (sortValue >> bitOffset) & 0xff;
                    int newOffset = histogram[sortByte]++;
                    working.SetScore(newOffset, _indexes[i], _lengths[i], _scores[i]);
                }

                // swap (brain transplant) innards
                int[] t = working._indexes;
                working._indexes = _indexes;
                _indexes = t;

                t = working._lengths;
                working._lengths = _lengths;
                _lengths = t;

                t = working._scores;
                working._scores = _scores;
                _scores = t;

                _size = working._size;
                working._size = 0;

                i = working._capacity;
                working._capacity = _capacity;
                _capacity = i;
            }
            s_pool.Return(histogramBuffer);
        }


        public int Add(int index, int length, int count)
        {
            return SetScore(_size, index, length, ComputeScore(length, count));
        }

        public int SetScore(int i, int index, int length, int score)
        {
            if (i >= _capacity)
            {
                int growBy = (((i - _capacity) / (8 * 1024)) + 1) * 8 * 1024;
                // Since this array is going to be VERY big, don't double.        

                var newindex = new int[_indexes.Length + growBy];
                Array.Copy(_indexes, 0, newindex, 0, _indexes.Length);
                _indexes = newindex;

                var newlength = new int[_lengths.Length + growBy];
                Array.Copy(_lengths, 0, newlength, 0, _lengths.Length);
                _lengths = newlength;

                var newscores = new int[_scores.Length + growBy];
                Array.Copy(_scores, 0, newscores, 0, _scores.Length);
                _scores = newscores;

                _capacity = _indexes.Length;
            }

            _indexes[i] = index;
            _lengths[i] = length;
            _scores[i] = score;

            _size = Math.Max(i + 1, _size);

            return i;
        }

        public void Remove(int i)
        {
            Array.Copy(_indexes, i + 1, _indexes, i, _size - i - 1);
            Array.Copy(_lengths, i + 1, _lengths, i, _size - i - 1);
            Array.Copy(_scores, i + 1, _scores, i, _size - i - 1);
            _size--;
        }

        public int Index(int i)
        {
            return _indexes[i];
        }

        public int Length(int i)
        {
            return _lengths[i];
        }

        public int Score(int i)
        {
            return _scores[i];
        }

        public int IndexOf(int s1, SubstringArray sa, int s2, byte[] s, int[] prefixes)
        {
            int index1 = _indexes[s1];
            int length1 = _lengths[s1];
            int index2 = sa._indexes[s2];
            int length2 = sa._lengths[s2];

            for (int i = prefixes[index1], n = prefixes[index1] + length1 - length2 + 1; i < n; i++)
            {
                bool found = true;
                for (int j = prefixes[index2], nj = prefixes[index2] + length2, i1 = i; j < nj; j++, i1++)
                {
                    if (s[i1] != s[j])
                    {
                        found = false;
                        break;
                    }
                }
                if (found)
                {
                    return i;
                }
            }
            return -1;
        }

        /*
     * Substring of length n occurring m times.  We will reduce output by n*m characters, and add 3*m offsets/lengths.  So net benefit is (n - 3)*m.
     * Costs n characters to include in the compression dictionary, so compute a "per character consumed in the compression dictionary" benefit.
     * score = m*(n-3)/n
     */

        private int ComputeScore(int length, int count)
        {
            if (length <= 3)
            {
                return 0;
            }
            return (100 * count * (length - 3)) / length;
        }
    }
}