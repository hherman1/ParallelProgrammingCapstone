// This code is part of the 15418 course project: Implementation and Comparison
// of Parallel LZ77 and LZ78 Algorithms and DCC 2013 paper: Practical Parallel
// Lempel-Ziv Factorization appearing in
// Copyright (c) 2012 Fuyao Zhao, Julian Shun
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <cstdio>
#include <iostream>

using namespace std;

//this module is share by others
//not test for n < 8

typedef intT long;

pair< pair<intT, intT>*, intT> ParallelLPFtoLZ(intT *lpf, intT *prev_occ, intT n) {
  intT l2 = cflog2(n);
  intT depth = l2 + 1;
  int nn = 1 << l2;
  intT *pointers = new intT[n];

  intT *flag = new intT[n + 1];

  parallel_for (intT i = 0; i < n; i++) {
    flag[i] = 0;
    pointers[i] = min(n, i + max<intT>(lpf[i], 1));
  }
  flag[n] = 0;

  l2 = max<intT>(l2, 256);
  intT sn = (n + l2 - 1) / l2;

  intT *next = new intT[sn + 1], *next2 = new intT[sn + 1];
  intT *sflag = new intT[sn + 1];

  //build the sub tree
  // This explores the pointer array and if any block lands on any other block it stores that.. if it doesn't it stores that too.
  parallel_for (intT i = 0; i < sn; i ++) {
    intT j;
    for (j = pointers[i * l2]; j % l2 && j != n; j = pointers[j]) ;
//    j followed the pointers either all the way to the end or it hit the end of its block on the nose
    if (j == n) next[i] = sn; // If it hit the end then next[i] is the number of blocks -- next.len() - 1
    else next[i] = j / l2; // otherwise next[i] is j / log(n) .. in this case we know j is divisible by l2 by our luck -- this will be the block
    // j stumbled upon the start of.
    sflag[i] = 0; // This is irrelevant
  }

  next[sn] = next2[sn] = sn;
  sflag[0] = 1; sflag[sn] = 0;

  //point jump
  intT dep = getDepth(sn); ; // If we build a binary tree out of next, next2, or sflag, how many layers would it have?
  for (intT d = 0; d < dep; d++) {
    parallel_for(intT i = 0; i < sn; i ++) {
      intT j = next[i]; // this is a pointer to another block in the next array - very easily could be the last block.
      if (sflag[i] == 1) { // if this block is marked to be included, mark the block it points to as included also.. race condition?
        sflag[j] = 1;
      }
      next2[i] = next[j]; // next2 points to the block the block this block points to points to.. its the next step in the process
    }
    std::swap(next, next2); // Advance everything in next to the block it points to
  }

  //filling the result
  parallel_for (intT i = 0; i < n; i += l2) {
    if (sflag[i / l2]) {
      flag[i] = 1;
      for (intT j = pointers[i]; j % l2 && j != n; j = pointers[j]) {
        flag[j] = 1;
      }
    }
  }
  delete sflag; delete next; delete next2; delete pointers;

  sequence::scan(flag, flag, n + 1, utils::addF<intT>(), (intT)0);

  intT m = flag[n];
  pair<intT, intT> *lz = new pair<intT, intT>[m];

  parallel_for(intT i = 0; i < n; i++) {
    if (flag[i] < flag[i + 1]) {
      lz[flag[i]] = make_pair(i, prev_occ[i]);
    }
  }
  delete flag;

  return make_pair(lz, m);
}

//Methods they use: make_pair, std::swap, sequence::scan, getDepth, cflog