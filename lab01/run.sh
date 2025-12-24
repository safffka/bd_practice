#!/bin/bash
set -e

if [ "$#" -lt 1 ]; then
  echo "Usage: ./run.sh WAP12.txt [file2 ...]"
  exit 1
fi


for f in "$@"; do
  iconv -f cp1251 -t utf-8 "$f"
done | perl -CS -ne '
  use utf8;
  while ( /([\p{L}]{4,})/g ) {
    my $w = lc($1);
    $cnt{$w}++;
  }
  END {
    for my $w (keys %cnt) {
      print "$w\t$cnt{$w}\n" if $cnt{$w} >= 10;
    }
  }
' | sort -k2,2nr -k1,1 | awk -F'\t' '{ print $1 " - " $2 }'
