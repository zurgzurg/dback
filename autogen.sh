#!/bin/sh

libtoolize --copy

aclocal -I ./m4 \
&& automake --add-missing \
&& autoconf
