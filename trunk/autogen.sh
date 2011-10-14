#!/bin/sh

libtoolize --copy

aclocal -I ./m4 \
&& autoheader \
&& automake --add-missing \
&& autoconf
