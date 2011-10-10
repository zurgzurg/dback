#!/bin/sh

##
## remove ALL the generated files.
##

rm -f aclocal.m4 configure depcomp
rm -f install-sh ltmain.sh missing Makefile Makefile.in *~
rm -f config.log config.status libtool cpptoken-*.tar.gz
rm -f m4/ltsugar.m4 m4/libtool.m4 m4/ltversion.m4 m4/lt~obsolete.m4
rm -f m4/ltoptions.m4
rm -f include/Makefile.in

rm -f src/*.o src/*.lo src/Makefile.in src/*~ src/utests src/*.a src/*.la
rm -f src/stamp-h1 src/config.h src/Makefile
rm -f include/Makefile include/Makefile.in


rm -fr autom4te.cache .deps src/.libs src/.deps

rm -f doxyfile texput.log
rm -fr doxygen-doc
