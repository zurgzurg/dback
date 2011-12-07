ACLOCAL_AMFLAGS = -I m4

include ./doxygen.am

lib_sources = \
	src/btree.cpp \
	src/r2btree.cpp \
	src/serialbuffer.cpp \
	src/dback_utils.cpp

noinst_PROGRAMS = dback_utests
bin_PROGRAMS = dback
TESTS = dback_utests

dback_SOURCES = src/dback.cpp
dback_LDADD = @BOOST_LDFLAGS@ @BOOST_THREAD_LIB@

lib_LTLIBRARIES = libdback.la
libdback_la_SOURCES = $(lib_sources) src/dback.h
libdback_la_CXXFLAGS = -I$(top_srcdir)/include

dback_utests_SOURCES = src/dback_utests.cpp
dback_utests_LDADD = libdback.la @BOOST_LDFLAGS@ @BOOST_THREAD_LIB@
dback_utests_CXXFLAGS = -I$(top_srcdir)/include

#################################
# dev support to run gcov
#################################
gcov_lib_objs = coverage/btree.o coverage/r2btree.o coverage/serialbuffer.o coverage/dback_utils.o

coverage-stamp:
	mkdir coverage
	touch coverage-stamp

coverage/dback_utests.o $(gcov_lib_objs): coverage/%.o: src/%.cpp
	g++ -c -g -O0 -o $@ -fprofile-arcs -ftest-coverage -I$(top_srcdir)/include $<

coverage/dback_utests: coverage-stamp coverage/dback_utests.o $(gcov_lib_objs)
	g++ -g -o $@ -fprofile-arcs -ftest-coverage coverage/dback_utests.o $(gcov_lib_objs) @BOOST_LDFLAGS@ @BOOST_THREAD_LIB@

coverage:
	rm -fr coverage* src/*.gcov ; make coverage/dback_utests ; ./coverage/dback_utests ; echo '-----' ; gcov --all-blocks --object-file ./coverage/serialbuffer.gcno  serialbuffer.cpp ; echo '-----' ; gcov --all-blocks --object-file ./coverage/btree.gcno  btree.cpp ; gcov --all-blocks --object-file ./coverage/r2btree.gcno  r2btree.cpp
