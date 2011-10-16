## Process this file with automake to produce Makefile.in

lib_sources = serialbuffer.cpp


noinst_PROGRAMS = dback_utests
bin_PROGRAMS = dback
TESTS = dback_utests

dback_SOURCES = dback.cpp

lib_LTLIBRARIES = libdback.la
libdback_la_SOURCES = $(lib_sources) dback.h
libdback_la_CXXFLAGS = -I$(top_srcdir)/include

dback_utests_SOURCES = dback_utests.cpp
dback_utests_LDADD = libdback.la
dback_utests_CXXFLAGS = -I$(top_srcdir)/include

#################################
# dev support to run gcov
#################################
tmp_obj_names = $(patsubst %.cpp,%.o,$(lib_sources))
gcov_lib_objs = $(foreach f,$(tmp_obj_names),coverage/$(f))

coverage-stamp:
	mkdir coverage
	touch coverage-stamp

coverage/dback_utests.o $(gcov_lib_objs): coverage/%.o: %.cpp
	g++ -c -o $@ -fprofile-arcs -ftest-coverage -I$(top_srcdir)/include $<

coverage/dback_utests: coverage-stamp coverage/dback_utests.o $(gcov_lib_objs)
	g++ -o $@ -fprofile-arcs -ftest-coverage coverage/dback_utests.o $(gcov_lib_objs)

all-local: coverage-stamp coverage/dback_utests
