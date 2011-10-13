noinst_PROGRAMS = dback_utests
bin_PROGRAMS = dback

dback_SOURCES = dback.cpp

lib_LTLIBRARIES = libdback.la
libdback_la_SOURCES = dback.cpp errors.cpp dback.h
libdback_la_CXXFLAGS = -I$(top_srcdir)/include

dback_utests_SOURCES = dback_utests.cpp
dback_utests_LDADD = libdback.la
dback_utests_CXXFLAGS = -I$(top_srcdir)/include
