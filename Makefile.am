ACLOCAL_AMFLAGS = -I m4

SUBDIRS = src

include ./doxygen.am

coverage:
	rm -fr src/coverage* src/*.gcov ; cd src ; make coverage/dback_utests ; ./coverage/dback_utests ; gcov --object-file ./coverage/serialbuffer.gcno  serialbuffer.cpp ; gcov --object-file ./coverage/btree.gcno  btree.cpp
