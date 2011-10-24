ACLOCAL_AMFLAGS = -I m4

SUBDIRS = src

include ./doxygen.am

coverage:
	rm -fr src/coverage* src/*.gcov ; cd src ; make coverage/dback_utests ; ./coverage/dback_utests ; echo '-----' ; gcov --all-blocks --object-file ./coverage/serialbuffer.gcno  serialbuffer.cpp ; echo '-----' ; gcov --all-blocks --object-file ./coverage/btree.gcno  btree.cpp
