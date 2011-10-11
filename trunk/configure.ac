AC_INIT([dback], [0.1], [http://code.google.com/p/dback/issues/list])

AC_CONFIG_SRCDIR([src/dback.cpp])
AM_INIT_AUTOMAKE

AC_CONFIG_MACRO_DIR([m4])

AC_PROG_CXXCPP
AC_PROG_CXX
AC_LANG([C++])

AC_CONFIG_HEADERS([src/config.h])
AC_CONFIG_FILES([Makefile src/Makefile])

###############################################################
## doxygen stuff
###############################################################
DX_DOXYGEN_FEATURE(ON)
DX_HTML_FEATURE(ON)
DX_PDF_FEATURE(ON)
DX_CHM_FEATURE(OFF)
DX_CHI_FEATURE(OFF)
DX_MAN_FEATURE(OFF)
DX_RTF_FEATURE(OFF)
DX_XML_FEATURE(OFF)
DX_PS_FEATURE(OFF)
DX_INIT_DOXYGEN([dback], [doxyfile])

###############################################################
## actually generate the output
###############################################################
AS_IF([test "x${DX_FLAG_doc}" = "x1" ],
      [AC_CONFIG_FILES(doxyfile)],[])

AC_OUTPUT
