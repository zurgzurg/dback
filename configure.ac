AC_INIT([dback], [0.1], [http://code.google.com/p/dback/issues/list])

AM_INIT_AUTOMAKE

AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/dback.cpp])
AC_CONFIG_FILES([Makefile src/Makefile])

AC_PROG_INSTALL

###############################################################
## c++ related
###############################################################
AC_PROG_CXXCPP
AC_PROG_CXX
AC_LANG([C++])

AX_CXX_HAVE_STL

###############################################################

AC_CONFIG_HEADERS([src/config.h])

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




#AC_PROG_LIBTOOL

#AC_ARG_ENABLE(
#	[mt-exec],
#	[AS_HELP_STRING(
#                        [--disable-mt-exec],
#                        [disable multi-threaded execution])],
#	[mt_exec=${enableval}],
#	[mt_exec=yes])

#have_pthreads=no
#AC_SEARCH_LIBS([pthread_create], [pthread], [have_pthreads=yes])
#if test "x${have_pthreads}" = "xyes"; then
#  AC_CHECK_HEADERS([pthread.h], [], [have_pthreads=no])
#fi
  

###############################################################
## summary
###############################################################
echo ""
echo "Summary of required items"
echo ""
echo "AS_HELP_STRING([Use multi-threading],[${mt_exec}])"
echo "AS_HELP_STRING([Have C++ STL],[${ax_cv_cxx_have_stl}])"
echo "AS_HELP_STRING([Have C++ namespaces],[${ax_cv_cxx_namespaces}])"

echo ""
echo "Summary of optional items"
echo ""
AS_IF([test "x${DX_FLAG_doc}" = "x1"],
      [echo "AS_HELP_STRING([Using doxygen],[yes])"],
      [echo "AS_HELP_STRING([Using doxygen at],[no])"])
AS_IF([test "x${DX_FLAG_html}" = "x1"],
      [echo "AS_HELP_STRING([Generating doxygen HTML],[yes])"],
      [echo "AS_HELP_STRING([Generating doxygen HTML],[no])"])
AS_IF([test "x${DX_FLAG_pdf}" = "x1"],
      [echo "AS_HELP_STRING([Generating doxygen PDF],[yes])"],
      [echo "AS_HELP_STRING([Generating doxygen PDF],[no])"])
echo ""
AS_IF([test "x${ax_cv_cxx_have_stl}" != "xyes"],
      [AC_MSG_ERROR([Required STL support missing. Build stops.])])
AS_IF([test "x${ax_cv_cxx_namespaces}" != "xyes"],
      [AC_MSG_ERROR([Required C++ namespaces support missing. Build stops.])])

echo "Useful next steps:"
echo "  make doxygen-doc -- generates documentation"
echo "                      output into doxygen-doc/*"
