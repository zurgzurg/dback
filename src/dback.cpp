#include <config.h>

#include <iostream>

int
main(int argc, const char **argv)
{
  std::cout << "Hello world\n";
  std::cout << "package=" PACKAGE_STRING "..\n";

  return 0;
}
